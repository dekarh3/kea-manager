#!/usr/bin/env python3
# Kea DHCP Manager v4.0.0 - Network Checks

import subprocess
import socket
import re
import os
from datetime import datetime
import sys

sys.path.insert(0, '/opt/kea-manager')
from config import LOG_FILE, PING_CACHE_FILE, KEA_CONFIG
import ipaddress
import json
import time


class NetworkChecker:
    """Класс для проверок сети (fping, nbtscan, arp, dns)"""

    def __init__(self):
        self.log_file = LOG_FILE
        self.cache_file = PING_CACHE_FILE
        self.config_path = KEA_CONFIG

    def _log(self, message):
        """Логирование в файл"""
        try:
            with open(self.log_file, 'a') as f:
                f.write(f"[{datetime.now().strftime('%F %T')}] {message}\n")
        except:
            pass

    def ping_subnet_with_fping(self, subnet_cidr, timeout=1000):
        """Пинг подсети через fping с CIDR"""
        results = {}
        try:
            network = ipaddress.ip_network(subnet_cidr, strict=False)
            all_ips = [str(ip) for ip in network.hosts()]
            self._log(f"fping scanning {len(all_ips)} IPs in {subnet_cidr}...")

            for ip in all_ips:
                results[ip] = False

            result = subprocess.run(
                ['fping', '-a', '-q', '-t', str(timeout), '-g', subnet_cidr],
                capture_output=True,
                timeout=60
            )

            online_count = 0
            for line in result.stdout.decode().strip().split('\n'):
                ip = line.strip()
                if ip and ip in results:
                    results[ip] = True
                    online_count += 1

            self._log(f"fping found {online_count} online hosts in {subnet_cidr}")
            return results
        except subprocess.TimeoutExpired:
            self._log(f"fping timeout for {subnet_cidr}")
            try:
                network = ipaddress.ip_network(subnet_cidr, strict=False)
                return {str(ip): False for ip in network.hosts()}
            except:
                return {}
        except Exception as e:
            self._log(f"fping error for {subnet_cidr}: {e}")
            return {}

    def get_mac_from_arp(self, ip):
        """Получение MAC через ARP (с ping перед запросом)"""
        try:
            # Сначала пинг для обновления ARP кэша
            subprocess.run(['ping', '-c', '1', '-W', '1', ip], capture_output=True, timeout=2)
            time.sleep(0.3)

            result = subprocess.run(['ip', 'neigh', 'show', ip], capture_output=True, text=True, timeout=2)
            if result.returncode == 0:
                match = re.search(r'([0-9a-fA-F:]{17})', result.stdout)
                if match:
                    mac = match.group(1).lower()
                    self._log(f"✅ ARP resolved {ip} -> {mac}")
                    return mac

            self._log(f"⚠️ ARP failed for {ip}")
        except Exception as e:
            self._log(f"❌ ARP error for {ip}: {e}")
        return None

    def get_hostname_from_dns(self, ip):
        """Получение имени через обратный DNS"""
        try:
            hostname, _, _ = socket.gethostbyaddr(ip)
            hostname = hostname.rstrip('.')
            self._log(f"✅ DNS resolved {ip} -> {hostname}")
            return hostname
        except:
            return None

    def get_hostname_from_nbtscan(self, ip):
        """Получение NetBIOS имени через nbtscan"""
        try:
            self._log(f"🔍 nbtscan REQUEST: nbtscan {ip}")

            result = subprocess.run(
                ['nbtscan', ip],
                capture_output=True,
                text=True,
                timeout=5
            )

            self._log(f"📤 nbtscan RESPONSE (returncode={result.returncode}):\n{result.stdout}")
            if result.stderr:
                self._log(f"⚠️ nbtscan STDERR:\n{result.stderr}")

            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if not line or line.startswith('Doing NBT') or line.startswith('IP address') or line.startswith(
                            '----'):
                        continue

                    parts = line.split()
                    if len(parts) >= 2 and parts[0] == ip:
                        name = parts[1]
                        name = re.sub(r'<[^>]+>', '', name).strip()
                        if name and name != '*' and len(name) > 1:
                            self._log(f"✅ nbtscan found name for {ip}: {name}")
                            return name

                self._log(f"⚠️ nbtscan returned no valid name for {ip}")
        except FileNotFoundError:
            self._log(f"❌ nbtscan not installed")
        except Exception as e:
            self._log(f"❌ nbtscan error for {ip}: {e}")
        return None

    def get_hostname_for_ip(self, ip):
        """Получение имени (DNS → nbtscan)"""
        # 1. Пробуем DNS
        hostname = self.get_hostname_from_dns(ip)
        if hostname:
            return hostname

        # 2. Пробуем nbtscan
        hostname = self.get_hostname_from_nbtscan(ip)
        if hostname:
            return hostname

        self._log(f"⚠️ No hostname found for {ip}")
        return None

    def load_ping_cache(self):
        """Загрузка кэша ping"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def save_ping_cache(self, cache):
        """Сохранение кэша ping"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(cache, f)
                os.chmod(self.cache_file, 0o644)
        except Exception as e:
            self._log(f"❌ Error saving ping cache: {e}")

    def run_ping_check(self, kea_manager, ping_in_progress_flag):
        """Запуск проверки ping"""
        if ping_in_progress_flag['value']:
            return

        ping_in_progress_flag['value'] = True
        start_time = time.time()

        try:
            new_cache = {}
            leases = kea_manager.get_active_leases()
            reservations = kea_manager.get_reservations_from_config()

            known_ips = set()
            known_ip_to_mac = {}

            for lease in leases:
                if lease.get('ip'):
                    known_ips.add(lease['ip'])
                    known_ip_to_mac[lease['ip']] = lease.get('mac', '')

            for mac, res in reservations.items():
                if res.get('ip'):
                    known_ips.add(res['ip'])
                    known_ip_to_mac[res['ip']] = mac

            subnet_cidrs = kea_manager.get_subnet_cidrs()
            for subnet_info in subnet_cidrs:
                cidr = subnet_info['cidr']
                subnet_id = subnet_info['subnet_id']
                self._log(f"Scanning {cidr}...")

                results = self.ping_subnet_with_fping(cidr, timeout=1000)

                for ip, status in results.items():
                    is_known = ip in known_ips
                    new_cache[ip] = {
                        'online': status,
                        'checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'known': is_known,
                        'subnet_id': subnet_id,
                        'mac': known_ip_to_mac.get(ip, '')
                    }

            self.save_ping_cache(new_cache)

            elapsed = time.time() - start_time
            known_online = sum(1 for v in new_cache.values() if v.get('known') and v.get('online'))
            known_offline = sum(1 for v in new_cache.values() if v.get('known') and not v.get('online'))
            unknown_online = sum(1 for v in new_cache.values() if not v.get('known') and v.get('online'))
            total_ips = len(new_cache)

            self._log(
                f"Ping scan completed in {elapsed:.1f}s: {total_ips} total, {known_online} known online, {known_offline} known offline, {unknown_online} spoofed")
        except Exception as e:
            self._log(f"PING ERROR: {e}")
        finally:
            ping_in_progress_flag['value'] = False