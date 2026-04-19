#!/usr/bin/env python3
# Kea DHCP Manager v4.2.1 - Kea DHCP Operations (с DDNS support и бэкапами в отдельной папке)

import socket
import json
import subprocess
import os
from datetime import datetime
import sys

sys.path.insert(0, '/opt/kea-manager')
from config import KEA_SOCKET, KEA_CONFIG, LOG_FILE, DHCP_BACKUP_DIR


class KeaManager:
    """Класс для работы с Kea DHCP через socket и конфиг"""

    def __init__(self):
        self.socket_path = KEA_SOCKET
        self.config_path = KEA_CONFIG
        self.backup_dir = DHCP_BACKUP_DIR
        # Создаём директорию для бэкапов если не существует
        self._ensure_backup_dir()

    def _ensure_backup_dir(self):
        """Создаёт директорию для бэкапов если она не существует"""
        try:
            if not os.path.exists(self.backup_dir):
                os.makedirs(self.backup_dir, mode=0o750, exist_ok=True)
                # Устанавливаем владельца kea:kea если возможно
                try:
                    import pwd, grp
                    uid = pwd.getpwnam('kea').pw_uid
                    gid = grp.getgrnam('kea').gr_gid
                    os.chown(self.backup_dir, uid, gid)
                except:
                    pass
        except Exception as e:
            pass

    def _log(self, message):
        """Логирование в файл"""
        try:
            with open(LOG_FILE, 'a') as f:
                f.write(f"[{datetime.now().strftime('%F %T')}] {message}\n")
        except:
            pass

    def _create_backup_filename(self, prefix='kea-dhcp4.conf.backup'):
        """Создаёт имя файла бэкапа с временной меткой"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{prefix}.{timestamp}"

    def _create_backup(self):
        """Создаёт бэкап конфигурационного файла"""
        try:
            if not os.path.exists(self.config_path):
                return None

            backup_filename = self._create_backup_filename()
            backup_path = os.path.join(self.backup_dir, backup_filename)

            subprocess.run(['cp', self.config_path, backup_path], check=True)
            os.chmod(backup_path, 0o640)

            self._log(f"💾 Backup created: {backup_path}")
            return backup_path
        except Exception as e:
            self._log(f"❌ ERROR creating backup: {e}")
            return None

    def socket_command(self, command, arguments=None):
        """Отправка команды через UNIX socket"""
        payload = {"command": command}
        if arguments:
            payload["arguments"] = arguments

        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(self.socket_path)
                s.sendall((json.dumps(payload) + "\n").encode())
                response = b""
                while True:
                    chunk = s.recv(8192)
                    if not chunk:
                        break
                    response += chunk
                    if b"\n" in response:
                        break
                return json.loads(response.decode().strip())
        except FileNotFoundError:
            return {"result": 1, "text": f"Socket not found: {self.socket_path}"}
        except Exception as e:
            return {"result": 1, "text": f"Socket error: {e}"}

    def get_active_leases(self):
        """Получение активных лизов"""
        self._log("📡 Fetching active leases via lease4-get-all...")
        result = self.socket_command("lease4-get-all")
        if result.get("result") != 0:
            self._log(f"⚠️ lease4-get-all failed: {result}")
            return []

        leases = []
        for lease in result.get("arguments", {}).get("leases", []):
            try:
                state = lease.get("state", 0)
                if state != 0:
                    continue
                leases.append({
                    'ip': lease.get("ip-address"),
                    'mac': lease.get("hw-address", "").lower(),
                    'hostname': lease.get("hostname", "").rstrip('.') if lease.get("hostname") else "",
                    'expire': lease.get("cltt", 0) + lease.get("valid-lft", 0),
                    'subnet_id': lease.get("subnet-id", 1),
                    'pool_id': 0,
                    'is_active': True
                })
            except:
                pass
        self._log(f"✅ Found {len(leases)} active leases")
        return leases

    def get_reservations_from_config(self):
        """Чтение резерваций из конфига"""
        reservations = {}
        if not os.path.exists(self.config_path):
            self._log(f"❌ Config file not found: {self.config_path}")
            return reservations

        try:
            mtime = os.path.getmtime(self.config_path)
            self._log(f"📖 Reading config (mtime={mtime})...")

            with open(self.config_path) as f:
                config = json.load(f)

            reservation_count = 0
            for subnet in config.get('Dhcp4', {}).get('subnet4', []):
                subnet_id = subnet.get('id', 0)
                for res in subnet.get('reservations', []):
                    mac = res.get('hw-address', '').lower()
                    if mac:
                        reservations[mac] = {
                            'mac': mac,
                            'ip': res.get('ip-address'),
                            'hostname': res.get('hostname', ''),
                            'subnet_id': subnet_id
                        }
                        reservation_count += 1

            self._log(f"✅ Found {reservation_count} reservations")
        except Exception as e:
            self._log(f"❌ Error reading reservations: {e}")

        return reservations

    def get_subnets_from_config(self):
        """Чтение подсетей из конфига"""
        subnets = {}
        if not os.path.exists(self.config_path):
            return subnets

        try:
            with open(self.config_path) as f:
                config = json.load(f)

            for subnet in config.get('Dhcp4', {}).get('subnet4', []):
                subnet_id = subnet.get('id', 0)
                subnets[subnet_id] = {
                    'subnet': subnet.get('subnet', ''),
                    'pools': subnet.get('pools', []),
                    'interface': subnet.get('interface', '')
                }
        except:
            pass

        return subnets

    def get_subnet_cidrs(self):
        """Получение CIDR подсетей"""
        cidrs = []
        try:
            with open(self.config_path) as f:
                config = json.load(f)

            for subnet in config.get('Dhcp4', {}).get('subnet4', []):
                subnet_str = subnet.get('subnet', '')
                subnet_id = subnet.get('id', 0)
                if '/' in subnet_str:
                    cidrs.append({
                        'cidr': subnet_str,
                        'subnet_id': subnet_id
                    })
        except Exception as e:
            pass

        return cidrs

    def _get_subnet_id_for_ip(self, ip):
        """Определяет subnet-id для данного IP из конфига"""
        try:
            import ipaddress
            with open(self.config_path) as f:
                config = json.load(f)

            ip_obj = ipaddress.ip_address(ip)

            for subnet in config.get('Dhcp4', {}).get('subnet4', []):
                subnet_str = subnet.get('subnet', '')
                if '/' in subnet_str:
                    network = ipaddress.ip_network(subnet_str, strict=False)
                    if ip_obj in network:
                        return subnet.get('id')
        except Exception as e:
            self._log(f"⚠️ Error determining subnet-id: {e}")

        return None

    def _nsupdate_update(self, fqdn, ip, old_fqdn=None):
        """Обновляет DNS запись через nsupdate с использованием файла ключа"""
        try:
            from config import (
                DDNS_SERVER, DDNS_PORT, DDNS_ZONE,
                DDNS_KEY_NAME, DDNS_KEY_ALGORITHM, DDNS_KEY_SECRET, DDNS_TTL
            )
            import tempfile

            self._log(f"🔑 Starting nsupdate for {fqdn} -> {ip}")

            # Проверка что nsupdate установлен
            try:
                subprocess.run(['which', 'nsupdate'], capture_output=True, text=True, timeout=5)
            except subprocess.CalledProcessError:
                self._log(f"⚠️ nsupdate not installed, skipping DNS update")
                return

            # Создаём временный файл ключа в правильном формате
            key_file_path = None
            try:
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.key') as key_file:
                    key_file.write(f'key "{DDNS_KEY_NAME}" {{\n')
                    key_file.write(f'    algorithm {DDNS_KEY_ALGORITHM};\n')
                    key_file.write(f'    secret "{DDNS_KEY_SECRET}";\n')
                    key_file.write('};\n')
                    key_file_path = key_file.name

                # Установим права на файл ключа
                os.chmod(key_file_path, 0o600)

                # Вычисляем обратную зону для PTR
                ip_parts = ip.split('.')
                ptr_zone = f"{ip_parts[2]}.{ip_parts[1]}.{ip_parts[0]}.in-addr.arpa."
                ptr_record = f"{ip_parts[3]}.{ptr_zone}"

                self._log(f"🔍 PTR zone: {ptr_zone}")
                self._log(f"🔍 PTR record: {ptr_record}")

                # 1. Обновляем ПРЯМУЮ зону (A запись) - ОТДЕЛЬНЫЙ запрос
                commands_a = []
                commands_a.append(f"server {DDNS_SERVER} {DDNS_PORT}")
                commands_a.append(f"zone {DDNS_ZONE}")

                if old_fqdn:
                    commands_a.append(f"update delete {old_fqdn} A")
                    self._log(f"🗑️ Deleting old A: {old_fqdn} from zone {DDNS_ZONE}")

                commands_a.append(f"update add {fqdn} {DDNS_TTL} A {ip}")
                self._log(f"➕ Adding new A: {fqdn} -> {ip} in zone {DDNS_ZONE}")

                commands_a.append("send")
                commands_a.append("answer")

                nsupdate_input_a = "\n".join(commands_a) + "\n"

                result_a = subprocess.run(
                    ['nsupdate', '-v', '-k', key_file_path],
                    input=nsupdate_input_a,
                    capture_output=True,
                    text=True,
                    timeout=10
                )

                if result_a.returncode != 0:
                    self._log(f"⚠️ nsupdate A record warning: {result_a.stderr}")

                # 2. Обновляем ОБРАТНУЮ зону (PTR запись) - ОТДЕЛЬНЫЙ запрос
                commands_ptr = []
                commands_ptr.append(f"server {DDNS_SERVER} {DDNS_PORT}")
                commands_ptr.append(f"zone {ptr_zone}")
                commands_ptr.append(f"update delete {ptr_record} PTR")
                self._log(f"🗑️ Deleting old PTR: {ptr_record} from zone {ptr_zone}")

                commands_ptr.append(f"update add {ptr_record} {DDNS_TTL} PTR {fqdn}")
                self._log(f"➕ Adding new PTR: {ptr_record} -> {fqdn} in zone {ptr_zone}")

                commands_ptr.append("send")
                commands_ptr.append("answer")

                nsupdate_input_ptr = "\n".join(commands_ptr) + "\n"

                result_ptr = subprocess.run(
                    ['nsupdate', '-v', '-k', key_file_path],
                    input=nsupdate_input_ptr,
                    capture_output=True,
                    text=True,
                    timeout=10
                )

                if result_ptr.returncode == 0:
                    self._log(f"✅ nsupdate success: {fqdn} -> {ip}")
                else:
                    self._log(f"❌ nsupdate failed (code {result_ptr.returncode}): {result_ptr.stderr}")

            except subprocess.TimeoutExpired:
                self._log(f"❌ nsupdate timeout after 10 seconds")
            except FileNotFoundError:
                self._log(f"❌ nsupdate command not found. Install dnsutils package.")
            except Exception as e:
                import traceback
                self._log(f"❌ ERROR _nsupdate_update: {e}")
                self._log(f"❌ Traceback: {traceback.format_exc()}")
            finally:
                if key_file_path and os.path.exists(key_file_path):
                    try:
                        os.unlink(key_file_path)
                    except:
                        pass

        except Exception as e:
            import traceback
            self._log(f"❌ CRITICAL ERROR in _nsupdate_update: {e}")
            self._log(f"❌ Traceback: {traceback.format_exc()}")

    def _update_lease_for_ddns(self, ip, mac, new_hostname, old_hostname=''):
        """Обновление DNS через nsupdate при изменении имени"""
        try:
            from config import DDNS_ENABLED, DDNS_ZONE

            # ✅ Проверяем, изменилось ли имя
            if new_hostname == old_hostname:
                self._log(f"⏭️ Hostname unchanged ({new_hostname}), skipping DDNS update")
                return

            if not new_hostname:
                self._log(f"⏭️ Hostname empty, skipping DDNS update")
                return

            if not DDNS_ENABLED:
                self._log("⏭️ DDNS disabled in config, skipping update")
                return

            # 1. Удаляем старый лиз
            del_result = self.socket_command("lease4-del", {"ip-address": ip})
            self._log(f"🗑️ Lease deleted: {ip} - {del_result}")

            # 2. Обновляем DNS через nsupdate
            domain = DDNS_ZONE.rstrip('.')
            fqdn = f"{new_hostname}.{domain}"
            old_fqdn = f"{old_hostname}.{domain}" if old_hostname else None

            self._nsupdate_update(fqdn, ip, old_fqdn)

            self._log(f"✅ DDNS updated via nsupdate: {fqdn} -> {ip}")

        except Exception as e:
            import traceback
            self._log(f"❌ ERROR _update_lease_for_ddns: {e}")
            self._log(f"❌ Traceback: {traceback.format_exc()}")

    def add_reservation(self, mac, ip, hostname=''):
        """Добавление резервации"""
        if not os.path.exists(self.config_path):
            return False

        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)

            # Определяем подсеть
            target_subnet_id = None
            subnets = self.get_subnets_from_config()
            for subnet_id, subnet_info in subnets.items():
                subnet_str = subnet_info.get('subnet', '')
                if '/' in subnet_str:
                    network, prefix = subnet_str.split('/')
                    net_parts = list(map(int, network.split('.')))
                    ip_parts = list(map(int, ip.split('.')))
                    prefix_bits = int(prefix)
                    mask = (0xFFFFFFFF << (32 - prefix_bits)) & 0xFFFFFFFF
                    net_num = (net_parts[0] << 24) + (net_parts[1] << 16) + (net_parts[2] << 8) + net_parts[3]
                    ip_num = (ip_parts[0] << 24) + (ip_parts[1] << 16) + (ip_parts[2] << 8) + ip_parts[3]
                    if (net_num & mask) == (ip_num & mask):
                        target_subnet_id = subnet_id
                        break

            if target_subnet_id is None:
                return False

            added = False
            for subnet in config.get('Dhcp4', {}).get('subnet4', []):
                if subnet.get('id') == target_subnet_id:
                    if 'reservations' not in subnet:
                        subnet['reservations'] = []
                    exists = any(r.get('hw-address', '').lower() == mac.lower() for r in subnet['reservations'])
                    if not exists:
                        new_res = {"hw-address": mac.upper(), "ip-address": ip}
                        if hostname:
                            new_res["hostname"] = hostname
                        subnet['reservations'].append(new_res)
                        added = True
                        break

            if added:
                # ✅ Создаём бэкап в отдельной директории
                self._create_backup()

                with open(self.config_path, 'w') as f:
                    json.dump(config, f, indent=2)
                # os.chmod(self.config_path, 0o640)
                # subprocess.run(['chown', 'root:kea', self.config_path], check=True)
                # subprocess.run(['systemctl', 'restart', 'kea-dhcp4'], timeout=30, check=True)

                # ✅ v4.2.1: Обновление лиза для DDNS
                self._update_lease_for_ddns(ip, mac, hostname)

                self._log(f"✅ Reservation added: {ip} {mac}")
                return True
        except Exception as e:
            import traceback
            self._log(f"❌ ERROR add_reservation: {e}")
            self._log(f"❌ Traceback: {traceback.format_exc()}")

        return False

    def update_reservation_hostname(self, mac, ip, new_hostname, old_hostname=''):
        """Обновление имени в резервации"""
        try:
            if not os.path.exists(self.config_path):
                return False

            self._log(f"📝 Updating hostname: {ip} [{old_hostname} -> {new_hostname}]")

            with open(self.config_path, 'r') as f:
                config = json.load(f)

            updated = False
            for subnet in config.get('Dhcp4', {}).get('subnet4', []):
                for res in subnet.get('reservations', []):
                    if res.get('hw-address', '').lower() == mac.lower() and res.get('ip-address') == ip:
                        self._log(f"🔍 Before update - hostname='{res.get('hostname', '')}'")

                        if new_hostname:
                            res['hostname'] = new_hostname
                        elif 'hostname' in res:
                            del res['hostname']
                        updated = True

                        self._log(f"🔍 After update - hostname='{res.get('hostname', '')}'")
                        break
                if updated:
                    break

            if updated:
                # ✅ Создаём бэкап в отдельной директории
                self._create_backup()

                with open(self.config_path, 'w') as f:
                    json.dump(config, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())

                # os.chmod(self.config_path, 0o640)
                # subprocess.run(['chown', 'root:kea', self.config_path], check=True)

                self._log(f"💾 Config written + fsync, restarting Kea...")
                # subprocess.run(['systemctl', 'restart', 'kea-dhcp4'], timeout=30, check=True)

                # ✅ v4.2.1: Обновление лиза через API для триггера DDNS
                self._update_lease_for_ddns(ip, mac, new_hostname, old_hostname)

                # Верификация
                with open(self.config_path, 'r') as f:
                    verify_config = json.load(f)
                for subnet in verify_config.get('Dhcp4', {}).get('subnet4', []):
                    for res in subnet.get('reservations', []):
                        if res.get('ip-address') == ip:
                            self._log(f"✅ VERIFY: Config contains hostname='{res.get('hostname', '')}' for {ip}")
                            break

                self._log(
                    f"✅ Kea restarted + DDNS trigger, hostname updated: {ip} {mac} [{old_hostname} -> {new_hostname}]")
                return True
            else:
                self._log(f"❌ Reservation not found for {ip} {mac}")
                return False
        except Exception as e:
            import traceback
            self._log(f"❌ CRITICAL ERROR in update_reservation_hostname: {e}")
            self._log(f"❌ Traceback: {traceback.format_exc()}")
            return False

    def remove_reservation(self, mac, ip):
        """Удаление резервации"""
        if not os.path.exists(self.config_path):
            return False

        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)

            removed = False
            for subnet in config.get('Dhcp4', {}).get('subnet4', []):
                reservations = subnet.get('reservations', [])
                original_count = len(reservations)
                subnet['reservations'] = [
                    r for r in reservations
                    if not (r.get('hw-address', '').lower() == mac.lower() and r.get('ip-address') == ip)
                ]
                if len(subnet['reservations']) < original_count:
                    removed = True

            if removed:
                # ✅ Создаём бэкап в отдельной директории
                self._create_backup()

                with open(self.config_path, 'w') as f:
                    json.dump(config, f, indent=2)
                # os.chmod(self.config_path, 0o640)
                # subprocess.run(['chown', 'root:kea', self.config_path], check=True)
                # subprocess.run(['systemctl', 'restart', 'kea-dhcp4'], timeout=30, check=True)

                self._log(f"✅ Reservation removed: {ip} {mac}")
                
                # ✅ После удаления резервации создаём простую аренду (если клиент активен)
                try:
                    self._log(f"🔄 Attempting to create new lease for {ip} ({mac})...")
                    lease_params = {
                        "ip-address": ip,
                        "hw-address": mac,
                        "lifetime": 86400  # 24 часа
                    }
                    self._log(f"📝 Lease params: {lease_params}")
                    result = self.socket_command("lease4-add", lease_params)
                    if result.get('result') == 0:
                        self._log(f"✅ Lease created successfully: {ip}")
                    else:
                        self._log(f"⚠️ Lease creation returned non-zero result: {result}")
                except Exception as e:
                    self._log(f"❌ Failed to create lease: {e}")
                
                return True
        except Exception as e:
            import traceback
            self._log(f"❌ ERROR remove_reservation: {e}")
            self._log(f"❌ Traceback: {traceback.format_exc()}")

        return False

    def delete_lease(self, ip):
        """Удаление лиза через API"""
        result = self.socket_command("lease4-del", {"ip-address": ip})
        self._log(f"🗑️ Lease deleted: {ip} - {result}")
        return result

    def get_backup_files(self, limit=50):
        """Получает список файлов бэкапов"""
        try:
            if not os.path.exists(self.backup_dir):
                return []

            files = []
            for f in sorted(os.listdir(self.backup_dir), reverse=True):
                if f.startswith('kea-dhcp4.conf.backup.'):
                    files.append({
                        'filename': f,
                        'path': os.path.join(self.backup_dir, f),
                        'size': os.path.getsize(os.path.join(self.backup_dir, f)),
                        'mtime': datetime.fromtimestamp(os.path.getmtime(os.path.join(self.backup_dir, f)))
                    })
                if len(files) >= limit:
                    break

            return files
        except Exception as e:
            self._log(f"❌ ERROR getting backup files: {e}")
            return []

    def restore_backup(self, backup_filename):
        """Восстанавливает конфиг из бэкапа"""
        try:
            backup_path = os.path.join(self.backup_dir, backup_filename)

            if not os.path.exists(backup_path):
                self._log(f"❌ Backup file not found: {backup_path}")
                return False

            # Создаём бэкап текущего конфига перед восстановлением
            self._create_backup()

            # Восстанавливаем из бэкапа
            subprocess.run(['cp', backup_path, self.config_path], check=True)
            # os.chmod(self.config_path, 0o640)
            # subprocess.run(['chown', 'root:kea', self.config_path], check=True)
            # subprocess.run(['systemctl', 'restart', 'kea-dhcp4'], timeout=30, check=True)

            self._log(f"✅ Config restored from backup: {backup_filename}")
            return True
        except Exception as e:
            import traceback
            self._log(f"❌ ERROR restoring backup: {e}")
            self._log(f"❌ Traceback: {traceback.format_exc()}")
            return False

    def cleanup_old_backups(self, keep_count=10):
        """Удаляет старые бэкапы, оставляя только последние keep_count"""
        try:
            if not os.path.exists(self.backup_dir):
                return 0

            files = []
            for f in os.listdir(self.backup_dir):
                if f.startswith('kea-dhcp4.conf.backup.'):
                    files.append({
                        'filename': f,
                        'path': os.path.join(self.backup_dir, f),
                        'mtime': os.path.getmtime(os.path.join(self.backup_dir, f))
                    })

            # Сортируем по времени (старые первые)
            files.sort(key=lambda x: x['mtime'])

            # Удаляем старые, оставляем keep_count
            deleted = 0
            for f in files[:-keep_count] if len(files) > keep_count else []:
                os.remove(f['path'])
                deleted += 1
                self._log(f"🗑️ Deleted old backup: {f['filename']}")

            return deleted
        except Exception as e:
            self._log(f"❌ ERROR cleaning up backups: {e}")
            return 0

    def refresh_all_dns(self):
        """Принудительное обновление всех DNS записей"""
        try:
            from config import DDNS_ENABLED, DDNS_ZONE

            if not DDNS_ENABLED:
                return {'success': False, 'error': 'DDNS disabled in config'}

            reservations = self.get_reservations_from_config()
            ping_cache = self.network_checker.load_ping_cache() if hasattr(self, 'network_checker') else {}

            updated = 0
            failed = 0

            domain = DDNS_ZONE.rstrip('.')

            for mac, res in reservations.items():
                ip = res.get('ip')
                hostname = res.get('hostname', '')

                if not ip or not hostname:
                    continue

                fqdn = f"{hostname}.{domain}"

                try:
                    self._nsupdate_update(fqdn, ip, None)
                    updated += 1
                    self._log(f"✅ DNS refreshed: {fqdn} -> {ip}")
                except Exception as e:
                    failed += 1
                    self._log(f"❌ DNS refresh failed for {fqdn}: {e}")

            return {
                'success': True,
                'updated': updated,
                'failed': failed,
                'total': len(reservations)
            }

        except Exception as e:
            import traceback
            self._log(f"❌ ERROR refresh_all_dns: {e}")
            self._log(f"❌ Traceback: {traceback.format_exc()}")
            return {'success': False, 'error': str(e)}
