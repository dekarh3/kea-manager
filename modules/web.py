#!/usr/bin/env python3
# Kea DHCP Manager v4.2.0 - Web Server
# FIX: Добавлена подробная диагностика ошибки 'refresh_all_dns'

import http.server
import socketserver
import base64
import threading
import time
import re
import os
import json
import io
from urllib.parse import parse_qs, urlparse
from datetime import datetime
import sys
import traceback  # Добавлено для детального логирования

sys.path.insert(0, '/opt/kea-manager')
from config import AUTH_ENABLED, AUTH_USERNAME, AUTH_PASSWORD, VERSION, FAVICON_SVG, DOMAIN_SUFFIX, KEA_SOCKET

try:
    import xlsxwriter

    XLSX_AVAILABLE = True
except ImportError:
    XLSX_AVAILABLE = False

# ============================================================================
# CSS СТИЛИ
# ============================================================================
HTML_STYLES = '''
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 0; padding: 20px; background: #f5f7fa; }
.container { max-width: 1800px; margin: 0 auto; background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); }
h1 { color: #0066cc; border-bottom: 3px solid #0066cc; padding-bottom: 12px; margin: 0 0 20px 0; }
.stats { display: flex; gap: 15px; margin-bottom: 25px; flex-wrap: wrap; }
.stat { padding: 15px 20px; border-radius: 8px; text-align: center; min-width: 120px; flex: 1; cursor: pointer; transition: transform 0.2s, box-shadow 0.2s; }
.stat:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.15); }
.stat.active-filter { ring: 3px solid #0066cc; box-shadow: 0 0 0 3px #0066cc; }
.stat.total { background: linear-gradient(135deg, #e3f2fd, #bbdefb); }
.stat.active { background: linear-gradient(135deg, #d4edda, #c3e6cb); }
.stat.reserved { background: linear-gradient(135deg, #fff3cd, #ffeaa7); }
.stat.inactive { background: linear-gradient(135deg, #e8e8e8, #d0d0d0); }
.stat.unknown { background: linear-gradient(135deg, #fff9c4, #fff3cd); }
.stat.spoofed { background: linear-gradient(135deg, #f8bbd0, #f48fb1); }
.stat-num { font-size: 28px; font-weight: 700; color: #1a1a1a; }
.stat-label { color: #555; font-size: 13px; margin-top: 4px; }
table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }
th, td { padding: 1px 4px; text-align: left; border-bottom: 1px solid #e0e0e0; }
th { background: #0066cc; color: white; font-weight: 600; position: sticky; top: 100px; z-index: 30; }
tr:hover { filter: brightness(0.95); }
tr.filtered { display: none; }
.btn { padding: 6px 12px; border: none; border-radius: 4px; cursor: pointer; font-size: 12px; margin-right: 6px; white-space: nowrap; }
.btn-reserve { background: #28a745; color: white; }
.btn-unreserve { background: #dc3545; color: white; }
.btn-delete { background: #6c757d; color: white; }
.btn-ping { background: #17a2b8; color: white; }
.btn-clear { background: #6c757d; color: white; }
.btn-edit { background: #0066cc; color: white; }
.btn-save { background: #28a745; color: white; }
.btn-cancel { background: #6c757d; color: white; }
.btn-export { background: #217346; color: white; }
.btn-dns { background: #ff9800; color: white; }
code { background: #f1f3f5; padding: 3px 6px; border-radius: 3px; font-family: monospace; }
.badge { padding: 4px 10px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; display: inline-block; }
.badge-online { background: #d4edda; color: #155724; }
.badge-offline { background: #f8d7da; color: #721c24; }
.badge-unknown { background: #fff3cd; color: #856404; }
.badge-reserved { background: #6c757d; color: white; }
.badge-spoofed { background: #f8bbd0; color: #880e4f; }
.pool-info { background: #f8f9fa; padding: 10px 15px; border-radius: 6px; margin: 10px 0; font-size: 13px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; position: sticky; top: 50px; z-index: 20; }
.pool-stats { display: flex; gap: 15px; flex-wrap: wrap; align-items: center; }
.pool-actions { display: flex; gap: 6px; flex-wrap: wrap; }
.footer { margin-top: 25px; padding-top: 15px; border-top: 1px solid #eee; font-size: 11px; color: #777; }
.form-inline { display: inline-flex; gap: 4px; align-items: center; }
.socket-status { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }
.socket-ok { background: #28a745; }
.socket-error { background: #dc3545; }
.toolbar { margin-bottom: 20px; padding: 15px; background: #f8f9fa; border-radius: 6px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
.legend { font-size: 12px; color: #666; margin-left: auto; }
.legend span { margin-left: 10px; }
.filter-info { background: #e3f2fd; padding: 10px 15px; border-radius: 6px; margin-bottom: 20px; display: none; align-items: center; gap: 10px; }
.filter-info.visible { display: flex; }
.filter-info strong { color: #0066cc; }
.subnet-tabs { display: flex; gap: 4px; margin-bottom: 15px; border-bottom: 2px solid #e0e0e0; padding-bottom: 0; overflow-x: auto; position: sticky; top: 0; z-index: 40; background: white; padding-top: 10px; }
.subnet-tab { padding: 10px 20px; background: #f1f3f5; border: none; border-radius: 8px 8px 0 0; cursor: pointer; font-size: 13px; font-weight: 500; color: #555; transition: all 0.2s; white-space: nowrap; }
.subnet-tab:hover { background: #e3f2fd; color: #0066cc; }
.subnet-tab.active { background: #0066cc; color: white; }
.subnet-tab .tab-count { margin-left: 8px; background: rgba(0,0,0,0.1); padding: 2px 8px; border-radius: 10px; font-size: 11px; }
.subnet-tab.active .tab-count { background: rgba(255,255,255,0.2); }
.subnet-content { display: none; }
.subnet-content.active { display: block; }
.edit-wrapper { display: inline-flex; gap: 4px; align-items: center; }
.edit-wrapper .hostname-text { display: none !important; }
.edit-wrapper .edit-input { display: none !important; visibility: hidden !important; width: 0 !important; height: 0 !important; padding: 0 !important; margin: 0 !important; border: none !important; }
.edit-wrapper .btn-save, .edit-wrapper .btn-cancel { display: none !important; }
.edit-wrapper.edit-mode .edit-input { display: inline-block !important; visibility: visible !important; width: 150px !important; height: auto !important; padding: 4px 8px !important; margin: 0 !important; border: 1px solid #0066cc !important; }
.edit-wrapper.edit-mode .btn-edit { display: none !important; }
.edit-wrapper.edit-mode .btn-save, .edit-wrapper.edit-mode .btn-cancel { display: inline-block !important; }
'''

# ============================================================================
# JAVASCRIPT
# ============================================================================
HTML_SCRIPTS = '''
<script>
let currentFilter = 'all';

// Сохранение и восстановление активной вкладки через localStorage
document.addEventListener('DOMContentLoaded', function() {
    const savedSubnet = localStorage.getItem('activeSubnet');
    if (savedSubnet) {
        switchSubnet(savedSubnet);
    }
});

function filterTable(filterType) {
    currentFilter = filterType;
    const rows = document.querySelectorAll('.filter-row');
    const filterInfo = document.getElementById('filter-info');
    const filterName = document.getElementById('filter-name');
    const statCards = document.querySelectorAll('.stat');
    statCards.forEach(card => card.classList.remove('active-filter'));
    rows.forEach(row => {
        let show = false;
        if (filterType === 'all') { show = true; filterInfo.classList.remove('visible'); }
        else if (filterType === 'active' && row.classList.contains('row-active')) { show = true; }
        else if (filterType === 'reserved' && row.classList.contains('row-reserved')) { show = true; }
        else if (filterType === 'online' && row.classList.contains('row-online')) { show = true; }
        else if (filterType === 'inactive' && row.classList.contains('row-inactive')) { show = true; }
        else if (filterType === 'unknown' && row.classList.contains('row-unknown')) { show = true; }
        else if (filterType === 'spoofed' && row.classList.contains('row-spoofed')) { show = true; }
        if (show) { row.classList.remove('filtered'); } else { row.classList.add('filtered'); }
    });
    if (filterType !== 'all') {
        filterInfo.classList.add('visible');
        const filterNames = {'active': 'Активные лизы', 'reserved': 'Постоянные', 'online': '🟢 Active', 'inactive': '🔴 Inactive', 'unknown': '⚪ Unknown', 'spoofed': '👻 Spoofed'};
        filterName.textContent = filterNames[filterType] || filterType;
        const activeCard = document.querySelector(`.stat[onclick="filterTable('${filterType}')"]`);
        if (activeCard) { activeCard.classList.add('active-filter'); }
    }
    updateTabCounts();
}
function clearFilter() { filterTable('all'); }
window.switchSubnet = function switchSubnet(subnetId) {
    document.querySelectorAll('.subnet-content').forEach(content => content.classList.remove('active'));
    document.querySelectorAll('.subnet-tab').forEach(tab => tab.classList.remove('active'));
    const contentEl = document.getElementById('subnet-content-' + subnetId);
    const tabEl = document.querySelector(`.subnet-tab[data-subnet="${subnetId}"]`);
    if (contentEl) contentEl.classList.add('active');
    if (tabEl) tabEl.classList.add('active');
    // Сохраняем активную вкладку в localStorage
    localStorage.setItem('activeSubnet', subnetId);
    updateTabCounts();
}
function updateTabCounts() {
    document.querySelectorAll('.subnet-tab').forEach(tab => {
        const subnetId = tab.dataset.subnet;
        const content = document.getElementById('subnet-content-' + subnetId);
        if (!content) return;
        const visibleRows = content.querySelectorAll('.filter-row:not(.filtered)').length;
        const countEl = tab.querySelector('.tab-count');
        if (countEl) countEl.textContent = visibleRows;
    });
}
function startEdit(btn, ip) {
    const wrapper = btn.closest('.edit-wrapper');
    const input = wrapper.querySelector('.edit-input');
    wrapper.dataset.originalValue = input.value;
    wrapper.classList.add('edit-mode');
    fetch('/?action=nbtscan_query&ip=' + encodeURIComponent(ip))
        .then(response => response.text())
        .then(nbtName => {
            if (nbtName && nbtName.trim() !== '' && nbtName !== '—' && nbtName !== 'Unknown') {
                input.value = nbtName.trim();
            }
        })
        .catch(error => { console.log('nbtscan fetch error:', error); });
    input.focus();
    input.select();
}
function cancelEdit(btn) {
    const wrapper = btn.closest('.edit-wrapper');
    const input = wrapper.querySelector('.edit-input');
    input.value = wrapper.dataset.originalValue;
    wrapper.classList.remove('edit-mode');
}
function saveEdit(btn) {
    const wrapper = btn.closest('.edit-wrapper');
    const input = wrapper.querySelector('.edit-input');
    const ip = wrapper.dataset.ip;
    const mac = wrapper.dataset.mac;
    const oldHostname = wrapper.dataset.oldHostname;
    const newHostname = input.value.trim();
    const url = '/?action=update_hostname&mac=' + encodeURIComponent(mac) + '&ip=' + encodeURIComponent(ip) + '&old_hostname=' + encodeURIComponent(oldHostname) + '&new_hostname=' + encodeURIComponent(newHostname);
    window.location.href = url;
}
function refreshAllDNS() {
    if (!confirm('⚠️ Вы уверены?\\n\\nЭто действие обновит DNS записи для всех резервированных хостов.\\nПроцесс может занять несколько минут.')) {
        return;
    }
    const btn = event.target;
    btn.disabled = true;
    btn.textContent = '⏳ Обновление...';
    fetch('/?action=refresh_dns', { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                alert('✅ DNS обновлён успешно!\\nОбновлено записей: ' + data.updated);
            } else {
                alert('❌ Ошибка: ' + (data.error || 'Неизвестная ошибка'));
            }
            btn.disabled = false;
            btn.textContent = '🔄 Обновить DNS';
        })
        .catch(error => {
            alert('❌ Ошибка сети: ' + error);
            btn.disabled = false;
            btn.textContent = '🔄 Обновить DNS';
        });
}
</script>
'''


def count_ips_in_pool(pool_str):
    """Calculate number of IPs in a pool range like '192.168.0.3 - 192.168.0.253'"""
    try:
        if ' - ' in pool_str:
            start_ip, end_ip = pool_str.split(' - ')
            start = tuple(map(int, start_ip.strip().split('.')))
            end = tuple(map(int, end_ip.strip().split('.')))
            start_num = start[0] * 256 ** 3 + start[1] * 256 ** 2 + start[2] * 256 + start[3]
            end_num = end[0] * 256 ** 3 + end[1] * 256 ** 2 + end[2] * 256 + end[3]
            return max(0, end_num - start_num + 1)
        return 0
    except:
        return 0


class WebServer:
    """HTTP сервер для веб-интерфейса"""

    def __init__(self, port, kea_manager, network_checker, ping_in_progress_flag):
        self.port = port
        self.kea_manager = kea_manager
        self.network_checker = network_checker
        self.ping_in_progress_flag = ping_in_progress_flag
        self.handler_class = self._create_handler()

    def _create_handler(self):
        kea_manager = self.kea_manager
        network_checker = self.network_checker
        ping_in_progress_flag = self.ping_in_progress_flag

        class KeaHandler(http.server.BaseHTTPRequestHandler):
            def check_auth(self):
                if not AUTH_ENABLED:
                    return True
                auth_header = self.headers.get('Authorization', '')
                if auth_header.startswith('Basic '):
                    try:
                        credentials = base64.b64decode(auth_header[6:]).decode('utf-8')
                        username, password = credentials.split(':', 1)
                        if username == AUTH_USERNAME and password == AUTH_PASSWORD:
                            return True
                    except:
                        pass
                self.send_response(401)
                self.send_header('WWW-Authenticate', 'Basic realm="Kea DHCP Manager"')
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write('<html><body><h1>401 Unauthorized</h1></body></html>'.encode('utf-8'))
                return False

            def _get_params(self):
                url_params = {}
                if '?' in self.path:
                    query = urlparse(self.path).query
                    url_params = parse_qs(query)
                body_params = {}
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 0:
                    post_data = self.rfile.read(content_length).decode('utf-8')
                    body_params = parse_qs(post_data)
                return {**url_params, **body_params}

            def do_GET(self):
                if not self.check_auth():
                    return
                if self.path == '/favicon.ico':
                    self.send_response(200)
                    self.send_header('Content-type', 'image/svg+xml')
                    self.end_headers()
                    self.wfile.write(FAVICON_SVG.encode('utf-8'))
                    return
                params = self._get_params()
                action = params.get('action', [None])[0]
                if action == 'nbtscan_query':
                    ip = params.get('ip', [None])[0]
                    if ip:
                        name = network_checker.get_hostname_from_nbtscan(ip)
                        self.send_response(200)
                        self.send_header('Content-type', 'text/plain; charset=utf-8')
                        self.end_headers()
                        self.wfile.write((name if name else '').encode('utf-8'))
                        return
                if action == 'update_hostname':
                    mac = params.get('mac', [None])[0]
                    ip = params.get('ip', [None])[0]
                    new_hostname = params.get('new_hostname', [''])[0]
                    old_hostname = params.get('old_hostname', [''])[0]
                    if mac and ip:
                        if kea_manager.update_reservation_hostname(mac, ip, new_hostname, old_hostname):
                            network_checker._log(f"✅ Hostname updated: {ip}")
                        self.send_response(303)
                        self.send_header('Location', '/?nocache=' + str(int(time.time())))
                        self.end_headers()
                        return
                if action == 'export_xlsx':
                    self.export_xlsx()
                    return
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
                self.end_headers()
                self.wfile.write(self.generate_html().encode('utf-8'))

            def do_POST(self):
                if not self.check_auth():
                    return
                params = self._get_params()
                action = params.get('action', [None])[0]

                if action == 'refresh_dns':
                    try:
                        # Проверяем наличие метода перед вызовом
                        if not hasattr(kea_manager, 'refresh_all_dns'):
                            available_methods = [m for m in dir(kea_manager) if
                                                 not m.startswith('_') and callable(getattr(kea_manager, m))]
                            error_msg = f"Метод 'refresh_all_dns' не найден в KeaManager. Доступные методы: {available_methods}"
                            network_checker._log(f"❌ CRITICAL: {error_msg}")
                            self.send_response(501)  # Not Implemented
                            self.send_header('Content-type', 'application/json; charset=utf-8')
                            self.end_headers()
                            self.wfile.write(json.dumps({
                                'success': False,
                                'error': 'Функция обновления DNS не реализована',
                                'details': error_msg,
                                'available_methods': available_methods[:20]  # Показываем первые 20 методов
                            }).encode('utf-8'))
                            return

                        result = kea_manager.refresh_all_dns()
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json; charset=utf-8')
                        self.end_headers()
                        self.wfile.write(json.dumps(result).encode('utf-8'))
                    except AttributeError as e:
                        # Детальное логирование для отладки
                        tb = traceback.format_exc()
                        available = [m for m in dir(kea_manager) if
                                     not m.startswith('_') and callable(getattr(kea_manager, m))]
                        network_checker._log(f"❌ AttributeError в refresh_dns: {e}")
                        network_checker._log(f"🔍 Доступные методы KeaManager: {available}")
                        network_checker._log(f"📋 Traceback:\n{tb}")
                        self.send_response(500)
                        self.send_header('Content-type', 'application/json; charset=utf-8')
                        self.end_headers()
                        self.wfile.write(json.dumps({
                            'success': False,
                            'error': f'AttributeError: {str(e)}',
                            'hint': 'Проверьте, реализован ли метод refresh_all_dns() в модуле kea.py'
                        }).encode('utf-8'))
                    except Exception as e:
                        tb = traceback.format_exc()
                        network_checker._log(f"❌ refresh_dns error: {e}")
                        network_checker._log(f"📋 Traceback:\n{tb}")
                        self.send_response(500)
                        self.send_header('Content-type', 'application/json; charset=utf-8')
                        self.end_headers()
                        self.wfile.write(json.dumps({'success': False, 'error': str(e)}).encode('utf-8'))
                    return

                if action == 'reserve':
                    mac = params.get('mac', [None])[0]
                    ip = params.get('ip', [None])[0]
                    hostname = params.get('hostname', [''])[0]
                    if mac and ip:
                        if mac == 'UNKNOWN':
                            resolved_mac = network_checker.get_mac_from_arp(ip)
                            if resolved_mac:
                                mac = resolved_mac
                            else:
                                self.send_response(303)
                                self.send_header('Location', '/')
                                self.end_headers()
                                return
                        if kea_manager.add_reservation(mac, ip, hostname):
                            self.send_response(303)
                            self.send_header('Location', '/')
                            self.end_headers()
                            return
                if action == 'unreserve':
                    mac = params.get('mac', [None])[0]
                    ip = params.get('ip', [None])[0]
                    if kea_manager.remove_reservation(mac, ip):
                        # ✅ После снятия резервации обновляем активные аренды
                        kea_manager.get_active_leases()
                        self.send_response(303)
                        self.send_header('Location', '/?nocache=' + str(int(time.time())))
                        self.end_headers()
                        return
                if action == 'delete_lease':
                    mac = params.get('mac', [None])[0]
                    ip = params.get('ip', [None])[0]
                    if kea_manager.delete_lease(ip):
                        self.send_response(303)
                        self.send_header('Location', '/')
                        self.end_headers()
                        return
                if action == 'ping_check':
                    thread = threading.Thread(target=network_checker.run_ping_check,
                                              args=(kea_manager, ping_in_progress_flag))
                    thread.daemon = True
                    thread.start()
                    self.send_response(303)
                    self.send_header('Location', '/')
                    self.end_headers()
                    return
                self.send_response(303)
                self.send_header('Location', '/')
                self.end_headers()

            def export_xlsx(self):
                if not XLSX_AVAILABLE:
                    self.send_response(500)
                    self.send_header('Content-type', 'text/plain; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(b'xlsxwriter not installed. Run: pip3 install xlsxwriter')
                    return
                try:
                    output = io.BytesIO()
                    workbook = xlsxwriter.Workbook(output)
                    active_leases = kea_manager.get_active_leases()
                    reservations = kea_manager.get_reservations_from_config()
                    subnets_config = kea_manager.get_subnets_from_config()
                    ping_cache = network_checker.load_ping_cache()
                    leases_by_subnet = {}
                    seen_macs = set()
                    for lease in active_leases:
                        mac = lease['mac']
                        seen_macs.add(mac)
                        lease['is_reserved'] = mac in reservations
                        lease['ping_status'] = ping_cache.get(lease['ip'], {}).get('online', None)
                        key = f"{lease['subnet_id']}|{lease['pool_id']}"
                        if key not in leases_by_subnet:
                            leases_by_subnet[key] = {'subnet_id': lease['subnet_id'], 'leases': []}
                        leases_by_subnet[key]['leases'].append(lease)
                    for mac, res in reservations.items():
                        if mac not in seen_macs:
                            ip = res['ip']
                            ping_status = ping_cache.get(ip, {}).get('online', None)
                            lease = {
                                'ip': ip, 'mac': mac, 'hostname': res.get('hostname', ''),
                                'expire': 0, 'subnet_id': res['subnet_id'],
                                'is_reserved': True, 'is_active': False,
                                'ping_status': ping_status
                            }
                            key = f"{res['subnet_id']}|0"
                            if key not in leases_by_subnet:
                                leases_by_subnet[key] = {'subnet_id': res['subnet_id'], 'leases': []}
                            leases_by_subnet[key]['leases'].append(lease)
                    for subnet_id, subnet_info in subnets_config.items():
                        subnet_str = subnet_info.get('subnet', f'Subnet {subnet_id}')
                        key = f"{subnet_id}|0"
                        if key not in leases_by_subnet:
                            leases_by_subnet[key] = {'subnet_id': subnet_id, 'leases': []}
                        leases_by_subnet[key]['subnet_str'] = subnet_str
                    for key, data in leases_by_subnet.items():
                        safe_subnet_name = re.sub(r'[\[\]:*?/\\\\]', '_',
                                                  data.get('subnet_str', f"Subnet {data['subnet_id']}"))[:31]
                        if not safe_subnet_name.strip():
                            safe_subnet_name = f"Subnet_{data['subnet_id']}"
                        worksheet = workbook.add_worksheet(safe_subnet_name)
                        format_header = workbook.add_format(
                            {'bold': True, 'bg_color': '#0066cc', 'font_color': 'white'})
                        format_cell = workbook.add_format()
                        headers = ['IP', 'MAC', 'Hostname', 'Status', 'Expires']
                        for col, header in enumerate(headers):
                            worksheet.write(0, col, header, format_header)
                        for row, lease in enumerate(
                                sorted(data['leases'], key=lambda x: tuple(map(int, x['ip'].split('.')))), 1):
                            status = 'Active' if lease.get('ping_status') else 'Inactive'
                            if lease.get('is_reserved'):
                                status = 'Reserved'
                            worksheet.write(row, 0, lease['ip'], format_cell)
                            worksheet.write(row, 1, lease['mac'], format_cell)
                            worksheet.write(row, 2, lease.get('hostname', ''), format_cell)
                            worksheet.write(row, 3, status, format_cell)
                            if lease.get('expire') and lease.get('expire') > 0:
                                expire_str = datetime.fromtimestamp(lease['expire']).strftime('%d.%m.%Y %H:%M')
                            else:
                                expire_str = ''
                            worksheet.write(row, 4, expire_str, format_cell)
                        worksheet.set_column('A:A', 15)
                        worksheet.set_column('B:B', 18)
                        worksheet.set_column('C:C', 20)
                        worksheet.set_column('D:D', 12)
                        worksheet.set_column('E:E', 18)
                    workbook.close()
                    self.send_response(200)
                    self.send_header('Content-type',
                                     'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    self.send_header('Content-Disposition',
                                     f'attachment; filename="kea-dhcp-{datetime.now().strftime("%Y%m%d-%H%M%S")}.xlsx"')
                    self.send_header('Content-Length', len(output.getvalue()))
                    self.end_headers()
                    self.wfile.write(output.getvalue())
                except Exception as e:
                    network_checker._log(f"❌ Export error: {e}")
                    self.send_response(500)
                    self.send_header('Content-type', 'text/plain; charset=utf-8')
                    self.end_headers()
                    self.wfile.write(f'Export error: {e}'.encode('utf-8'))

            def generate_html(self):
                active_leases = kea_manager.get_active_leases()
                reservations = kea_manager.get_reservations_from_config()
                subnets_config = kea_manager.get_subnets_from_config()
                ping_cache = network_checker.load_ping_cache()
                leases_by_subnet = {}
                seen_macs = set()
                # Сначала добавляем все резервации из конфига
                for mac, res in reservations.items():
                    ip = res['ip']
                    ping_status = ping_cache.get(ip, {}).get('online', None)
                    lease = {
                        'ip': ip, 'mac': mac, 'hostname': res.get('hostname', ''),
                        'expire': 0, 'subnet_id': res['subnet_id'], 'pool_id': 0,
                        'is_reserved': True, 'is_active': False,
                        'ping_status': ping_status, 'is_spoofed': False
                    }
                    key = f"{res['subnet_id']}|0"
                    if key not in leases_by_subnet:
                        leases_by_subnet[key] = {'subnet_id': res['subnet_id'], 'pool_id': 0, 'leases': []}
                    leases_by_subnet[key]['leases'].append(lease)
                    seen_macs.add(mac)
                # Добавляем активные аренды только если MAC нет в резервациях
                for lease in active_leases:
                    mac = lease['mac']
                    if mac in seen_macs:
                        continue
                    lease['is_reserved'] = False
                    lease['ping_status'] = ping_cache.get(lease['ip'], {}).get('online', None)
                    lease['is_spoofed'] = False
                    key = f"{lease['subnet_id']}|{lease['pool_id']}"
                    if key not in leases_by_subnet:
                        leases_by_subnet[key] = {'subnet_id': lease['subnet_id'], 'pool_id': lease['pool_id'],
                                                 'leases': []}
                    leases_by_subnet[key]['leases'].append(lease)
                total_leases = sum(len(d['leases']) for d in leases_by_subnet.values())
                total_reserved = sum(1 for d in leases_by_subnet.values() for l in d['leases'] if l['is_reserved'])
                total_active = sum(
                    1 for d in leases_by_subnet.values() for l in d['leases'] if l.get('is_active', False))
                total_online = sum(
                    1 for d in leases_by_subnet.values() for l in d['leases'] if l.get('ping_status') == True)
                total_inactive = sum(
                    1 for d in leases_by_subnet.values() for l in d['leases'] if l.get('ping_status') == False)
                total_spoofed = sum(
                    1 for d in leases_by_subnet.values() for l in d['leases'] if l.get('is_spoofed', False))

                def esc(s):
                    if not s: return ''
                    return str(s).replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')

                def clean_hostname(hostname):
                    if not hostname: return '—'
                    if hostname.endswith(DOMAIN_SUFFIX):
                        hostname = hostname[:-len(DOMAIN_SUFFIX)]
                    if hostname.startswith('dhcp-'):
                        hostname = hostname[5:]
                    if re.match(r'^\d+-\d+-\d+-\d+$', hostname):
                        hostname = hostname.replace('-', '.')
                    return hostname if hostname else '—'

                def format_expire(expire_ts, is_reserved, is_spoofed=False):
                    if is_spoofed:
                        return '<span class="badge badge-spoofed">👻 Spoofed</span>'
                    if is_reserved or not expire_ts or expire_ts == 0:
                        return '<span class="badge badge-reserved">🔒 Постоянные</span>'
                    try:
                        dt = datetime.fromtimestamp(expire_ts)
                        now = datetime.now()
                        diff = dt - now
                        hours_left = diff.total_seconds() / 3600
                        if hours_left < 0:
                            color = '#dc3545'
                            text = 'Истекло'
                        elif hours_left < 2:
                            color = '#dc3545'
                            text = f'< {hours_left:.1f} ч'
                        elif hours_left < 24:
                            color = '#ffc107'
                            text = f'{hours_left:.1f} ч'
                        else:
                            color = '#28a745'
                            text = f'{hours_left / 24:.1f} дн'
                        time_str = dt.strftime("%d.%m %H:%M")
                        return f'<span style="color:{color};font-weight:600;">{text}</span><br><small style="color:#888;">{time_str}</small>'
                    except:
                        return str(expire_ts)

                def get_row_style(is_active, is_reserved, ping_status, is_spoofed=False):
                    if is_spoofed: return 'background-color: #f8bbd0;'
                    if ping_status == False:
                        return 'background-color: #f0f0f0;' if is_reserved else 'background-color: #e8e8e8;'
                    elif ping_status is None:
                        return 'background-color: #fff9c4;' if is_reserved else 'background-color: #fff3cd;'
                    else:
                        return 'background-color: #d4edda;' if is_reserved else ''

                def get_ping_badge(ping_status, is_spoofed=False):
                    if is_spoofed: return '<span class="badge badge-spoofed">👻 Spoofed</span>'
                    if ping_status == True:
                        return '<span class="badge badge-online">🟢 Active</span>'
                    elif ping_status == False:
                        return '<span class="badge badge-offline">🔴 Inactive</span>'
                    else:
                        return '<span class="badge badge-unknown">⚪ Unknown</span>'

                html_parts = []
                socket_class = 'socket-ok' if os.path.exists(KEA_SOCKET) else 'socket-error'
                export_button = '<button class="btn btn-export" onclick="location.href=\'/?action=export_xlsx\'">📊 Экспорт XLSX</button>' if XLSX_AVAILABLE else ''

                # Кнопка обновления DNS: отключаем, если метод не реализован
                dns_button_disabled = not hasattr(kea_manager, 'refresh_all_dns')
                if dns_button_disabled:
                    disabled_attr = 'disabled style="opacity:0.5;cursor:not-allowed;"'
                else:
                    disabled_attr = ''
                dns_btn_html = f'<button class="btn btn-dns" onclick="refreshAllDNS()" {disabled_attr}>🔄 Обновить DNS</button>'
                html_parts.append(f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate, max-age=0">
<title>⚡ Kea DHCP Manager v{VERSION}</title>
<link rel="icon" href="/favicon.ico" type="image/svg+xml">
<style>{HTML_STYLES}</style>
<script>{HTML_SCRIPTS.replace('<script>', '').replace('</script>', '')}</script>
</head>
<body>
<div class="container">
<h1>⚡ Kea DHCP Manager v{VERSION}</h1>
<p style="margin: 0 0 20px 0; color: #666;">
<small>
<span class="socket-status {socket_class}"></span>
Socket: {KEA_SOCKET} | Обновлено: <span id="update-time"></span>
</small>
<script>document.getElementById('update-time').textContent = new Date().toLocaleString('ru-RU')</script>
| <a href="/" style="color: #0066cc;">🔄 Обновить</a>
</p>
<div class="toolbar" style="display:none;">
<form method="POST" style="display:inline;">
<input type="hidden" name="action" value="ping_check">
<button type="submit" class="btn btn-ping" onclick="this.disabled=true;this.textContent='Сканирование...'">📡 Сканировать сеть</button>
</form>
{export_button}
{dns_btn_html}
<div class="legend">
<span>🟢 Known</span><span>🔴 Inactive</span><span>⚪ Unknown</span><span style="background:#f8bbd0;padding:2px 6px;border-radius:4px;">👻 Spoofed</span>
</div>
</div>
<div class="filter-info" id="filter-info">
<span>🔍 Активный фильтр: <strong id="filter-name"></strong></span>
<button class="btn btn-clear" onclick="clearFilter()">✕ Сбросить</button>
</div>
<div class="stats">
<div class="stat total" onclick="filterTable('all')"><div class="stat-num">{total_leases}</div><div class="stat-label">Всего записей</div></div>
<div class="stat active" onclick="filterTable('active')"><div class="stat-num">{total_active}</div><div class="stat-label">Активных лизов</div></div>
<div class="stat active" onclick="filterTable('online')"><div class="stat-num">{total_online}</div><div class="stat-label">🟢 Active</div></div>
<div class="stat inactive" onclick="filterTable('inactive')"><div class="stat-num">{total_inactive}</div><div class="stat-label">🔴 Inactive</div></div>
<div class="stat spoofed" onclick="filterTable('spoofed')"><div class="stat-num">{total_spoofed}</div><div class="stat-label">👻 Spoofed</div></div>
<div class="stat reserved" onclick="filterTable('reserved')"><div class="stat-num">{total_reserved}</div><div class="stat-label">Постоянных</div></div>
</div>
''')
                subnet_ids = sorted(set(d['subnet_id'] for d in leases_by_subnet.values()) | set(subnets_config.keys()))
                html_parts.append('<div class="subnet-tabs">')
                for subnet_id in subnet_ids:
                    subnet_info = subnets_config.get(subnet_id, {})
                    subnet_str = subnet_info.get('subnet', f'Подсеть {subnet_id}')
                    lease_count = len(leases_by_subnet.get(f"{subnet_id}|0", {'leases': []}).get('leases', []))
                    active_class = ' active' if subnet_id == subnet_ids[0] else ''
                    html_parts.append(
                        f'<button class="subnet-tab{active_class}" data-subnet="{subnet_id}" onclick="switchSubnet({subnet_id})">📡 {subnet_str} <span class="tab-count">{lease_count}</span></button>')
                html_parts.append('</div>')
                for subnet_id in subnet_ids:
                    subnet_info = subnets_config.get(subnet_id, {})
                    subnet_str = subnet_info.get('subnet', f'Подсеть {subnet_id}')
                    interface = subnet_info.get('interface', '')
                    content_active_class = ' active' if subnet_id == subnet_ids[0] else ''
                    html_parts.append(
                        f'<div class="subnet-content{content_active_class}" id="subnet-content-{subnet_id}">')
                    pools = subnet_info.get('pools', [{'pool': 'N/A'}])
                    for pool_idx, pool in enumerate(pools):
                        pool_range = pool.get('pool', 'N/A')
                        pool_data = leases_by_subnet.get(f"{subnet_id}|{pool_idx}", {'leases': []})
                        leases = pool_data['leases']
                        count_active = sum(1 for l in leases if l.get('ping_status') == True)
                        count_reserved = sum(1 for l in leases if l.get('is_reserved', False))
                        pool_total = count_ips_in_pool(pool_range)
                        count_active_non_reserved = sum(
                            1 for l in leases if l.get('ping_status') == True and not l.get('is_reserved', False))
                        count_free = max(0, pool_total - count_reserved - count_active_non_reserved)
                        html_parts.append(f'''<div class="pool-info">
<div class="pool-stats"><strong>Пул {pool_idx + 1}:</strong> {pool_range} | Всего в пуле: <strong>{pool_total}</strong> | Активно: <strong>{count_active}</strong> | Постоянные: <strong>{count_reserved}</strong> | Свободно: <strong>{count_free}</strong></div>
<div class="pool-actions">
<button class="btn btn-ping" onclick="location.href='/?action=scan'">🔄 Сеть</button>
<button class="btn btn-export" onclick="location.href='/?action=export_xlsx'">📊 В .xlsx</button>
<button class="btn btn-dns" onclick="location.href='/?action=refresh_dns'">⬇️ DNS</button>
</div>
</div>''')
                        if leases:
                            html_parts.append(
                                '''<table><thead><tr><th>IP</th><th>MAC</th><th>Hostname</th><th>Expires</th><th>Status</th><th>Actions</th></tr></thead><tbody>''')
                            for lease in sorted(leases, key=lambda x: tuple(map(int, x['ip'].split('.')))):
                                ip_esc = esc(lease['ip'])
                                mac_esc = esc(lease['mac'])
                                hostname_clean = clean_hostname(lease.get('hostname', ''))
                                hostname_esc = esc(hostname_clean)
                                is_active = lease.get('is_active', False)
                                is_reserved = lease.get('is_reserved', False)
                                ping_status = lease.get('ping_status')
                                expire_ts = lease.get('expire', 0)
                                is_spoofed = lease.get('is_spoofed', False)
                                row_classes = []
                                if is_active: row_classes.append('row-active')
                                if is_reserved: row_classes.append('row-reserved')
                                if ping_status == True and not is_spoofed:
                                    row_classes.append('row-online')
                                elif ping_status == False:
                                    row_classes.append('row-inactive')
                                elif is_spoofed:
                                    row_classes.append('row-spoofed')
                                else:
                                    row_classes.append('row-unknown')
                                row_style = get_row_style(is_active, is_reserved, ping_status, is_spoofed)
                                ping_badge = get_ping_badge(ping_status, is_spoofed)
                                actions = []
                                if is_active and not is_reserved:
                                    actions.append(f'''<form method="POST" class="form-inline" style="margin-right:6px;">
<input type="hidden" name="action" value="reserve">
<input type="hidden" name="mac" value="{mac_esc}">
<input type="hidden" name="ip" value="{ip_esc}">
<input type="hidden" name="hostname" value="{hostname_esc}">
<button type="submit" class="btn btn-reserve">🔒</button>
</form>''')
                                if is_active and not is_reserved:
                                    actions.append(f'''<form method="POST" class="form-inline" style="margin-right:6px;">
<input type="hidden" name="action" value="delete_lease">
<input type="hidden" name="mac" value="{mac_esc}">
<input type="hidden" name="ip" value="{ip_esc}">
<button type="submit" class="btn btn-delete" onclick="return confirm('Удалить лиз?')">🗑️</button>
</form>''')
                                if is_reserved:
                                    actions.append(
                                        f'''<span class="edit-wrapper" data-ip="{ip_esc}" data-mac="{mac_esc}" data-old-hostname="{hostname_esc}">
<input type="text" class="edit-input" value="{hostname_esc}" autocomplete="off">
<button type="button" class="btn btn-edit" onclick="startEdit(this, '{ip_esc}')">✏️</button>
<button type="button" class="btn btn-save" onclick="saveEdit(this)">💾</button>
<button type="button" class="btn btn-cancel" onclick="cancelEdit(this)">✕</button>
</span>''')
                                    actions.append(f'''<form method="POST" class="form-inline" style="margin-left:6px;">
<input type="hidden" name="action" value="unreserve">
<input type="hidden" name="mac" value="{mac_esc}">
<input type="hidden" name="ip" value="{ip_esc}">
<button type="submit" class="btn btn-unreserve" onclick="return confirm('Снять резерв?')">🔓 Снять</button>
</form>''')
                                if is_spoofed:
                                    actions.append(f'''<form method="POST" class="form-inline">
<input type="hidden" name="action" value="reserve">
<input type="hidden" name="mac" value="UNKNOWN">
<input type="hidden" name="ip" value="{ip_esc}">
<button type="submit" class="btn btn-reserve">🔒</button>
</form>''')
                                actions_html = ''.join(actions) if actions else '—'
                                html_parts.append(f'''<tr class="filter-row {' '.join(row_classes)}" style="{row_style}">
<td><strong>{ip_esc}</strong></td>
<td><code>{mac_esc}</code></td>
<td>{hostname_esc}</td>
<td>{format_expire(expire_ts, is_reserved, is_spoofed)}</td>
<td>{ping_badge}</td>
<td>{actions_html}</td>
</tr>''')
                            html_parts.append('</tbody></table>')
                        else:
                            html_parts.append('<p style="color:#999;font-size:13px;padding:10px;">📭 Нет записей</p>')
                    html_parts.append('</div>')
                html_parts.append('''<div class="footer">
<strong>Paths:</strong> Config: /etc/kea/kea-dhcp4.conf | Socket: /run/kea/dhcp4.sock | Ping Cache: /var/lib/kea/ping_cache.json<br>
<strong>Logs:</strong> /var/log/kea-manager.log | <a href="/" style="color:#0066cc;">🔄 Обновить</a>
</div>
</div>
</body>
</html>''')
                html = ''.join(html_parts)
                network_checker._log(f"HTML generation completed, length={len(html)}")
                return html

            def log_message(self, format, *args):
                pass

        return KeaHandler

    def start(self):
        socketserver.TCPServer.allow_reuse_address = True
        with socketserver.TCPServer(("", self.port), self.handler_class) as httpd:
            print(f"✓ Kea DHCP Manager v{VERSION} running on http://0.0.0.0:{self.port}/")
            httpd.serve_forever()
