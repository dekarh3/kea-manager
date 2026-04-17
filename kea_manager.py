#!/usr/bin/env python3
# Kea DHCP Manager v4.0.0 - Main Entry Point

import sys
import threading
import os

sys.path.insert(0, '/opt/kea-manager')

from config import PORT, VERSION, KEA_SOCKET, LOG_FILE
from modules.kea import KeaManager
from modules.checks import NetworkChecker
from modules.web import WebServer


def main():
    print(f"🚀 Starting Kea DHCP Manager v{VERSION}...")

    # Создаем директорию для логов если не существует
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    # Инициализация компонентов
    kea_manager = KeaManager()
    network_checker = NetworkChecker()
    ping_in_progress_flag = {'value': False}

    # Запуск начальной проверки ping
    print("🔍 Запуск начальной проверки ping...")
    initial_ping_thread = threading.Thread(
        target=network_checker.run_ping_check,
        args=(kea_manager, ping_in_progress_flag)
    )
    initial_ping_thread.daemon = True
    initial_ping_thread.start()

    # Запуск веб-сервера
    web_server = WebServer(PORT, kea_manager, network_checker, ping_in_progress_flag)
    print(f"✓ Socket: {KEA_SOCKET}")
    print(f"✓ Web: http://0.0.0.0:{PORT}/")
    print(f"✓ Logs: {LOG_FILE}")
    web_server.start()


if __name__ == '__main__':
    main()