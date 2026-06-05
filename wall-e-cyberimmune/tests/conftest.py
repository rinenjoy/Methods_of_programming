"""
Конфигурация pytest: отключение системного HTTP-прокси для localhost.

На Windows с VPN/Shadowsocks/V2Ray системный прокси (127.0.0.1:12334)
перехватывает ВСЕ HTTP-запросы, включая localhost. Это ломает тесты,
которые ходят к Docker-сервисам на localhost:8000.

Фикс: полностью удаляем прокси из окружения И патчим requests.Session
чтобы он НИКОГДА не использовал системный прокси.
"""
import os

# 1. Удаляем ВСЕ прокси-переменные из окружения
for var in list(os.environ.keys()):
    if 'proxy' in var.lower():
        del os.environ[var]

# 2. Устанавливаем NO_PROXY на * (обходить прокси для всех адресов)
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

# 3. Monkey-patch requests.Session: каждая новая сессия создаётся с trust_env=False
#    Это гарантирует что requests не будет читать прокси из Windows-реестра
import requests
import requests.adapters

_original_session_init = requests.Session.__init__

def _patched_session_init(self, *args, **kwargs):
    _original_session_init(self, *args, **kwargs)
    self.trust_env = False
    self.proxies = {}

requests.Session.__init__ = _patched_session_init
