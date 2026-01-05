"""Configuration settings for Yandex Webmaster project."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем .env файл
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)


class Settings:
    """Упрощенные настройки без Pydantic (как в inmpc)"""

    # Database
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'yandex_webmaster')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')

    # API - ИСПРАВЛЕНО: используем v4 вместо v4.2
    API_TOKEN = os.getenv('API_TOKEN', '')
    BASE_URL = os.getenv('BASE_URL', 'https://api.webmaster.yandex.net/v4')  # v4!
    USER_ID = os.getenv('USER_ID', '238948933')
    HOST_ID = os.getenv('HOST_ID', 'https:profi-filter.ru:443')

    # App
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    DAYS_BACK = int(os.getenv('DAYS_BACK', 20))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 500))

    @property
    def db(self):
        class DB:
            @property
            def connection_string(self):
                return f'postgresql://{Settings.DB_USER}:{Settings.DB_PASSWORD}@{Settings.DB_HOST}:{Settings.DB_PORT}/{Settings.DB_NAME}'
        return DB()

    @property
    def api(self):
        class API:
            token = Settings.API_TOKEN
            base_url = Settings.BASE_URL
            user_id = Settings.USER_ID
            host_id = Settings.HOST_ID
        return API()

    @property
    def app(self):
        class App:
            log_level = Settings.LOG_LEVEL
            days_back = Settings.DAYS_BACK
            batch_size = Settings.BATCH_SIZE
        return App()


settings = Settings()
