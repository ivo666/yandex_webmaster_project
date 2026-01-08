from typing import List, Set
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

# Загружаем .env
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

# Простой объект настроек
class Settings:
    DB_NAME = os.getenv('DB_NAME', 'yandex_webmaster')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DAYS_BACK = int(os.getenv('DAYS_BACK', 20))

settings = Settings()

# Импортируем после загрузки настроек
import psycopg2
from ..api.webmaster_client import WebmasterClient


class DateManager:
    def __init__(self, client: WebmasterClient):
        self.client = client

    def get_existing_dates(self) -> Set[str]:
        existing_dates = set()
        try:
            conn = psycopg2.connect(
                dbname=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                host=settings.DB_HOST,
                port=settings.DB_PORT
            )
            cursor = conn.cursor()
            
            cursor.execute("SELECT DISTINCT date FROM rdl.webm_api ORDER BY date")
            dates = cursor.fetchall()
            existing_dates = {date[0].strftime('%Y-%m-%d') for date in dates}
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Error getting dates from DB: {e}")
        return existing_dates

    def get_missing_dates(self) -> List[str]:
        existing_dates = self.get_existing_dates()
        
        # Генерируем последние N дней
        days_back = settings.DAYS_BACK
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back - 1)
        
        all_dates = []
        current_date = start_date
        while current_date <= end_date:
            all_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        # Проверяем какие даты есть в API
        available_dates = []
        for date_str in all_dates:
            if self.client.check_date_has_data(date_str):
                available_dates.append(date_str)
        
        # Находим недостающие
        missing_dates = [date for date in available_dates if date not in existing_dates]
        
        print(f"Статистика: {len(available_dates)} доступно, {len(existing_dates)} в БД, {len(missing_dates)} отсутствует")
        return missing_dates
