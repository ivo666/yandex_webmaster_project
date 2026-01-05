from typing import List, Set
from datetime import datetime, timedelta
from config.settings import settings
from models.database import get_db, WebmasterData
from api.webmaster_client import WebmasterClient


class DateManager:
    def __init__(self, client: WebmasterClient):
        self.client = client

    def get_existing_dates(self) -> Set[str]:
        existing_dates = set()
        try:
            with get_db() as db:
                dates = db.query(WebmasterData.date).distinct().all()
                existing_dates = {date[0].strftime('%Y-%m-%d') for date in dates}
        except Exception as e:
            print(f"Error getting dates from DB: {e}")
        return existing_dates

    def get_missing_dates(self) -> List[str]:
        existing_dates = self.get_existing_dates()

        # Генерируем последние N дней
        days_back = settings.app.days_back
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
