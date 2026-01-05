from typing import List, Dict, Any
from models.database import get_db, WebmasterData
from api.webmaster_client import WebmasterClient


class DataLoader:
    def __init__(self, client: WebmasterClient):
        self.client = client
        self.device_types = ['DESKTOP', 'MOBILE', 'TABLET']

    def load_data_for_date(self, target_date: str) -> int:
        print(f"Загрузка данных за {target_date}...")

        # Получаем все URL для даты
        urls = self.client.get_urls_for_date(target_date)
        print(f"Найдено {len(urls)} URL для обработки")

        if not urls:
            print(f"Нет URL с данными за {target_date}")
            return 0

        total_records = 0

        for url in urls:
            for device in self.device_types:
                records = self.client.get_queries_for_url_and_date(target_date, url, device)
                saved = self._save_records(records)
                total_records += saved

        print(f"Загружено {total_records} записей за {target_date}")
        return total_records

    def _save_records(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0

        saved_count = 0
        try:
            with get_db() as db:
                for record_data in records:
                    # Проверяем дубликаты
                    exists = db.query(WebmasterData).filter(
                        WebmasterData.date == record_data['date'],
                        WebmasterData.page_path == record_data['page_path'],
                        WebmasterData.query == record_data['query'],
                        WebmasterData.device == record_data['device']
                    ).first()

                    if not exists:
                        record = WebmasterData(**record_data)
                        db.add(record)
                        saved_count += 1

                print(f"Добавлено {saved_count} новых записей")

        except Exception as e:
            print(f"Ошибка при сохранении: {e}")

        return saved_count
