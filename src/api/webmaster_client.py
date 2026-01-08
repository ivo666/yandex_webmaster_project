import requests
import json
from typing import List, Dict, Any
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

# Загружаем .env
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)


class WebmasterClient:
    def __init__(self):
        self.base_url = os.getenv('BASE_URL', 'https://api.webmaster.yandex.net/v4')
        self.headers = {
            'Authorization': f'OAuth {os.getenv("API_TOKEN", "")}',
            'Content-Type': 'application/json'
        }
        self.user_id = os.getenv('USER_ID', '238948933')
        self.host_id = os.getenv('HOST_ID', 'https:profi-filter.ru:443')

    def check_date_has_data(self, target_date: str) -> bool:
        """Проверяет есть ли данные за указанную дату в Яндекс.Вебмастер v4.2."""
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/query-analytics/list'
        
        payload = {
            "date_from": target_date,
            "date_to": target_date,
            "limit": 1,
            "filters": {
                "query_indicator": "TOTAL_SHOWS"
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Правильная проверка - есть ли count > 0
                count = data.get('count', 0)
                return count > 0
            elif response.status_code in [404, 400]:
                # Нет данных за эту дату
                return False
            else:
                print(f"  API Error {response.status_code}: {response.text[:100]}")
                return False
        except Exception as e:
            print(f"  Exception checking date {target_date}: {e}")
            return False

    def get_popular_queries(self, date_str: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Получает популярные запросы за указанную дату."""
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/query-analytics/list'
        
        payload = {
            "date_from": date_str,
            "date_to": date_str,
            "limit": limit,
            "filters": {
                "query_indicator": "TOTAL_SHOWS"
            },
            "order_by": "TOTAL_SHOWS_DESC"
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                # Правильный путь к данным
                if 'text_indicator_to_statistics' in data:
                    # Преобразуем структуру если нужно
                    return self._transform_response(data['text_indicator_to_statistics'])
                return []
            else:
                print(f"Error getting queries for {date_str}: {response.status_code}")
                return []
        except Exception as e:
            print(f"Exception getting queries for {date_str}: {e}")
            return []

    def _transform_response(self, data: Dict) -> List[Dict[str, Any]]:
        """Преобразует ответ API в стандартный формат."""
        # TODO: Нужно понять структуру text_indicator_to_statistics
        # и преобразовать в список queries
        print(f"  Need to transform data structure: {list(data.keys())[:3] if data else 'empty'}")
        return []
