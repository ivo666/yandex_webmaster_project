import requests
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
        """Проверяет есть ли данные за указанную дату в Яндекс.Вебмастер."""
        # В API v4 endpoint может отличаться от v4.2
        # Пробуем стандартный endpoint
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/search-queries'
        params = {
            "date_from": target_date,
            "date_to": target_date,
            "limit": 1
        }

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # В v4 структура ответа может быть другой
                return data.get('queries') and len(data.get('queries', [])) > 0
            return False
        except:
            return False

    def get_urls_for_date(self, target_date: str) -> List[str]:
        """Получает все уникальные URL для указанной даты - АДАПТИРУЕМ ПОД v4"""
        # В v4 нужно использовать другой подход
        # Пока упростим - будем использовать старый endpoint если работает
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/search-queries'
        urls = set()
        offset = 0
        limit = 500

        while True:
            params = {
                "date_from": target_date,
                "date_to": target_date,
                "limit": limit,
                "offset": offset
            }

            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    queries = data.get('queries', [])

                    if not queries:
                        break

                    for query in queries:
                        url_value = query.get('page_url', '')
                        if url_value and url_value != 'N/A':
                            urls.add(url_value)

                    if len(queries) < limit:
                        break

                    offset += limit
                else:
                    print(f"Ошибка при получении URL за {target_date}: {response.status_code}")
                    print(f"Response: {response.text[:200]}")
                    break
            except Exception as e:
                print(f"Ошибка при получении URL: {e}")
                break

        return list(urls)

    def get_queries_for_url_and_date(self, target_date: str, page_url: str, device: str) -> List[Dict[str, Any]]:
        """В v4 может не быть такого endpoint - нужно адаптировать"""
        # Упрощенная версия для v4
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/search-queries'
        params = {
            "date_from": target_date,
            "date_to": target_date,
            "limit": 500
        }

        data_rows = []

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                for query in data.get('queries', []):
                    if query.get('page_url') == page_url:
                        data_rows.append({
                            'date': datetime.strptime(target_date, '%Y-%m-%d').date(),
                            'page_path': page_url,
                            'query': query.get('query_text', 'N/A'),
                            'demand': int(query.get('impressions', 0)),  # В v4 может не быть demand
                            'impressions': int(float(query.get('impressions', 0))),
                            'clicks': int(float(query.get('clicks', 0))),
                            'position': float(query.get('position', 0)),
                            'device': device.lower()
                        })
        except Exception as e:
            print(f"Error: {e}")

        return data_rows
