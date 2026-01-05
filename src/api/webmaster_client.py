import requests
from typing import List, Dict, Any
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import settings


class WebmasterClient:
    def __init__(self):
        self.base_url = settings.api.base_url
        self.headers = {
            'Authorization': f'OAuth {settings.api.token}',
            'Content-Type': 'application/json'
        }
        self.user_id = settings.api.user_id
        self.host_id = settings.api.host_id

    def check_date_has_data(self, target_date: str) -> bool:
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/query-analytics/list'
        payload = {
            "limit": 1,
            "text_indicator": "QUERY",
            "filters": {
                "statistic_filters": [{
                    "statistic_field": "IMPRESSIONS",
                    "operation": "GREATER_THAN",
                    "value": "0",
                    "from": target_date,
                    "to": target_date
                }]
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                return len(data.get('text_indicator_to_statistics', [])) > 0
            return False
        except:
            return False

    def get_urls_for_date(self, target_date: str) -> List[str]:
        """Получает все уникальные URL для указанной даты"""
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/query-analytics/list'
        urls = set()
        offset = 0
        limit = 500

        while True:
            payload = {
                "offset": offset,
                "limit": limit,
                "text_indicator": "URL",
                "filters": {
                    "statistic_filters": [{
                        "statistic_field": "IMPRESSIONS",
                        "operation": "GREATER_THAN",
                        "value": "0",
                        "from": target_date,
                        "to": target_date
                    }]
                }
            }

            try:
                response = requests.post(url, headers=self.headers, json=payload)
                if response.status_code == 200:
                    data = response.json()
                    stats_list = data.get('text_indicator_to_statistics', [])

                    if not stats_list:
                        break

                    for item in stats_list:
                        url_value = item.get('text_indicator', {}).get('value', '')
                        if url_value and url_value != 'N/A':
                            urls.add(url_value)

                    if len(stats_list) < limit:
                        break

                    offset += limit
                else:
                    print(f"Ошибка при получении URL за {target_date}: {response.status_code}")
                    break
            except Exception as e:
                print(f"Ошибка при получении URL: {e}")
                break

        return list(urls)

    def get_queries_for_url_and_date(self, target_date: str, page_url: str, device: str) -> List[Dict[str, Any]]:
        url = f'{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/query-analytics/list'
        payload = {
            "limit": 500,
            "text_indicator": "QUERY",
            "device_type_indicator": device,
            "filters": {
                "text_filters": [{
                    "text_indicator": "URL",
                    "operation": "TEXT_MATCH",
                    "value": page_url
                }],
                "statistic_filters": [{
                    "statistic_field": "DEMAND",
                    "operation": "GREATER_THAN",
                    "value": "0",
                    "from": target_date,
                    "to": target_date
                }]
            }
        }

        data_rows = []

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                for item in data.get('text_indicator_to_statistics', []):
                    query_text = item.get('text_indicator', {}).get('value', 'N/A')
                    metrics = {}
                    for stat in item.get('statistics', []):
                        if stat.get('date') == target_date:
                            metrics[stat.get('field')] = stat.get('value', 0)

                    if metrics.get('DEMAND', 0) > 0:
                        data_rows.append({
                            'date': datetime.strptime(target_date, '%Y-%m-%d').date(),
                            'page_path': page_url,
                            'query': query_text,
                            'demand': int(metrics.get('DEMAND', 0)),
                            'impressions': int(float(metrics.get('IMPRESSIONS', 0))),
                            'clicks': int(float(metrics.get('CLICKS', 0))),
                            'position': float(metrics.get('POSITION', 0)),
                            'device': device.lower()
                        })
        except Exception as e:
            print(f"Error: {e}")

        return data_rows
