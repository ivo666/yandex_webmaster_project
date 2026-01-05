"""Yandex Webmaster data loader."""
import requests
import logging
from typing import List, Dict, Any
from datetime import datetime

from config.settings import settings
from models.database import get_db, WebmasterData

logger = logging.getLogger(__name__)


class WebmasterDataLoader:
    """Загрузчик данных из Яндекс.Вебмастер API."""
    
    def __init__(self):
        self.base_url = settings.api.base_url
        self.headers = {
            "Authorization": f"OAuth {settings.api.token}",
            "Content-Type": "application/json"
        }
        self.user_id = settings.api.user_id
        self.host_id = settings.api.host_id
    
    def get_all_urls_for_date(self, target_date: str) -> List[str]:
        """Получает все уникальные URL для указанной даты"""
        logger.debug(f"Получение URL для даты: {target_date}")
        
        url = f"{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/query-analytics/list"
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
                logger.debug(f"Отправка запроса: offset={offset}, limit={limit}")
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                logger.debug(f"Статус ответа: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    stats_list = data.get('text_indicator_to_statistics', [])
                    logger.debug(f"Получено элементов: {len(stats_list)}")

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
                    logger.error(f"Ошибка {response.status_code}: {response.text[:200]}")
                    break
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                break

        return list(urls)

    def get_queries_for_url_and_date(self, target_date: str, page_url: str, device: str) -> List[Dict[str, Any]]:
        """Получает запросы для URL и устройства."""
        logger.debug(f"Получение запросов для URL: {page_url}, устройство: {device}")
        
        url = f"{self.base_url}/user/{self.user_id}/hosts/{self.host_id}/query-analytics/list"
        payload = {
            "limit": 500,
            "text_indicator": "QUERY",
            "device_type_indicator": device,
            "filters": {
                "text_filters": [{
                    "text_indicator": "URL",
                    "operation": "TEXT_MATCH",
                    "value": page_url
                }]
                # УБИРАЕМ фильтр DEMAND > 0 - сохраняем все данные
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            logger.debug(f"Статус ответа: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                data_rows = []
                
                for item in data.get('text_indicator_to_statistics', []):
                    query_text = item.get('text_indicator', {}).get('value', 'N/A')
                    metrics = {}
                    
                    for stat in item.get('statistics', []):
                        if stat.get('date') == target_date:
                            metrics[stat.get('field')] = stat.get('value', 0)
                    
                    # Сохраняем ВСЕ данные, даже с DEMAND = 0
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
                
                logger.debug(f"Найдено записей: {len(data_rows)}")
                return data_rows
            else:
                logger.error(f"Ошибка {response.status_code}: {response.text[:200]}")
                return []
                
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return []

    def save_to_database(self, records: List[Dict[str, Any]]) -> int:
        """Сохраняет записи в базу данных."""
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
                
                logger.info(f"Сохранено {saved_count} новых записей")
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении: {e}")
        
        return saved_count
    
    def load_date(self, target_date: str) -> int:
        """Загружает данные за указанную дату."""
        logger.info(f"Загрузка данных за {target_date}")
        
        # 1. Получаем URL
        urls = self.get_all_urls_for_date(target_date)
        logger.info(f"Найдено URL: {len(urls)}")
        
        if not urls:
            return 0
        
        # 2. Для каждого URL получаем данные
        all_records = []
        device_types = ['DESKTOP', 'MOBILE', 'TABLET']
        
        for i, page_url in enumerate(urls, 1):
            logger.debug(f"Обработка URL {i}/{len(urls)}: {page_url[:50]}...")
            
            for device in device_types:
                records = self.get_queries_for_url_and_date(target_date, page_url, device)
                # Фильтруем записи с demand > 0
                filtered_records = [r for r in records if r['demand'] > 0]
                all_records.extend(filtered_records)
        
        # 3. Сохраняем в БД
        if all_records:
            saved = self.save_to_database(all_records)
            logger.info(f"Итого сохранено записей за {target_date}: {saved}")
            return saved
        
        return 0
