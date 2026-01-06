"""
Исправленный ETL процессор.
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any
from sqlalchemy import func

from src.core.database import DatabaseManager
from src.models.rdl.models import WebmApi
from src.models.ppl import (
    WebmasterAggregated,
    WebmasterPositions,
    WebmasterClicks
)

logger = logging.getLogger(__name__)

class WebmasterETLProcessorFixed:
    """Исправленный ETL процессор"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.batch_size = 5000
        
    def get_processing_status(self) -> Dict[str, Any]:
        """Получить текущий статус обработки"""
        with self.db_manager.get_session() as session:
            # Последняя дата в rdl
            last_rdl_date = session.query(
                func.max(WebmApi.date)
            ).scalar()
            
            # Последняя обработанная дата в ppl
            last_ppl_date = session.query(
                func.max(WebmasterAggregated.date)
            ).scalar()
            
            # Количество необработанных записей
            unprocessed_count = 0
            
            # Если PPL пустая, то все записи из RDL необработанные
            if last_ppl_date is None:
                unprocessed_count = session.query(WebmApi).count()
                needs_processing = unprocessed_count > 0
            elif last_rdl_date and last_rdl_date > last_ppl_date:
                unprocessed_count = session.query(WebmApi).filter(
                    WebmApi.date > last_ppl_date
                ).count()
                needs_processing = True
            else:
                needs_processing = False
            
            return {
                'last_rdl_date': last_rdl_date,
                'last_ppl_date': last_ppl_date,
                'unprocessed_count': unprocessed_count,
                'needs_processing': needs_processing,
                'ppl_empty': last_ppl_date is None
            }
    
    def process_incremental(self) -> Dict[str, Any]:
        """Инкрементальная обработка"""
        start_time = time.time()
        stats = {
            'process_type': 'incremental',
            'status': 'success',
            'aggregated_inserted': 0,
            'positions_created': 0,
            'clicks_created': 0,
            'errors': 0
        }
        
        try:
            status = self.get_processing_status()
            
            if not status['needs_processing']:
                logger.info("✅ Нет новых данных для обработки")
                stats['status'] = 'skipped'
                return stats
            
            logger.info(f"🚀 Начало обработки")
            logger.info(f"   Необработанных записей: {status['unprocessed_count']}")
            
            with self.db_manager.get_session() as session:
                # Если PPL пустая, обрабатываем все даты
                if status['ppl_empty']:
                    dates_to_process = session.query(
                        func.distinct(WebmApi.date)
                    ).order_by(WebmApi.date).all()
                else:
                    # Иначе только даты после последней обработанной
                    dates_to_process = session.query(
                        func.distinct(WebmApi.date)
                    ).filter(
                        WebmApi.date > status['last_ppl_date']
                    ).order_by(WebmApi.date).all()
                
                dates_to_process = [d[0] for d in dates_to_process]
                
                logger.info(f"   Будет обработано {len(dates_to_process)} дней")
                
                for date in dates_to_process:
                    day_stats = self._process_date(session, date)
                    stats['aggregated_inserted'] += day_stats['aggregated_inserted']
                    stats['positions_created'] += day_stats['positions_created']
                    stats['clicks_created'] += day_stats['clicks_created']
                    stats['errors'] += day_stats['errors']
                    
                    if day_stats['aggregated_inserted'] > 0:
                        logger.info(f"   📅 {date}: {day_stats['aggregated_inserted']} записей")
                
                session.commit()
            
            duration = int(time.time() - start_time)
            logger.info(f"✅ Обработка завершена за {duration}с")
            
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            stats['status'] = 'failed'
        
        return stats
    
    def _process_date(self, session, date) -> Dict[str, int]:
        """Обработка данных за конкретную дату"""
        stats = {
            'aggregated_inserted': 0,
            'positions_created': 0,
            'clicks_created': 0,
            'errors': 0
        }
        
        try:
            # Получаем записи за дату
            records = session.query(WebmApi).filter(
                WebmApi.date == date
            ).all()
            
            for record in records:
                try:
                    # Применяем бизнес-логику
                    processed_data = self._apply_business_logic(record)
                    
                    # Создаем запись в aggregated
                    aggregated = WebmasterAggregated(
                        date=processed_data['date'],
                        query=processed_data['query'],
                        page_path=processed_data['page_path'],
                        device=processed_data['device'],
                        demand=processed_data['demand'],
                        impressions=processed_data['impressions'],
                        clicks=processed_data['clicks'],
                        position=processed_data['position']
                    )
                    
                    session.add(aggregated)
                    session.flush()
                    
                    # Создаем позиции и клики
                    stats['positions_created'] += self._create_positions(session, aggregated, processed_data)
                    stats['clicks_created'] += self._create_clicks(session, aggregated, processed_data)
                    
                    stats['aggregated_inserted'] += 1
                    
                except Exception as e:
                    logger.error(f"Ошибка записи {record.id}: {e}")
                    stats['errors'] += 1
        
        except Exception as e:
            logger.error(f"Ошибка даты {date}: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _apply_business_logic(self, record: WebmApi) -> Dict[str, Any]:
        """Применение бизнес-логики"""
        processed = {
            'date': record.date,
            'query': record.query,
            'page_path': record.page_path,
            'device': record.device,
            'demand': record.demand,
            'impressions': record.impressions,
            'clicks': record.clicks,
            'position': record.position
        }
        
        # Бизнес-правила
        if processed['impressions'] > processed['demand']:
            processed['impressions'] = processed['demand']
        
        if processed['clicks'] > processed['impressions']:
            processed['clicks'] = processed['impressions']
        
        return processed
    
    def _create_positions(self, session, aggregated, data: Dict[str, Any]) -> int:
        """Создание записей позиций показов"""
        positions_created = 0
        impressions = data['impressions']
        
        if impressions > 0:
            base_position = int(data['position'])
            for i in range(impressions):
                if i < impressions * 0.7:
                    position = base_position
                elif i < impressions * 0.85:
                    position = max(1, base_position - 1)
                else:
                    position = min(10, base_position + 1)
                
                position_record = WebmasterPositions(
                    id=aggregated.id,
                    impression_position=position,
                    impression_order=i + 1
                )
                session.add(position_record)
                positions_created += 1
        
        return positions_created
    
    def _create_clicks(self, session, aggregated, data: Dict[str, Any]) -> int:
        """Создание записей кликов"""
        clicks_created = 0
        clicks = data['clicks']
        
        if clicks > 0:
            base_position = int(data['position'])
            for i in range(clicks):
                position = base_position if i < clicks * 0.8 else max(1, base_position - 1)
                
                click_record = WebmasterClicks(
                    id=aggregated.id,
                    click_position=position,
                    impression_order=i + 1
                )
                session.add(click_record)
                clicks_created += 1
        
        return clicks_created
    
    def process_full_reload(self) -> Dict[str, Any]:
        """Полная перезагрузка данных"""
        logger.warning("⚠️ Запуск полной перезагрузки!")
        
        # Очищаем ppl таблицы
        with self.db_manager.get_session() as session:
            session.query(WebmasterClicks).delete()
            session.query(WebmasterPositions).delete()
            session.query(WebmasterAggregated).delete()
            session.commit()
            logger.info("✅ PPL таблицы очищены")
        
        # Запускаем обработку
        return self.process_incremental()
