"""
ETL процессор v2 - исправленная работа с автоинкрементом.
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

class WebmasterETLProcessorV2:
    """ETL процессор v2 с исправленной работой с ID"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.batch_size = 1000  # Уменьшим размер батча
    
    def get_processing_status(self) -> Dict[str, Any]:
        """Получить текущий статус обработки"""
        with self.db_manager.get_session() as session:
            last_rdl_date = session.query(
                func.max(WebmApi.date)
            ).scalar()
            
            last_ppl_date = session.query(
                func.max(WebmasterAggregated.date)
            ).scalar()
            
            unprocessed_count = 0
            
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
            
            # Обрабатываем небольшими батчами
            with self.db_manager.get_session() as session:
                if status['ppl_empty']:
                    # Обрабатываем все данные порциями
                    offset = 0
                    total = status['unprocessed_count']
                    
                    while offset < total:
                        batch = session.query(WebmApi).order_by(
                            WebmApi.date, WebmApi.id
                        ).offset(offset).limit(self.batch_size).all()
                        
                        if not batch:
                            break
                        
                        for record in batch:
                            try:
                                # Создаем aggregated запись без указания id
                                aggregated = WebmasterAggregated(
                                    date=record.date,
                                    query=record.query,
                                    page_path=record.page_path,
                                    device=record.device,
                                    demand=record.demand,
                                    impressions=record.impressions,
                                    clicks=record.clicks,
                                    position=record.position
                                )
                                
                                session.add(aggregated)
                                
                                # После коммита получим id
                                session.flush()  # Теперь это должно работать
                                
                                # Создаем позиции и клики
                                self._create_positions(session, aggregated, record)
                                self._create_clicks(session, aggregated, record)
                                
                                stats['aggregated_inserted'] += 1
                                
                            except Exception as e:
                                logger.error(f"Ошибка записи {record.id}: {e}")
                                stats['errors'] += 1
                                session.rollback()
                                continue
                        
                        offset += len(batch)
                        session.commit()  # Коммитим батч
                        logger.info(f"   Обработано {offset}/{total} записей")
                        
                else:
                    # Инкрементальная обработка только новых дат
                    dates_to_process = session.query(
                        func.distinct(WebmApi.date)
                    ).filter(
                        WebmApi.date > status['last_ppl_date']
                    ).order_by(WebmApi.date).all()
                    
                    dates_to_process = [d[0] for d in dates_to_process]
                    
                    for date in dates_to_process:
                        records = session.query(WebmApi).filter(
                            WebmApi.date == date
                        ).all()
                        
                        for record in records:
                            try:
                                aggregated = WebmasterAggregated(
                                    date=record.date,
                                    query=record.query,
                                    page_path=record.page_path,
                                    device=record.device,
                                    demand=record.demand,
                                    impressions=record.impressions,
                                    clicks=record.clicks,
                                    position=record.position
                                )
                                
                                session.add(aggregated)
                                session.flush()
                                
                                self._create_positions(session, aggregated, record)
                                self._create_clicks(session, aggregated, record)
                                
                                stats['aggregated_inserted'] += 1
                                
                            except Exception as e:
                                logger.error(f"Ошибка записи {record.id}: {e}")
                                stats['errors'] += 1
                                continue
                        
                        session.commit()
                        if records:
                            logger.info(f"   📅 {date}: {len(records)} записей")
            
            duration = int(time.time() - start_time)
            logger.info(f"✅ Обработка завершена за {duration}с")
            
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            stats['status'] = 'failed'
        
        return stats
    
    def _create_positions(self, session, aggregated, record):
        """Создание записей позиций показов"""
        impressions = min(record.impressions, record.demand)  # Применяем бизнес-логику
        
        if impressions > 0:
            base_position = int(record.position)
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
    
    def _create_clicks(self, session, aggregated, record):
        """Создание записей кликов"""
        clicks = min(record.clicks, min(record.impressions, record.demand))  # Бизнес-логика
        
        if clicks > 0:
            base_position = int(record.position)
            for i in range(clicks):
                position = base_position if i < clicks * 0.8 else max(1, base_position - 1)
                
                click_record = WebmasterClicks(
                    id=aggregated.id,
                    click_position=position,
                    impression_order=i + 1
                )
                session.add(click_record)
    
    def process_full_reload(self) -> Dict[str, Any]:
        """Полная перезагрузка данных"""
        logger.warning("⚠️ Запуск полной перезагрузки!")
        
        with self.db_manager.get_session() as session:
            session.query(WebmasterClicks).delete()
            session.query(WebmasterPositions).delete()
            session.query(WebmasterAggregated).delete()
            session.commit()
            logger.info("✅ PPL таблицы очищены")
        
        return self.process_incremental()
