from sqlalchemy.orm import Session
from app.models import DailyStats, LastUpdateTime
from datetime import date, datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DailyStatsCRUD:
    def __init__(self, db_session: Session):
        self.db = db_session

    def get_daily_stat(self, record_date: date, campaign_id: str) -> Optional[DailyStats]:
        """Получает статистику по дате и ID кампании."""
        return self.db.query(DailyStats).filter_by(
            date=record_date, campaign_id=campaign_id
        ).first()

    def create_daily_stat(
        self,
        record_date: date,
        campaign_id: str,
        spend: float,
        conversions: int,
        cpa: Optional[float] = None
    ) -> DailyStats:
        """Создает новую запись DailyStats."""
        db_stat = DailyStats(
            date=record_date,
            campaign_id=campaign_id,
            spend=spend,
            conversions=conversions,
            cpa=cpa
        )
        self.db.add(db_stat)
        self.db.commit()
        self.db.refresh(db_stat)
        logger.debug(f"Создана новая запись: {record_date} - {campaign_id}")
        return db_stat

    def update_daily_stat(
        self,
        db_stat: DailyStats,
        spend: float,
        conversions: int,
        cpa: Optional[float] = None
    ) -> DailyStats:
        """Обновляет существующую запись DailyStats."""
        db_stat.spend = spend
        db_stat.conversions = conversions
        db_stat.cpa = cpa
        self.db.commit()
        self.db.refresh(db_stat)
        logger.debug(f"Обновлена существующая запись: {db_stat.date} - {db_stat.campaign_id}")
        return db_stat

    def upsert_daily_stat(
        self,
        record_date: date,
        campaign_id: str,
        spend: float,
        conversions: int,
        cpa: Optional[float] = None
    ) -> DailyStats:
        """
        Создание или обновление записи DailyStats.
        Если запись существует, она обновляется; иначе создается новый.
        """
        existing_stat = self.get_daily_stat(record_date, campaign_id)

        if existing_stat:
            return self.update_daily_stat(existing_stat, spend, conversions, cpa)
        else:
            return self.create_daily_stat(record_date, campaign_id, spend, conversions, cpa)


class LastUpdateTimeCRUD:
    def __init__(self, db_session: Session):
        self.db = db_session

    def get_last_update_info(self, record_date: date) -> Optional[LastUpdateTime]:
        return self.db.query(LastUpdateTime).filter_by(date=record_date).first()

    def set_last_update_info(self, record_date: date, is_complete: bool = False) -> LastUpdateTime:
        existing_info = self.get_last_update_info(record_date)
        if existing_info:
            existing_info.last_updated_at = datetime.utcnow()
            existing_info.is_complete = is_complete
            self.db.commit()
            self.db.refresh(existing_info)
            logger.debug(f"Обновлено LastUpdateTime для {record_date}: complete={is_complete}")
            return existing_info
        else:
            new_info = LastUpdateTime(date=record_date, last_updated_at=datetime.utcnow(), is_complete=is_complete)
            self.db.add(new_info)
            self.db.commit()
            self.db.refresh(new_info)
            logger.debug(f"Обновлено LastUpdateTime для {record_date}: complete={is_complete}")
            return new_info
