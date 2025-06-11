from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Boolean
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class DailyStats(Base):
    __tablename__ = "daily_stats"

    date = Column(Date, primary_key=True)
    campaign_id = Column(String, primary_key=True)
    spend = Column(Float, nullable=False)
    conversions = Column(Integer, nullable=False)
    cpa = Column(Float, nullable=True)

    def __repr__(self):
        return (
            f"<DailyStats(date={self.date}, campaign_id='{self.campaign_id}', "
            f"spend={self.spend}, conversions={self.conversions}, cpa={self.cpa})>"
        )


class LastUpdateTime(Base):
    __tablename__ = "last_update_time"

    date = Column(Date, primary_key=True)
    last_updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    is_complete = Column(Boolean, default=False)

    def __repr__(self):
        return (
            f"<LastUpdateTime(date={self.date}, last_updated_at={self.last_updated_at}, "
            f"is_complete={self.is_complete})>"
        )
