from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class SpendEntry:
    """Представляет запись о расходах, полученную из API."""
    date: str
    campaign_id: str
    spend: float

@dataclass
class ConversionEntry:
    """Представляет запись о конверсиях, полученную из API."""
    date: str
    campaign_id: str
    conversions: int

@dataclass
class CombinedDailyStatData:
    """Представляет объединенные данные для DailyStats перед сохранением в базе данных."""
    date: date
    campaign_id: str
    spend: float
    conversions: int
    cpa: Optional[float] = None