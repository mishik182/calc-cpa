import datetime
from collections import defaultdict
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

from app.crud import DailyStatsCRUD, LastUpdateTimeCRUD
from app.api import ApiDataSource
from app.data_models import SpendEntry, ConversionEntry, CombinedDailyStatData


class DataLoader:
    def __init__(
            self,
            api_data_source: ApiDataSource,
            db_crud: DailyStatsCRUD,
            update_crud: LastUpdateTimeCRUD
    ):
        self.api_data_source = api_data_source
        self.db_crud = db_crud
        self.update_crud = update_crud

    def _should_fetch_data(self, record_date: datetime.date) -> bool:
        """
        Проверяет, нужно ли загружать данные для определенной даты,
        учитывая стратегию "обновления 1 раз в день" и полноту данных.
        """
        last_update = self.update_crud.get_last_update_info(record_date)
        current_utc_time = datetime.datetime.now()

        if not last_update:
            logger.info(f"Данных для {record_date.isoformat()} еще нет в системе. Загружаем.")
            return True

        # Если данные неполны или последнее обновление было более 24 часов назад
        if not last_update.is_complete or \
                (current_utc_time - last_update.last_updated_at).total_seconds() > (24 * 3600):
            logger.info(
                f"Данные для {record_date.isoformat()} неполны или устаревшие (последнее обновление: {last_update.last_updated_at.isoformat()}). Загружаем.")
            return True

        logger.info(
            f"Данные для {record_date.isoformat()} полны и актуальны (обновлено {last_update.last_updated_at.isoformat()}). Пропускаем.")
        return False

    def process_daily_stats(
            self,
            start_date: Optional[datetime.date] = None,
            end_date: Optional[datetime.date] = None
    ):
        logger.info("Загрузка сырых данных о расходах по API Data Source...")
        spend_data: List[SpendEntry] = self.api_data_source.fetch_fb_spend_data()
        logger.info("Загрузка сырых данных о конверсиях с API Data Source...")
        conversions_data: List[ConversionEntry] = self.api_data_source.fetch_network_conversions_data()

        if not spend_data and not conversions_data:
            logger.warning("Не получены данные ни из источника затрат, ни из источника конверсий. Пропускаем обработку.")
            return

        combined_raw_data = defaultdict(lambda: {"spend": 0.0, "conversions": 0})

        # Собираем все уникальные даты из полученных данных
        all_dates_in_data = set()
        for entry in spend_data:
            all_dates_in_data.add(datetime.date.fromisoformat(entry.date))
        for entry in conversions_data:
            all_dates_in_data.add(datetime.date.fromisoformat(entry.date))

        # Определяем, какие даты требуют обработки на основе фильтров и стратегии обновления
        dates_to_process = []
        for current_date in sorted(list(all_dates_in_data)):  # Сортируем для консистентности
            # Фильтруем по аргументам командной строки
            if (start_date and current_date < start_date) or \
                    (end_date and current_date > end_date):
                logger.debug(f"Дата {current_date.isoformat()} выходит за указанный диапазон. Пропускаем.")
                continue

            # Фильтруем по стратегии "1 раз в день"
            if self._should_fetch_data(current_date):
                dates_to_process.append(current_date)

        if not dates_to_process:
            logger.info("Нет новых или устаревших данных для обработки в указанном диапазоне.")
            return

        logger.info(f"Будут обработаны данные для следующих дат: {[d.isoformat() for d in dates_to_process]}")

        # Агрегация данных из обоих источников, но только для дат, которые мы решили обрабатывать
        for entry in spend_data:
            entry_date = datetime.date.fromisoformat(entry.date)
            if entry_date in dates_to_process:
                key = (entry.date, entry.campaign_id)
                combined_raw_data[key]["spend"] += entry.spend

        for entry in conversions_data:
            entry_date = datetime.date.fromisoformat(entry.date)
            if entry_date in dates_to_process:
                key = (entry.date, entry.campaign_id)
                combined_raw_data[key]["conversions"] += entry.conversions

        processed_data: List[CombinedDailyStatData] = []
        # Конвертируем агрегированные данные в CombinedDailyStatData
        for (date_str, campaign_id), values in combined_raw_data.items():
            record_date = datetime.date.fromisoformat(date_str)

            # Ця перевірка по суті повторна, але гарантує, що ми додаємо лише ті, що були обрані
            # if record_date not in dates_to_process:
            #     continue

            spend = values["spend"]
            conversions = values["conversions"]
            cpa = spend / conversions if conversions > 0 else None

            processed_data.append(
                CombinedDailyStatData(
                    date=record_date,
                    campaign_id=campaign_id,
                    spend=spend,
                    conversions=conversions,
                    cpa=cpa
                )
            )

        if not processed_data:
            logger.info("После фильтрации не осталось данных для сохранения.")
            return

        logger.info(f"Сохранение {len(processed_data)} обработанных записей в базу данных...")
        for data_item in processed_data:
            self.db_crud.upsert_daily_stat(
                record_date=data_item.date,
                campaign_id=data_item.campaign_id,
                spend=data_item.spend,
                conversions=data_item.conversions,
                cpa=data_item.cpa
            )

        # Обновляем LastUpdateTime
        for processed_date in dates_to_process:
            self.update_crud.set_last_update_info(processed_date, is_complete=True)

        logger.info("Загрузка данных завершена.")
