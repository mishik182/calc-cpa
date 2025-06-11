import argparse
import datetime
import logging
from typing import Optional

from app.api import ApiDataSource
from app.crud import DailyStatsCRUD, LastUpdateTimeCRUD
from app.data_loader import DataLoader
from app.db import database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app_sync.log"), # Логирование в файл
        logging.StreamHandler() # Логирование в консоль
    ]
)
logger = logging.getLogger(__name__)


def run(start_date: Optional[datetime.date] = None, end_date: Optional[datetime.date] = None):
    api_data_source = ApiDataSource()
    logger.info("Запуск программы синхронизации данных.")
    if start_date and end_date:
        logger.info(f"Диапазон дат: з {start_date.isoformat()} по {end_date.isoformat()}.")
    elif start_date:
        logger.info(f"Начальная дата: з {start_date.isoformat()}.")
    elif end_date:
        logger.info(f"Конечная дата: по {end_date.isoformat()}.")
    else:
        logger.info("Диапазон дат не указано (будут учтены все доступные даты, требующие обновления).")

    with database.get_db() as db_session:
        db_crud = DailyStatsCRUD(db_session)
        update_crud = LastUpdateTimeCRUD(db_session)
        data_loader = DataLoader(api_data_source, db_crud, update_crud)

        data_loader.process_daily_stats(start_date=start_date, end_date=end_date)

    logger.info("Завершение работы.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Скрипт для синхронизации рекламных данных.")
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Начальная дата для загрузки данных (формат: YYYY-MM-DD). Влияет на фильтрацию сырых данных.",
        required=False
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Конечная дата для загрузки данных (формат: YYYY-MM-DD). Влияет на фильтрацию сырых данных.",
        required=False
    )

    args = parser.parse_args()
    run(start_date=args.start_date, end_date=args.end_date)
