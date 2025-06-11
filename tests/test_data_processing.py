from datetime import date, datetime, timedelta
from typing import List

import pytest

from app.data_loader import DataLoader
from app.data_models import SpendEntry, ConversionEntry


class MockApiDataSource:
    def __init__(self):
        # Моковые данные о расходах из Facebook
        self._fb_spend_data: List[SpendEntry] = [
            SpendEntry(date="2025-06-04", campaign_id="CAMP-123", spend=37.50),
            SpendEntry(date="2025-06-04", campaign_id="CAMP-456", spend=19.90),
            SpendEntry(date="2025-06-05", campaign_id="CAMP-123", spend=42.10),
            SpendEntry(date="2025-06-05", campaign_id="CAMP-789", spend=11.00),
            SpendEntry(date="2025-06-06", campaign_id="CAMP-999", spend=5.25)
        ]
        # Моковые данные о конверсиях из партнерской сети
        self._network_conv_data: List[ConversionEntry] = [
            ConversionEntry(date="2025-06-04", campaign_id="CAMP-123", conversions=14),
            ConversionEntry(date="2025-06-04", campaign_id="CAMP-456", conversions=3),
            ConversionEntry(date="2025-06-05", campaign_id="CAMP-123", conversions=10),
            ConversionEntry(date="2025-06-05", campaign_id="CAMP-456", conversions=5),
            ConversionEntry(date="2025-06-06", campaign_id="CAMP-888", conversions=7)
        ]

    def fetch_fb_spend_data(self) -> List[SpendEntry]:
        """Возвращает моковые данные о расходах."""
        return self._fb_spend_data

    def fetch_network_conversions_data(self) -> List[ConversionEntry]:
        """Возвращает моковые данные о конверсиях."""
        return self._network_conv_data


class MockDailyStatsCRUD:
    def __init__(self, db_session=None):
        # Хранилище для данных, которые были бы "вставлены или обновлены" в БД
        self.upserted_data = []

    def upsert_daily_stat(self, record_date, campaign_id, spend, conversions, cpa):
        # Имитируем логику upsert: если запись уже есть, обновляем, иначе добавляем.
        found = False
        for i, item in enumerate(self.upserted_data):
            if item["date"] == record_date and item["campaign_id"] == campaign_id:
                self.upserted_data[i] = {
                    "date": record_date,
                    "campaign_id": campaign_id,
                    "spend": spend,
                    "conversions": conversions,
                    "cpa": cpa
                }
                found = True
                break
        if not found:
            self.upserted_data.append({
                "date": record_date,
                "campaign_id": campaign_id,
                "spend": spend,
                "conversions": conversions,
                "cpa": cpa
            })
        return True


class MockLastUpdateTimeCRUD:
    def __init__(self, db_session=None):
        # Хранилище для информации о последнем обновлении по датам
        # date -> LastUpdateTime object (моковый объект)
        self.update_info = {}

    def get_last_update_info(self, record_date: date):
        """Возвращает моковую информацию о последнем обновлении для указанной даты."""
        return self.update_info.get(record_date)

    def set_last_update_info(self, record_date: date, is_complete: bool = False):
        # Имитируем сохранение LastUpdateTime
        # Создаем mock-объект, который имеет нужные атрибуты
        # Используем type() для динамического создания класса-мока
        self.update_info[record_date] = type('LastUpdateTimeMock', (object,), {
            'date': record_date,
            'last_updated_at': datetime.utcnow(),
            'is_complete': is_complete
        })()
        return self.update_info[record_date]


def test_cpa_calculation_and_merge_all_data():
    """Тест проверяет правильность вычисления CPA и слияния данных без фильтрации по датам."""
    # Arrange (Подготовка)
    mock_api_source = MockApiDataSource()
    mock_db_crud = MockDailyStatsCRUD()
    mock_update_crud = MockLastUpdateTimeCRUD()

    data_loader = DataLoader(mock_api_source, mock_db_crud, mock_update_crud)

    # Act (Действие)
    # Запускаем без фильтрации по датам, чтобы обработать все, что есть в моках
    data_loader.process_daily_stats()

    # Assert (Проверка)
    upserted_data = mock_db_crud.upserted_data

    # Ожидаемые результаты после слияния и вычисления CPA
    # Обратите внимание на даты и campaign_id, которые могут иметь только расходы или только конверсии
    expected_results = {
        (date(2025, 6, 4), "CAMP-123"): {"spend": 37.50, "conversions": 14, "cpa": 37.50 / 14},
        (date(2025, 6, 4), "CAMP-456"): {"spend": 19.90, "conversions": 3, "cpa": 19.90 / 3},
        (date(2025, 6, 5), "CAMP-123"): {"spend": 42.10, "conversions": 10, "cpa": 42.10 / 10},
        (date(2025, 6, 5), "CAMP-789"): {"spend": 11.00, "conversions": 0, "cpa": None},  # CAMP-789 не имеет конверсий
        (date(2025, 6, 5), "CAMP-456"): {"spend": 0.0, "conversions": 5, "cpa": 0.0 / 5},
        # CAMP-456 05.06 не имеет расходов
        (date(2025, 6, 6), "CAMP-999"): {"spend": 5.25, "conversions": 0, "cpa": None},  # CAMP-999 не имеет конверсий
        (date(2025, 6, 6), "CAMP-888"): {"spend": 0.0, "conversions": 7, "cpa": 0.0 / 7},  # CAMP-888 не имеет расходов
    }

    assert len(upserted_data) == len(expected_results)

    for item in upserted_data:
        key = (item["date"], item["campaign_id"])
        assert key in expected_results, f"Неожиданная запись: {item}"
        expected_item = expected_results[key]

        assert pytest.approx(item["spend"]) == expected_item["spend"], f"Неверный spend для {key}"
        assert item["conversions"] == expected_item["conversions"], f"Неверные conversions для {key}"

        if expected_item["cpa"] is None:
            assert item["cpa"] is None, f"CPA должно быть None для {key}"
        else:
            assert pytest.approx(item["cpa"]) == expected_item["cpa"], f"Неверный CPA для {key}"

    # Проверка конкретных случаев CPA = None или CPA = 0.0
    assert next((item["cpa"] for item in upserted_data if item["campaign_id"] == "CAMP-789"), None) is None
    assert next((item["cpa"] for item in upserted_data if item["campaign_id"] == "CAMP-999"), None) is None
    assert next((item["cpa"] for item in upserted_data if
                     item["campaign_id"] == "CAMP-456" and item["date"] == date(2025, 6, 5)), -1) == 0.0
    assert next((item["cpa"] for item in upserted_data if item["campaign_id"] == "CAMP-888"), -1) == 0.0


def test_data_filtering_by_date_range():
    """Тест проверяет, что DataLoader обрабатывает данные только в указанном диапазоне дат."""
    mock_api_source = MockApiDataSource()
    mock_db_crud = MockDailyStatsCRUD()
    mock_update_crud = MockLastUpdateTimeCRUD()

    data_loader = DataLoader(mock_api_source, mock_db_crud, mock_update_crud)

    # Запускаем с конкретным диапазоном дат
    data_loader.process_daily_stats(
        start_date=date(2025, 6, 5),
        end_date=date(2025, 6, 5)
    )

    # Ожидаем, что будут обработаны только данные за 2025-06-05
    processed_dates = {item["date"] for item in mock_db_crud.upserted_data}
    campaign_ids = {item["campaign_id"] for item in mock_db_crud.upserted_data}

    assert len(processed_dates) == 1
    assert date(2025, 6, 5) in processed_dates
    assert date(2025, 6, 4) not in processed_dates
    assert date(2025, 6, 6) not in processed_dates

    # Проверяем, что обработаны только CAMP-123, CAMP-789, CAMP-456 за 05.06
    assert "CAMP-123" in campaign_ids
    assert "CAMP-789" in campaign_ids
    assert "CAMP-456" in campaign_ids
    assert "CAMP-999" not in campaign_ids  # Это с 06.06
    assert "CAMP-888" not in campaign_ids  # Это с 06.06


def test_data_filtering_by_last_update_strategy():
    """Тест проверяет, что DataLoader пропускает уже обновленные данные."""
    mock_api_source = MockApiDataSource()
    mock_db_crud = MockDailyStatsCRUD()
    mock_update_crud = MockLastUpdateTimeCRUD()

    # Устанавливаем, что данные за 2025-06-04 уже были загружены и считаются полными
    # и время обновления только что произошло (актуальны)
    mock_update_crud.set_last_update_info(date(2025, 6, 4), is_complete=True)

    data_loader = DataLoader(mock_api_source, mock_db_crud, mock_update_crud)

    # Запускаем без указания дат, что означает "обработать все доступные даты, которые нуждаются в обновлении"
    data_loader.process_daily_stats()

    # Ожидаем, что 2025-06-04 не будет обработана повторно (потому что она "полна" и "актуальна").
    # А 2025-06-05 и 2025-06-06 будут обработаны, так как они не были помечены как обновленные ранее.
    processed_dates = {item["date"] for item in mock_db_crud.upserted_data}

    assert date(2025, 6, 4) not in processed_dates  # Должно быть пропущено
    assert date(2025, 6, 5) in processed_dates  # Должно быть обработано
    assert date(2025, 6, 6) in processed_dates  # Должно быть обработано
    assert len(processed_dates) == 2  # Только две даты были обработаны


def test_data_reprocessing_if_outdated():
    """Тест проверяет, что DataLoader обрабатывает данные, если они устарели (более 24 часов)."""
    mock_api_source = MockApiDataSource()
    mock_db_crud = MockDailyStatsCRUD()
    mock_update_crud = MockLastUpdateTimeCRUD()

    # Устанавливаем, что данные за 2025-06-04 были загружены, но ОЧЕНЬ давно
    outdated_time = datetime.now() - timedelta(hours=25)  # 25 часов назад
    mock_update_crud.update_info[date(2025, 6, 4)] = type('LastUpdateTimeMock', (object,), {
        'date': date(2025, 6, 4),
        'last_updated_at': outdated_time,
        'is_complete': True  # Считались полными, но устарели
    })()

    data_loader = DataLoader(mock_api_source, mock_db_crud, mock_update_crud)

    # Запускаем
    data_loader.process_daily_stats()

    processed_dates = {item["date"] for item in mock_db_crud.upserted_data}

    # Теперь 2025-06-04 должна быть обработана снова, так как данные устарели
    assert date(2025, 6, 4) in processed_dates
    assert date(2025, 6, 5) in processed_dates
    assert date(2025, 6, 6) in processed_dates
    assert len(processed_dates) == 3  # Все три даты были обработаны