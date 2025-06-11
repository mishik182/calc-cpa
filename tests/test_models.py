from datetime import date, datetime

from sqlalchemy import Column

from app.models import Base, DailyStats, LastUpdateTime


class TestModels:
    def test_daily_stats_model_definition(self):
        """
        Тест объявления модели DailyStats:
        Проверяет, что таблица и столбцы корректно определены.
        """
        assert DailyStats.__tablename__ == "daily_stats"

        # Доступ к объекту Column через .prop.columns[0]
        assert isinstance(DailyStats.date.prop.columns[0], Column)
        assert DailyStats.date.prop.columns[0].type.python_type == date
        assert DailyStats.date.primary_key is True

        assert isinstance(DailyStats.campaign_id.prop.columns[0], Column)
        assert DailyStats.campaign_id.prop.columns[0].type.python_type == str
        assert DailyStats.campaign_id.primary_key is True

        assert isinstance(DailyStats.spend.prop.columns[0], Column)
        assert DailyStats.spend.prop.columns[0].type.python_type == float
        assert DailyStats.spend.nullable is False

        assert isinstance(DailyStats.conversions.prop.columns[0], Column)
        assert DailyStats.conversions.prop.columns[0].type.python_type == int
        assert DailyStats.conversions.nullable is False

        assert isinstance(DailyStats.cpa.prop.columns[0], Column)
        assert DailyStats.cpa.prop.columns[0].type.python_type == float
        assert DailyStats.cpa.nullable is True

        # Проверка, что DailyStats является подклассом Base
        assert issubclass(DailyStats, Base)

    def test_daily_stats_repr(self):
        """Тест метода __repr__ для DailyStats."""
        daily_stat = DailyStats(
            date=date(2025, 6, 11),
            campaign_id="CAMP-ABC",
            spend=150.75,
            conversions=20,
            cpa=7.5375
        )
        expected_repr = (
            f"<DailyStats(date={daily_stat.date}, campaign_id='{daily_stat.campaign_id}', "
            f"spend={daily_stat.spend}, conversions={daily_stat.conversions}, cpa={daily_stat.cpa})>"
        )
        assert repr(daily_stat) == expected_repr

        # Тест с None для cpa
        daily_stat_no_cpa = DailyStats(
            date=date(2025, 6, 12),
            campaign_id="CAMP-XYZ",
            spend=50.00,
            conversions=0,
            cpa=None
        )
        expected_repr_no_cpa = (
            f"<DailyStats(date={daily_stat_no_cpa.date}, campaign_id='{daily_stat_no_cpa.campaign_id}', "
            f"spend={daily_stat_no_cpa.spend}, conversions={daily_stat_no_cpa.conversions}, cpa={daily_stat_no_cpa.cpa})>"
        )
        assert repr(daily_stat_no_cpa) == expected_repr_no_cpa


    def test_last_update_time_model_definition(self):
        """
        Тест объявления модели LastUpdateTime:
        Проверяет, что таблица и столбцы корректно определены.
        """
        assert LastUpdateTime.__tablename__ == "last_update_time"

        # Доступ к объекту Column через .prop.columns[0]
        assert isinstance(LastUpdateTime.date.prop.columns[0], Column)
        assert LastUpdateTime.date.prop.columns[0].type.python_type == date
        assert LastUpdateTime.date.primary_key is True

        assert isinstance(LastUpdateTime.last_updated_at.prop.columns[0], Column)
        assert LastUpdateTime.last_updated_at.prop.columns[0].type.python_type == datetime
        assert LastUpdateTime.last_updated_at.nullable is False
        assert LastUpdateTime.last_updated_at.prop.columns[0].default is not None

        assert isinstance(LastUpdateTime.is_complete.prop.columns[0], Column)
        assert LastUpdateTime.is_complete.prop.columns[0].type.python_type == bool
        assert LastUpdateTime.is_complete.prop.columns[0].default.arg is False

        # Проверка, что LastUpdateTime является подклассом Base
        assert issubclass(LastUpdateTime, Base)


    def test_last_update_time_repr(self):
        """
        Тест метода __repr__ для LastUpdateTime.
        """
        test_date = date(2025, 6, 11)
        test_now = datetime(2025, 6, 11, 12, 30, 0) # Используем фиксированное время для предсказуемости

        last_update_complete = LastUpdateTime(
            date=test_date,
            last_updated_at=test_now,
            is_complete=True
        )
        expected_repr_complete = (
            f"<LastUpdateTime(date={test_date}, last_updated_at={test_now}, "
            f"is_complete=True)>"
        )
        assert repr(last_update_complete) == expected_repr_complete

        # Тест для случая с is_complete=False
        last_update_incomplete = LastUpdateTime(
            date=test_date,
            last_updated_at=test_now,
            is_complete=False
        )
        expected_repr_incomplete = (
            f"<LastUpdateTime(date={test_date}, last_updated_at={test_now}, "
            f"is_complete=False)>"
        )
        assert repr(last_update_incomplete) == expected_repr_incomplete

        last_update_default = LastUpdateTime(
            date=date(2025, 6, 12),
            last_updated_at=datetime(2025, 6, 12, 10, 0, 0), # Явно передаем для repr
            is_complete=False
        )
        expected_repr_default = (
            "<LastUpdateTime(date=2025-06-12, last_updated_at=2025-06-12 10:00:00, "
            "is_complete=False)>"
        )
        assert repr(last_update_default) == expected_repr_default
