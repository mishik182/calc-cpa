from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from app.db import Database, database


class TestDatabase:
    @patch('app.db.create_engine')
    @patch('app.db.sessionmaker')
    def test_database_init(self, mock_sessionmaker, mock_create_engine):
        """Тест инициализации класса Database: проверяет, что engine и sessionmaker вызываются корректно."""
        db_url = "sqlite:///./test_temp.db"

        mock_engine_instance = MagicMock()
        mock_session_local_instance = MagicMock()

        mock_create_engine.return_value = mock_engine_instance
        mock_sessionmaker.return_value = mock_session_local_instance

        db = Database(db_url)

        mock_create_engine.assert_called_once_with(db_url, connect_args={"check_same_thread": False})
        mock_sessionmaker.assert_called_once_with(autocommit=False, autoflush=False, bind=mock_engine_instance)

        assert db.database_url == db_url
        assert db.engine == mock_engine_instance
        assert db.SessionLocal == mock_session_local_instance

    def test_get_db_yields_and_closes_session(self):
        """Тест метода get_db: проверяет, что сессия возвращается и закрывается."""
        # Создаем моки для SessionLocal и для самой сессии
        mock_SessionLocal = MagicMock()
        mock_session_instance = MagicMock(spec=Session)
        mock_SessionLocal.return_value = mock_session_instance

        # Создаем экземпляр Database, но подменяем его SessionLocal на наш мок
        db = Database("sqlite:///./test_temp.db")
        db.SessionLocal = mock_SessionLocal  # <-- Ключова зміна тут!

        with db.get_db() as session:
            assert session == mock_session_instance

        mock_SessionLocal.assert_called_once()
        mock_session_instance.close.assert_called_once()

    def test_global_database_instance_exists(self):
        """Тест, что глобальный экземпляр 'database' существует."""
        # Поскольку 'database = Database()' вызывается при импорте app.db,
        # этот тест просто проверяет, что объект 'database' существует и является экземпляром 'Database'.
        assert isinstance(database, Database)
        assert database.database_url == "sqlite:///./db.sqlite3"
        assert database.engine is not None
        assert database.SessionLocal is not None