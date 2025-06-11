from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.models import Base


class Database:
    def __init__(self, database_url: str = "sqlite:///./db.sqlite3"):
        """
        Инициализирует объект базы данных.

        Args:
            database_url: URL для подключения к базе данных (по умолчанию SQLite).
        """
        self.database_url = database_url
        self.engine = create_engine(
            self.database_url, connect_args={"check_same_thread": False}
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    @contextmanager
    def get_db(self) -> Session:
        """
        Возвращает генератор сессии базы данных.
        Предназначен для использования в качестве контекстного менеджера (`with`).
        """
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

database = Database()
