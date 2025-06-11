import json
from typing import List, Dict, Any
import requests
import logging

logger = logging.getLogger(__name__)

from app.data_models import SpendEntry, ConversionEntry

class ApiDataSource:
    def __init__(self):
        """
        Инициализирует источник данных API с указанными URL-адресами.
        """
        self.fb_spend_url = "https://179c1438-5a21-4e5c-b700-3412c1473e22.mock.pstmn.io/fb_spend"
        self.network_conv_url = "https://179c1438-5a21-4e5c-b700-3412c1473e22.mock.pstmn.io/network_conv"

    def _fetch_data_from_api(self, url: str) -> List[Dict[str, Any]]:
        """
        Выполняет HTTP GET-запрос к указанному URL и возвращает JSON-ответ.
        Включает в себя базовую обработку ошибок.
        """
        try:
            logger.info(f"Выполнение GET-запроса к: {url}")
            response = requests.get(url, timeout=10) # Додаємо таймаут
            response.raise_for_status()  # Викличе HTTPError для поганих відповідей (4xx або 5xx)
            logger.info(f"Успешно получены данные из {url}")
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Таймаут запроса к {url}. Сервер не отвечает.")
            return []
        except requests.exceptions.ConnectionError:
            logger.error(f"Ошибка соединения при запросе {url}. Проверьте подключение к Интернету.")
            return []
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP ошибка при получении данных из {url}: {e} - Статус: {response.status_code}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Неизвестная ошибка при получении данных из {url}: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON из {url}: {e}. Возможно, ответ не является валидным JSON.")
            return []

    def fetch_fb_spend_data(self) -> List[SpendEntry]:
        """
        Получает данные о расходах по API Facebook,
        возвращая список объектов SpendEntry.
        """
        raw_data = self._fetch_data_from_api(self.fb_spend_url)
        return [SpendEntry(**item) for item in raw_data]

    def fetch_network_conversions_data(self) -> List[ConversionEntry]:
        """
        Получает данные о конверсиях с сетевого API,
        возвращая список объектов ConversionEntry.
        """
        raw_data = self._fetch_data_from_api(self.network_conv_url)
        return [ConversionEntry(**item) for item in raw_data]
