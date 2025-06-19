import requests
import json
from typing import List, Dict, Any, Optional
import logging
from logging.handlers import RotatingFileHandler
import datetime
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Настройка логирования
logger = logging.getLogger("selectumproperty")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler("selectumproperty.log", maxBytes=10*1024*1024, backupCount=1, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]

class SelectumPropertyParser:
    BASE_IMAGE_URL = "https://selectumproperty.com/_next/image?url=https%3A%2F%2Fselectumproperty.com%2Fapi%2Ffiles%2Fproperty-images%2F"

    def __init__(self, cookies: Optional[dict] = None, headers: Optional[dict] = None):
        self.session = requests.Session()
        self.cookies = cookies or {'language': 'ru'}
        self.headers = headers or {
            'accept': '*/*',
            'accept-language': 'ru,en;q=0.9',
            'dnt': '1',
            'referer': 'https://selectumproperty.com/realestates',
            'rsc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0',
        }

    def extract_realestates(self, text: str) -> List[Dict[str, Any]]:
        key = '"realEstatesData":{'
        start = text.find(key)
        if start == -1:
            return []
        i = start + len(key) - 1
        stack = ['{']
        for j in range(i + 1, len(text)):
            if text[j] == '{':
                stack.append('{')
            elif text[j] == '}':
                stack.pop()
                if not stack:
                    end = j + 1
                    break
        else:
            return []
        block = text[start + len('"realEstatesData":'):end]
        try:
            data = json.loads(block)
            return data.get('realEstates', [])
        except Exception as e:
            print('Ошибка парсинга JSON:', e)
            return []

    def decode_text(self, text: str) -> str:
        if not text:
            return ""
        try:
            return text.encode('latin1').decode('utf-8')
        except Exception:
            return text

    def format_estate(self, estate: Dict[str, Any]) -> Dict[str, Any]:
        # Декодируем только текстовые поля, остальные оставляем как есть
        result = dict(estate)
        for key in ["title", "location", "area", "houseType"]:
            if key in result and result[key] is not None:
                result[key] = self.decode_text(result[key])
        # Формируем массив ссылок на изображения
        images = result.get('images', [])
        result['image_urls'] = [
            self.BASE_IMAGE_URL + img['file_name'] + '&w=2048&q=75'
            for img in images if img.get('file_name')
        ]
        # types: строка из name через запятую
        types = result.get('types')
        if types and isinstance(types, list):
            result['types'] = ', '.join(
                self.decode_text(t.get('name', '')) for t in types if t.get('name')
            )
        else:
            result['types'] = ''
        return result

    def get_realestates_page(self, page: int = 1) -> List[Dict[str, Any]]:
        url = 'https://selectumproperty.com/realestates'
        params = {'page': str(page)}
        response = self.session.get(url, params=params, cookies=self.cookies, headers=self.headers)
        raw_estates = self.extract_realestates(response.text)
        estates = [self.format_estate(e) for e in raw_estates]
        logger.info(f"Страница {page}: найдено объектов {len(estates)}")
        return estates

    def get_all_realestates(self, max_pages: int = 10) -> List[Dict[str, Any]]:
        all_estates = []
        for page in range(1, max_pages + 1):
            estates = self.get_realestates_page(page)
            if not estates:
                logger.info(f"Нет данных на странице {page}, остановка.")
                break
            all_estates.extend(estates)
        logger.info(f"Всего обработано страниц: {page if estates else page-1}")
        logger.info(f"Всего объектов собрано: {len(all_estates)}")
        return all_estates

    def save_to_json(self, data: List[Dict[str, Any]], filename: str = 'selectum_properties.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def save_to_postgres(self, estates, db_params=None):
        """
        Сохраняет список объектов недвижимости в таблицу realestates (PostgreSQL)
        :param estates: список словарей
        :param db_params: dict с ключами host, port, user, password, dbname
        """
        load_dotenv()  # загружаем переменные окружения из .env
        db_params = db_params or {
            'host': os.getenv('PG_HOST', 'localhost'),
            'port': int(os.getenv('PG_PORT', 5432)),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASSWORD', ''),
            'dbname': os.getenv('PG_DB', 'postgres'),
        }
        conn = None
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            rows = [
                (
                    e.get('id'),
                    e.get('title'),
                    e.get('bed_room'),
                    e.get('max_bed'),
                    e.get('bathroom'),
                    e.get('metrage'),
                    e.get('price'),
                    e.get('price_min'),
                    e.get('price_max'),
                    e.get('location'),
                    e.get('area'),
                    e.get('money_type'),
                    e.get('is_multi'),
                    e.get('houseType'),
                    str(e.get('types')) if e.get('types') is not None else None,
                    e.get('image_urls'),
                )
                for e in estates
            ]
            sql = '''
                INSERT INTO public.realestates (
                    id, title, bed_room, max_bed, bathroom, metrage, price, price_min, price_max,
                    location, area, money_type, is_multi, houseType, types, image_urls
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    bed_room = EXCLUDED.bed_room,
                    max_bed = EXCLUDED.max_bed,
                    bathroom = EXCLUDED.bathroom,
                    metrage = EXCLUDED.metrage,
                    price = EXCLUDED.price,
                    price_min = EXCLUDED.price_min,
                    price_max = EXCLUDED.price_max,
                    location = EXCLUDED.location,
                    area = EXCLUDED.area,
                    money_type = EXCLUDED.money_type,
                    is_multi = EXCLUDED.is_multi,
                    houseType = EXCLUDED.houseType,
                    types = EXCLUDED.types,
                    image_urls = EXCLUDED.image_urls
            '''
            execute_values(cur, sql, rows)
            conn.commit()
            logger.info(f"В базу записано объектов: {len(rows)}")
            print(f"Сохранено объектов в Postgres: {len(rows)}")
        except Exception as e:
            logger.error(f"Ошибка при записи в Postgres: {e}")
            print(f"Ошибка при записи в Postgres: {e}")
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    logger.info("\n" + "="*20 + f" Запуск парсинга: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} " + "="*20)
    parser = SelectumPropertyParser()
    all_estates = parser.get_all_realestates(max_pages=10)
    parser.save_to_postgres(all_estates)