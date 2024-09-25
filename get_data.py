from sqlalchemy import create_engine, insert, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.sql.sqltypes import TIMESTAMP, CHAR, REAL, INT, TEXT
from sqlalchemy.orm import DeclarativeBase, Mapped, Session
from sqlalchemy.orm import mapped_column
import requests


def strip(x: str):
    return x.strip('\n')


def get_data_interday_json(symb: str, month: str, api_key: str) -> requests.Response:
    url = api_main_domain + \
          'function=TIME_SERIES_INTRADAY&' \
          'interval=5min&' \
          f'symbol={symb}&' \
          'extended_hours=true&' \
          f'month={month}&' \
          'outputsize=full&' \
          f'datatype=json&' \
          f'apikey={api_key}'

    result = requests.get(url)
    return result.json()


def get_data_interday_result(symb: str, month: str, api_key: str) -> requests.Response:
    url = api_main_domain + \
          'function=TIME_SERIES_INTRADAY&' \
          'interval=5min&' \
          f'symbol={symb}&' \
          'extended_hours=true&' \
          f'month={month}&' \
          'outputsize=full&' \
          f'datatype=json&' \
          f'apikey={api_key}'
    return requests.get(url)


def get_data_interday_url(symb: str, month: str, api_key: str) -> requests.Response:
    url = api_main_domain + \
          'function=TIME_SERIES_INTRADAY&' \
          'interval=5min&' \
          f'symbol={symb}&' \
          'extended_hours=true&' \
          f'month={month}&' \
          'outputsize=full&' \
          f'datatype=json&' \
          f'apikey={api_key}'
    return url


def process_api_data(raw_data: dict) -> list[dict]:
    processed_data = []
    metadata = raw_data['Meta Data']
    for timestamp, vals in raw_data['Time Series (5min)'].items():
        processed_data.append({
            'timestamp_': timestamp,
            'symbol_': metadata['2. Symbol'],
            'open_': float(vals['1. open']),
            'high_': float(vals['2. high']),
            'low_': float(vals['3. low']),
            'close_': float(vals['4. close']),
            'volume_': float(vals['5. volume'])
        })
    return processed_data


class Base(DeclarativeBase):
    pass


class OHLC(Base):
    __tablename__ = 'ohlc'

    timestamp_: Mapped[TIMESTAMP] = mapped_column(__type_pos=TIMESTAMP, primary_key=True, nullable=False)
    symbol_: Mapped[CHAR] = mapped_column(__name_pos='symbol_', __type_pos=CHAR(10), primary_key=True, nullable=False)
    open_: Mapped[float] = mapped_column(__type_pos=REAL)
    high_: Mapped[float] = mapped_column(__type_pos=REAL)
    low_: Mapped[float] = mapped_column(__type_pos=REAL)
    close_: Mapped[float] = mapped_column(__type_pos=REAL)
    volume_: Mapped[int] = mapped_column(__type_pos=INT)


class MetaDataApi(Base):
    __tablename__ = "apiMetaData"

    last_fetched_symbols: Mapped[str] = mapped_column(CHAR(10), nullable=False, primary_key=True)
    last_fetched_timestamp: Mapped[TEXT] = mapped_column(TEXT, nullable=False)


def save_last_timestamp(session: Session, symbol: str, timestamp: str):
    stmt = select(MetaDataApi).where(MetaDataApi.last_fetched_symbols == symbol)
    try:
        metadata_entry = session.scalars(stmt).one()
        metadata_entry.last_fetched_timestamp = timestamp
    except NoResultFound as e:
        metadata_entry = MetaDataApi(last_fetched_symbols=symbol, last_fetched_timestamp=timestamp)
        session.add(metadata_entry)
    session.commit()


def find_last_timestamp(session: Session, symbol: str) -> str | None:
    stmt = select(MetaDataApi).where(MetaDataApi.last_fetched_symbols == symbol)
    try:
        metadata_entry = session.scalars(stmt).one()
        return metadata_entry.last_fetched_timestamp
    except NoResultFound as e:
        return None


def load_data_to_database(raw_data: dict):
    with engine.connect() as conn:
        conn.execute(
            insert(OHLC),
            process_api_data(raw_data)
        )
        conn.commit()


expected_ohlc_structure = {
    'Meta Data': {},
    'Time Series (5min)': {}
}


def validate_response_structure(response_data, expected_structure) -> bool:
    if not isinstance(response_data, dict):
        return False

    if isinstance(response_data, dict):
        return set(response_data.keys()) == set(expected_structure.keys())

    return False


def upload_data(api_keys: list[str]):
    session = Session(engine)

    api_iter = iter(api_keys)
    api_key = next(api_iter)
    # proxy_iter = iter(proxies.keys())
    # proxy_key = next(proxy_iter)

    # Начать перебор по символам
    for symb in comps:
        print(f'Symbol: {symb}')
        # Ищем в таблице metadata последнее обращение к этому символу
        last_data = find_last_timestamp(session, symb)
        if last_data:
            print(f"Last timestamp have found: {last_data}")
            date_list = dates[dates.index(last_data) + 1:]
        else:
            print(f"No last timestamp in db")
            date_list = dates
        print(f'Date list have formed: {date_list}')
        # Начинаем вытягивать данные
        for date_index in range(len(date_list)):
            while True:
                try:
                    api_url = get_data_interday_url(symb, date_list[date_index], api_key)
                    response = requests.get(api_url)

                    if not validate_response_structure(response.json(), expected_ohlc_structure):
                        raise ValueError("Структура данных ответа не совпадает с ожидаемой.")

                    print("Данные корректны, продолжаем обработку")
                    load_data_to_database(response.json())
                    print(f"Данные успешно загружены. Symb: {symb}, date:{date_list[date_index]}")
                    print(f"Metadata have been saved with last timestamp: {date_list[date_index]}")
                    save_last_timestamp(session, symb, timestamp=date_list[date_index])
                    break

                except ValueError as ve:
                    print(ve)

                    if 'Information' in response.json():
                        print("Превышен дневной лимит")
                        try:
                            api_key = next(api_iter)
                            # proxy_key = next(proxy_iter)
                            print(f"Переходим к следующему ключу {api_key}")
                            continue

                        except StopIteration as e:
                            print("Ключи закончились.")
                            if last_data is not None:
                                print(f"Metadata have been saved with last timestamp: {date_list[date_index-1]}")
                                save_last_timestamp(session, symb, timestamp=date_list[date_index-1])
                            else:
                                print(f"Metadata have not been saved. Nothing to save")
                        return

                    else:
                        print("Другая проблема cо структурой данных ответа")
                        if last_data is not None:
                            print(f"Metadata have been saved with last timestamp: {date_list[date_index-1]}")
                            save_last_timestamp(session, symb, timestamp=date_list[date_index-1])
                        else:
                            print(f"Metadata have not been saved. Nothing to save")
                        return

                except Exception as e:
                    print("Возникла ошибка", e)
                    if last_data is not None:
                        print(f"Metadata have been saved with last timestamp: {date_list[date_index-1]}")
                        save_last_timestamp(session, symb, timestamp=date_list[date_index-1])
                    else:
                        print(f"Metadata have not been saved. Nothing to save")
                    return


if __name__ == '__main__':
    with open('apikey.txt', 'r') as f:
        api_keys = list(map(strip, f.readlines()))

    api_main_domain = 'https://www.alphavantage.co/query?'

    engine = create_engine("postgresql+psycopg2://admin:admin@localhost:5434/main_storage", pool_pre_ping=True)
    Base.metadata.create_all(engine)

    comps = ['TSLA', 'AAPL', 'NVDA']
    dates = [f'{year}-{month}' if month >= 10 else f'{year}-0{month}'
             for year in range(2021, 2024) for month in range(1, 13)]

    # with open("./proxy.json", 'r') as f:
    #     proxies = json.loads(f.read())

    upload_data(api_keys)
