import faust

class Stock(faust.Record, serializer='json'):
    """
    Stock 데이터 모델
    Faust에서 Kafka 메시지를 처리하기 위해 사용되는 데이터 클래스.
    """
    id: int
    name: str
    symbol: str
    timestamp: str
    open: float
    close: float
    high: float
    low: float
    rate_price: float
    rate: float
    volume: int
    trading_value: float
