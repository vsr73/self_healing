import random
import time
import uuid

from pipeline.config import PIPELINE_DEFAULTS


SYMBOLS = {
    "AAPL": 212.35,
    "MSFT": 428.10,
    "GOOGL": 172.85,
    "AMZN": 189.40,
    "TSLA": 176.25,
    "NVDA": 924.70,
}


class StockEventGenerator:
    def __init__(self):
        self.prices = SYMBOLS.copy()
        self.symbols = list(self.prices.keys())

    def _next_price(self, symbol):
        base_price = self.prices[symbol]
        movement = random.uniform(-2.5, 2.5)
        next_price = max(1.0, round(base_price + movement, 2))
        self.prices[symbol] = next_price
        return next_price

    def _value_for_field(self, field_name, symbol, price, volume, timestamp):
        normalized = field_name.lower()
        if normalized in {"event_id", "id"}:
            return str(uuid.uuid4())
        if normalized == "symbol":
            return symbol
        if normalized in {"price", "new_price", "trade_price", "price_renamed"}:
            return price
        if normalized in {"volume", "vol", "volume_renamed"}:
            return volume
        if normalized in {"timestamp", "ts", "event_ts"}:
            return timestamp
        if normalized == "exchange":
            return PIPELINE_DEFAULTS["default_exchange"]
        if normalized in {"currency", "curr", "currency_code"}:
            return PIPELINE_DEFAULTS["default_currency"]
        return None

    def generate_event(self, field_names=None):
        symbol = random.choice(self.symbols)
        price = self._next_price(symbol)
        volume = random.randint(100, 25000)
        timestamp = int(time.time())

        if not field_names:
            field_names = [
                "event_id",
                "symbol",
                "price",
                "volume",
                "timestamp",
                "exchange",
                "currency",
            ]

        event = {}
        for field_name in field_names:
            event[field_name] = self._value_for_field(
                field_name,
                symbol,
                price,
                volume,
                timestamp,
            )

        return event
