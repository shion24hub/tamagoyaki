from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()


class Candle(Base):

    __tablename__ = "candle"

    id = Column(Integer, primary_key=True)

    exchange = Column(String)
    symbol = Column(String)
    datetime = Column(DateTime)
    open = Column(String)
    high = Column(String)
    low = Column(String)
    close = Column(String)
    volume = Column(String)
    buy_volume = Column(String)
    sell_volume = Column(String)

    def __repr__(self) -> str:
        return f"<Candle(exchange={self.exchange}, symbol={self.symbol}, datetime={self.datetime}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, volume={self.volume}, buy_volume={self.buy_volume}, sell_volume={self.sell_volume})>"
    
    
class Database:

    def __init__(self, url: str) -> None:
        self.engine = create_engine(url)
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()
