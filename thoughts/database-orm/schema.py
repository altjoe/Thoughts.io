import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class TradeData(Base):
    __tablename__ = 'trade'

    id: Mapped[int] = mapped_column(primary_key=True)
    time: Mapped[datetime.datetime]
    price: Mapped[float]
    amount: Mapped[float]
    side: Mapped[str]

    def __repr__(self):
        return "<TradeData(id='{}', time='{}', price='{}', amount='{}')>".format(
            self.id, self.time, self.price, self.amount)

