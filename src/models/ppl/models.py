"""Models for PPL layer (processed data)."""
from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class WebmasterAggregated(Base):
    """Aggregated webmaster data."""
    __tablename__ = 'webmaster_aggregated'
    __table_args__ = {'schema': 'ppl'}

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    query = Column(String, nullable=False)
    page_path = Column(String, nullable=False)
    device = Column(String(20), nullable=False)
    demand = Column(Integer, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    position = Column(Float, nullable=False)

    def __repr__(self):
        return f"<WebmasterAggregated(id={self.id}, date={self.date}, query={self.query[:20]}...)>"


class WebmasterPositions(Base):
    """Restored impression positions."""
    __tablename__ = 'webmaster_positions'
    __table_args__ = {'schema': 'ppl'}

    id = Column(Integer, ForeignKey('ppl.webmaster_aggregated.id'), primary_key=True)
    impression_position = Column(Integer, primary_key=True)
    impression_order = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"<WebmasterPositions(id={self.id}, pos={self.impression_position}, order={self.impression_order})>"


class WebmasterClicks(Base):
    """Restored clicks data."""
    __tablename__ = 'webmaster_clicks'
    __table_args__ = {'schema': 'ppl'}

    id = Column(Integer, ForeignKey('ppl.webmaster_aggregated.id'), primary_key=True)
    click_position = Column(Integer, primary_key=True)
    impression_order = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"<WebmasterClicks(id={self.id}, pos={self.click_position}, order={self.impression_order})>"
