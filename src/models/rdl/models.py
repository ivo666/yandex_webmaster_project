"""Models for RDL layer (raw data)."""
from sqlalchemy import Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class WebmApi(Base):
    """Raw webmaster data from API."""
    __tablename__ = 'webm_api'
    __table_args__ = {'schema': 'rdl'}

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    page_path = Column(String, nullable=False)
    query = Column(String, nullable=False)
    demand = Column(Integer, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    position = Column(Float, nullable=False)
    device = Column(String(20), nullable=False)

    def __repr__(self):
        return f"<WebmApi(id={self.id}, date={self.date}, query={self.query[:20]}...)>"
