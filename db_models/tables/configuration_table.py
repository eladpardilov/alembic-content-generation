from sqlalchemy import Column, DateTime, Integer, String
from db_models import db_base


class ConfigurationsTable(db_base):
    __tablename__ = "configurations"
    service = Column(String(100), primary_key=True)
    version = Column(String(100), primary_key=True)
    hostname = Column(String(1000))
    port = Column(Integer)
    status = Column(String(100))
    updated_at = Column(DateTime, server_default="now()", info={"exclude_from_automation": True})
