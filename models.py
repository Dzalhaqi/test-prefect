import os
from sqlalchemy import create_engine, Column, String, JSON, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import uuid

Base = declarative_base()

class RegulationMetadata(Base):
    __tablename__ = 'regulation_metadata'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    regulation_title = Column(String, nullable=True)
    regulation_type_number = Column(String, nullable=True)
    detail_url = Column(String, nullable=False, unique=True)
    categories = Column(JSON, nullable=True)
    status_info = Column(JSON, nullable=True)
    pdf_url = Column(String, nullable=True)
    abstract = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

def get_engine():
    db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return create_engine(db_url)

def get_session():
    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
