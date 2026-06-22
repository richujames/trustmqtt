from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ..config import config
from .models_orm import Base

# SQLAlchemy Engine
engine = create_engine(config.database_url, echo=False)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Create all tables in the database if they don't exist yet."""
    Base.metadata.create_all(bind=engine)

def get_db():
    """Generator to provide a transactional database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
