from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Generator

Base = declarative_base()

class PGClient:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        echo: bool = False  # Controls SQLAlchemy logging output - when True, logs all SQL statements
    ):
        """Initialize database connection with provided parameters."""
        dialect = "postgresql"
        driver = "psycopg2"
        self.connection_string = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.connection_string, echo=echo)
        self.Session = sessionmaker(bind=self.engine)
    
    
    def create_table_if_not_exist(self, model_class):
        """Create tables for a specific model class if they don't already exist."""
        model_class.__table__.create(self.engine, checkfirst=True)
    
    @contextmanager
    def session_scope(self) -> Generator:
        """Provide a transactional scope around operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def execute_raw(self, query: str, **params):
        """Execute a raw SQL query."""
        with self.engine.connect() as connection:
            result = connection.execute(query, **params)
            return [dict(row) for row in result]


