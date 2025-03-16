from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Generator

Base = declarative_base()

class DatabaseManager:
    """
    A minimal SQLAlchemy database manager with core functionality.
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        dialect: str = "postgresql",
        driver: str = "psycopg2",
        echo: bool = False
    ):
        """Initialize database connection with provided parameters."""
        self.connection_string = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.connection_string, echo=echo)
        self.Session = sessionmaker(bind=self.engine)
    
    def create_tables_if_not_exist(self):
        """Create tables only if they don't already exist."""
        Base.metadata.create_all(self.engine, checkfirst=True)
    
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


# Example usage:
if __name__ == "__main__":
    # Example model definition
    from sqlalchemy import Column, String
    
    class DroneProfile(Base):
        __tablename__ = "drone_profiles"
        
        drone_id = Column(String(50), primary_key=True)
        model = Column(String(100), nullable=False)
        hardware_id = Column(String(100), unique=True, nullable=False)
    
    # Initialize the database manager
    db_manager = DatabaseManager(
        host="localhost",
        port=5432,
        database="drone_db",
        username="postgres",
        password="password"
    )
    
    # Create tables if they don't exist
    db_manager.create_tables_if_not_exist()
    
    # Execute raw SQL
    result = db_manager.execute_raw("SELECT * FROM drone_profiles")
    print(result)
    
    # Use session for operations
    with db_manager.session_scope() as session:
        # Create
        drone = DroneProfile(drone_id="DRN001", model="QuadX", hardware_id="HW123")
        session.add(drone)
        
        # Query
        drones = session.query(DroneProfile).all()
        print(drones)