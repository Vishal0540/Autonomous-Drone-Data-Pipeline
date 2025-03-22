from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection


class CassandraClient:
    """Class for Cassandra database operations using asyncio"""
    
    def __init__(self, hosts, keyspace="aerodronefleet", port=9042):
        """Initialize the client - cluster is created immediately"""
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        
        # Create cluster with async connection class right away
        self.cluster = Cluster(
            self.hosts, 
            port=self.port,
            connection_class=AsyncioConnection
        )
        
        # We'll initialize the keyspace in the async init method
        self._keyspace_initialized = False
    
    async def async_init(self):
        """Async initialization - must be called after creating the object"""
        if not self._keyspace_initialized:
            # Create temporary session just for keyspace setup
            temp_session = await self.cluster.connect_async()
            
            # Create keyspace if it doesn't exist
            await temp_session.execute_async("""
                CREATE KEYSPACE IF NOT EXISTS drone_telemetry
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """)
            
            # Close this temporary session
            await temp_session.shutdown()
            self._keyspace_initialized = True
    
    async def get_new_async_session(self):
        """Create and return a brand new session"""
        if not self._keyspace_initialized:
            # Initialize if not done yet
            await self.async_init()
            
        new_session = await self.cluster.connect_async(self.keyspace)
        return new_session
    
    async def close(self):
        """Close the cluster connection"""
        if self.cluster:
            self.cluster.shutdown()