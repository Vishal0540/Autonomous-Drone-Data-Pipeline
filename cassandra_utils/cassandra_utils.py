from cassandra.cluster import Cluster
import asyncio

class CassandraClient:
    """Class for Cassandra database operations using synchronous connection"""
    
    def __init__(self, hosts, keyspace="aerodronefleet", port=9042):
        """Initialize the client - cluster is created immediately"""
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        
        # Create cluster with standard connection
        self.cluster = Cluster(
            self.hosts, 
            port=self.port,
            connect_timeout=15 
        )
        
        self._keyspace_initialized = False
        self.initialize_keyspace()
    
    def initialize_keyspace(self):
        """Initialize keyspace - called automatically during initialization"""
        if not self._keyspace_initialized:
            # Create temporary session without keyspace
            temp_session = self.cluster.connect()
            
            # Create keyspace if it doesn't exist
            create_keyspace_query = """
                CREATE KEYSPACE IF NOT EXISTS drone_telemetry
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
            temp_session.execute(create_keyspace_query)
            
            # Close this temporary session
            temp_session.shutdown()
            self._keyspace_initialized = True
    
    def get_session(self):
        """Create and return a new session"""
        if not self._keyspace_initialized:
            # Initialize if not done yet
            self.initialize_keyspace()
            
        # Connect with keyspace
        session = self.cluster.connect(self.keyspace)
        return session
    
    # async def execute_async_query(self, query, params):
    #     loop = asyncio.get_event_loop()
    #     future = self.session.execute_async(query, params)
        
    #     # Convert the Cassandra future to an asyncio future
    #     return await loop.run_in_executor(
    #         None,
    #         lambda: future.result()
    #     )
    
    def close(self):
        """Close the cluster connection"""
        if self.cluster:
            self.cluster.shutdown()
            

