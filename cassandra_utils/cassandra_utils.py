from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import asyncio
import os

class CassandraClient:
    """Class for Cassandra database operations using synchronous connection"""
    
    def __init__(self, keyspace="aerodronefleet", cloud_config=None):
        """Initialize the client - cluster is created immediately"""
        print("Initializing Cassandra client")
        self.keyspace = keyspace
        
        if cloud_config:
            # Connect to Astra DB cloud
            self.cloud_config = {
                'secure_connect_bundle': cloud_config['bundle_path'],
                'client_id': cloud_config['client_id'],
                'client_secret': cloud_config['client_secret']
            }
            
            auth_provider = PlainTextAuthProvider(
                self.cloud_config['client_id'],
                self.cloud_config['client_secret']
            )
            
            self.cluster = Cluster(
                cloud=self.cloud_config,
                auth_provider=auth_provider
            )
        else:
            # Local connection fallback
            self.cluster = Cluster(
                ['localhost'],
                port=9042,
                connect_timeout=15
            )
        
    def get_session(self):
        session = self.cluster.connect(self.keyspace)
        return session
    
    def close(self):
        """Close the cluster connection"""
        if self.cluster:
            self.cluster.shutdown()
