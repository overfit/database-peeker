import sqlalchemy
import os
from dotenv import load_dotenv

load_dotenv()


class ConnectionService:
    def __init__(self):
        # Build the connection URL
        connection_url = sqlalchemy.engine.URL.create(
            "mssql+pyodbc",
            username=os.getenv("CLIENT_DB_USER"),
            password=os.getenv("CLIENT_DB_PASSWORD"),
            host=os.getenv("CLIENT_DB_HOST"),
            database=os.getenv("CLIENT_DB_NAME"),
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                # if needed, you can add other parameters, e.g.:
                "Encrypt": "yes",
                "TrustServerCertificate": "yes"
            }
        )
        # Create the SQLAlchemy engine
        self.engine = sqlalchemy.create_engine(connection_url)