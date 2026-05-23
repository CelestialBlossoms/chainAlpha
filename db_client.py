import time
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from config import DB_CONFIG

class DBClient:
    """A client for interacting with the PostgreSQL database."""

    def __init__(self, config=None):
        self.config = config or DB_CONFIG
        # psycopg2 uses 'dbname' instead of 'database' in connection params
        self.conn_params = self.config.copy()
        if 'database' in self.conn_params:
            self.conn_params['dbname'] = self.conn_params.pop('database')

    def _new_conn(self):
        """Create a fresh connection."""
        conn = psycopg2.connect(
            **self.conn_params,
            connect_timeout=10,
            options="-c lock_timeout=15s",
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )
        conn.autocommit = False
        return conn

    @contextmanager
    def get_conn(self):
        """Yield a psycopg2 connection context manager."""
        conn = self._new_conn()
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, op_fn_callable, retries=2):
        """
        Execute a DB operation with its own connection, reconnecting on loss.
        The op_fn_callable should accept a connection object as its first argument.
        """
        for attempt in range(retries + 1):
            conn = self._new_conn()
            try:
                result = op_fn_callable(conn)
                conn.commit()
                return result
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if attempt == retries:
                    print(f"Database operation failed after {retries} retries: {e}")
                    raise
                time.sleep(0.5)
            except Exception as e:
                conn.rollback()
                print(f"Database error: {e}")
                raise
            finally:
                conn.close()

# Singleton instance for easy access
db_client = DBClient()

# Maintain original function names from strategy_tracker.py for compatibility
def get_db():
    return db_client.get_conn()

def db_op(op_fn_callable, retries=2):
    return db_client.execute(op_fn_callable, retries)
