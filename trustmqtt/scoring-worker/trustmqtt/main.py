from .queue_consumer import run_consumer
from .storage.db import init_db

if __name__ == '__main__':
    print("Initializing database...")
    init_db()
    run_consumer()
