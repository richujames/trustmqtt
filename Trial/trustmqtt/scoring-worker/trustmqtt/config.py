import os
from dataclasses import dataclass

@dataclass
class Config:
    redis_host: str = os.getenv('REDIS_HOST', 'localhost')
    redis_port: int = int(os.getenv('REDIS_PORT', '6379'))
    database_url: str = os.getenv('DATABASE_URL', 'sqlite:///trustmqtt.db')
    shadow_mode: bool = os.getenv('SHADOW_MODE', 'true').lower() in ('1','true','yes')

config = Config()
