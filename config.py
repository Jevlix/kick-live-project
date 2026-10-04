import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = DATA_DIR / 'logs'
SUMMARIES_DIR = DATA_DIR / 'summaries'
DB_PATH = DATA_DIR / 'kick_live.db'

CHANNEL_SLUG = os.getenv('KICK_CHANNEL_SLUG', '').strip().lower()
KICK_CLIENT_ID = os.getenv('KICK_CLIENT_ID', '').strip()
KICK_CLIENT_SECRET = os.getenv('KICK_CLIENT_SECRET', '').strip()

API_POLL_SECONDS = max(5, int(os.getenv('KICK_API_POLL_SECONDS', '15')))
METADATA_POLL_SECONDS = max(15, int(os.getenv('KICK_METADATA_POLL_SECONDS', '30')))
RECONNECT_DELAY_SECONDS = max(1, int(os.getenv('KICK_RECONNECT_DELAY_SECONDS', '5')))
OFFLINE_GRACE_SECONDS = max(30, int(os.getenv('KICK_OFFLINE_GRACE_SECONDS', '90')))
DB_BATCH_SIZE = max(50, int(os.getenv('DB_BATCH_SIZE', '250')))
DB_BATCH_WAIT_MS = max(10, int(os.getenv('DB_BATCH_WAIT_MS', '250')))
MAX_JSONL_MB = max(10, int(os.getenv('MAX_JSONL_MB', '200')))
RAW_JSON_IN_DB = os.getenv('RAW_JSON_IN_DB', '0').lower() in {'1', 'true', 'yes'}
PRINT_CHAT = os.getenv('PRINT_CHAT', '0').lower() in {'1', 'true', 'yes'}
LOG_OFFSTREAM_CHAT = os.getenv('LOG_OFFSTREAM_CHAT', '1').lower() in {'1', 'true', 'yes'}

PUSHER_KEY = os.getenv('KICK_PUSHER_KEY', '32cbd69e4b950bf97679')
PUSHER_REGION = os.getenv('KICK_PUSHER_REGION', 'us2')
PUSHER_WS_URL = (
    f'wss://ws-{PUSHER_REGION}.pusher.com/app/{PUSHER_KEY}'
    '?protocol=7&client=js&version=8.4.0&flash=false'
)

API_BASE = 'https://api.kick.com'
OAUTH_BASE = 'https://id.kick.com'
INTERNAL_CHANNEL_URL = 'https://kick.com/api/v2/channels/{slug}'

for path in (DATA_DIR, LOGS_DIR, SUMMARIES_DIR):
    path.mkdir(parents=True, exist_ok=True)
