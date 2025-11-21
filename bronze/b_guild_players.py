import gzip
from io import BytesIO
from swgoh_comlink import SwgohComlink
from datetime import datetime, timezone
import json
from google.cloud import storage
from dotenv import load_dotenv
import os

load_dotenv()

guild_id = os.getenv('GUILD_ID')
bucket_name = os.getenv('GCS_BUCKET_NAME')

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

comlink = SwgohComlink()
now = datetime.now(timezone.utc)

def upload_json_gzip_to_gcs(data, path):
    try:
        json_bytes = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')

        out = BytesIO()
        with gzip.GzipFile(fileobj=out, mode='wb') as f:
            f.write(json_bytes)
        out.seek(0)

        blob = bucket.blob(path)
        blob.upload_from_file(
            out,
            content_type='application/octet-stream',
            client=storage_client
        )

        print(f"Upload gzip realizado: {path}")

    except Exception as e:
        print(f"Falha no upload gzip {path}: {e}")

folder_path = f"{guild_id}/{now.year}/{now.month:02}/{now.day:02}"

try:
    guild = comlink.get_guild(guild_id=guild_id, include_recent_guild_activity_info=True)
except Exception as e:
    print(f"Falha ao buscar guild: {e}")
    guild = {}

upload_json_gzip_to_gcs(guild, f"{folder_path}/guild.json.gz")

players = []
for member in guild.get("member", []):
    player_id = member.get("playerId")
    if not player_id:
        continue
    try:
        player_data = comlink.get_player(player_id=player_id)
        players.append(player_data)
    except Exception as e:
        print(f"Falha ao buscar player {player_id}: {e}")

upload_json_gzip_to_gcs(players, f"{folder_path}/players.json.gz")
