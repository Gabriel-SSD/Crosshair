from datetime import datetime, timezone
from dotenv import load_dotenv
from swgoh_comlink import SwgohComlink
import utils
import os

load_dotenv()

guild_id = os.getenv('GUILD_ID')
bucket_name = os.getenv('GCS_BUCKET_NAME')

if not guild_id or not bucket_name:
    raise ValueError("Certifique-se de que GUILD_ID e GCS_BUCKET_NAME estejam definidos no .env")

storage_client = utils.GCSClient(bucket_name)

comlink = SwgohComlink()
now = datetime.now(timezone.utc)

folder_path = f"{guild_id}/daily/{now.year}/{now.month:02}/{now.day:02}"

try:
    guild = comlink.get_guild(guild_id=guild_id, include_recent_guild_activity_info=True, enums=True)
except Exception as e:
    print(f"Falha ao buscar guild: {e}")
    guild = {}

storage_client.upload_json_gzip(guild, f"{folder_path}/guild.json.gz")

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

storage_client.upload_json_gzip(players, f"{folder_path}/players.json.gz")