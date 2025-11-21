import json, gzip, re
from io import BytesIO
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
import os
from datetime import datetime, timezone

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GUILD_ID = os.getenv("GUILD_ID")

if not GCS_BUCKET_NAME or not GUILD_ID:
    raise ValueError("Defina GCS_BUCKET_NAME e GUILD_ID no .env")

storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

now = datetime.now(timezone.utc)
file_path = f"{GUILD_ID}/{now.year}/{now.month:02}/{now.day:02}/guild.json.gz"
print(f"Lendo arquivo do GCS: {file_path}")

def load_json_gzip_from_gcs(path):
    blob = bucket.blob(path)
    data = blob.download_as_bytes()
    with gzip.GzipFile(fileobj=BytesIO(data), mode="rb") as f:
        return json.loads(f.read().decode("utf-8"))

guild_raw = load_json_gzip_from_gcs(file_path)
print("Arquivo guild.json.gz carregado com sucesso!")

members = guild_raw.get("member", [])

type_map = {
    "CONTRIBUTION_TYPE_TRIBUTE": "ticket",
    "CONTRIBUTION_TYPE_COMMENDATION": "token",
    "CONTRIBUTION_TYPE_DONATION": "donation"
}

df_contribut = pd.DataFrame([
    {"player_id": m["playerId"], **c} 
    for m in members for c in m.get("memberContribution", [])
])
df_contribut["datetime"] = now
df_contribut.columns = [
    re.sub(r'([A-Z]+)', r'_\1', c).replace("-", "_").lower().lstrip("_")
    for c in df_contribut.columns
]
df_contribut["type"] = df_contribut["type"].map(lambda x: type_map.get(x, x))

# Guild members
role_map = {
    "GUILD_LEADER": "leader",
    "GUILD_OFFICER": "officer",
    "GUILD_MEMBER": "member"
}

df_guild_members = pd.DataFrame([
    {
        "player_id": m.get("playerId"),
        "join_time": datetime.fromtimestamp(int(m.get("guildJoinTime") or 0), tz=timezone.utc),
        "role": m.get("memberLevel")
    }
    for m in members
])
df_guild_members["datetime"] = now
df_guild_members["role"] = df_guild_members["role"].map(lambda x: role_map.get(x, x))

print("Contributions:")
print(df_contribut.head())
print("\nGuild Members:")
print(df_guild_members.head())
