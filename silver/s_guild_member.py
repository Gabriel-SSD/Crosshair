import re
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
import os

import utils
from google.cloud import bigquery

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GUILD_ID = os.getenv("GUILD_ID")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = "silver"

if not GCS_BUCKET_NAME or not GUILD_ID:
    raise ValueError("Defina GCS_BUCKET_NAME e GUILD_ID no .env")

gcs = utils.GCSClient(GCS_BUCKET_NAME)

now = datetime.now(timezone.utc)
file_path = f"{GUILD_ID}/daily/{now.year}/{now.month:02}/{now.day:02}/guild.json.gz"

guild_raw = gcs.load_json_gzip(file_path)
if guild_raw is None:
    raise ValueError(f"Falha ao carregar o arquivo {file_path}")

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

client = bigquery.Client()

members_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.guild_members"

job_members = client.load_table_from_dataframe(
    df_guild_members,
    members_table_id,
    job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
)
job_members.result()

contrib_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.guild_contributions"

job_contrib = client.load_table_from_dataframe(
    df_contribut,
    contrib_table_id,
    job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
)
job_contrib.result()

print("Dados gravados com sucesso no BigQuery!")
