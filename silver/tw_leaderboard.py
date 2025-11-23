from datetime import datetime, timezone, timedelta

import pandas as pd
from dotenv import load_dotenv
import os, json, re

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
yesterday = (now - timedelta(days=1)).strftime("%Y%m%d")

file_path = f"{GUILD_ID}/events/tw/{yesterday}/twleaderboard.json.gz"

tw_l_raw = gcs.load_json_gzip(file_path)
if tw_l_raw is None:
    raise ValueError(f"Falha ao carregar o arquivo {file_path}")

with open("tw_l_raw.json", "w", encoding="utf-8") as f:
    json.dump(tw_l_raw, f, ensure_ascii=False, indent=2)

data = tw_l_raw["data"]

total = pd.DataFrame(data.get("totalBanners", []), columns=["memberId", "banners"])
attack = pd.DataFrame(data.get("attackBanners", []), columns=["memberId", "banners"])
defense = pd.DataFrame(data.get("defenseBanners", []), columns=["memberId", "banners"])
rogue = pd.DataFrame(data.get("rogueActions", []), columns=["memberId", "rogueActions"])


total   = total.rename(columns={"memberId": "player_id", "banners": "total_banners"})
attack  = attack.rename(columns={"memberId": "player_id", "banners": "ofensive_banners"})
defense = defense.rename(columns={"memberId": "player_id", "banners": "defensive_banners"})
rogue   = rogue.rename(columns={"memberId": "player_id", "rogueActions": "rogue_actions"})


df = (
    total.merge(attack, on="player_id", how="outer")
         .merge(defense, on="player_id", how="outer")
         .merge(rogue, on="player_id", how="outer")
)


numeric_cols = ["total_banners", "ofensive_banners", "defensive_banners", "rogue_actions"]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")


df[numeric_cols] = df[numeric_cols].fillna(0)
df[numeric_cols] = df[numeric_cols].astype(int)

tw_timestamp_ms = int(
    re.search(r'O(\d+)', tw_l_raw.get('territoryMapId')).group(1)
)

tw_date = datetime.fromtimestamp(tw_timestamp_ms // 1000, tz=timezone.utc)

df["tw_date"] = tw_date

table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.tw_leaderboard"
client = bigquery.Client()

job = client.load_table_from_dataframe(
    df,
    table_id,
    job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
)
job.result()

print("Dados gravados com sucesso no BigQuery!")
