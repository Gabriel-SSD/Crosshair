from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import requests
import google.generativeai as genai
import os

load_dotenv()

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not BQ_PROJECT_ID or not DISCORD_WEBHOOK_URL or not GEMINI_API_KEY:
    raise ValueError("Defina BQ_PROJECT_ID, DISCORD_WEBHOOK_URL e GEMINI_API_KEY no .env")


genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

client = bigquery.Client()

QUERY = f"""
SELECT 
    gm.player_name,
    tw.total_banners,
    tw.ofensive_banners,
    tw.defensive_banners,
    tw.rogue_actions,
    tw.tw_date
FROM `{BQ_PROJECT_ID}.silver.tw_leaderboard` tw
INNER JOIN `{BQ_PROJECT_ID}.silver.guild_members` gm ON tw.player_id = gm.player_id
WHERE tw.tw_date = (SELECT MAX(tw_date) FROM `silver.tw_leaderboard`)
ORDER BY tw.total_banners DESC
"""

df = client.query(QUERY).to_dataframe()

if df.empty:
    raise ValueError("Nenhum dado encontrado no BigQuery!")


def df_to_table(df: pd.DataFrame) -> str:
    return "```\n" + df.to_string(index=False) + "\n```"

table_str = df_to_table(df)


prompt = f"""
You are Crosshair from Star Wars: The Bad Batch.
Keep your tone cold, precise, calm, tactical, and slightly sarcastic, the way Crosshair speaks.
Short sentences. Direct. Military style.

Analyze the Territory War (SWGOH) performance table below and produce a concise, objective summary containing:

- Highlights of the players with the highest total banners
- Who contributed the most on offense
- Who contributed the most on defense
- Who performed rogue actions
- Any relevant observations, patterns, or strategic weaknesses you detect
- Keep the writing natural, sharp, and easy to read

TABLE:
{df.to_string(index=False)}
"""

response = model.generate_content(prompt)
summary = response.text


payload = {
    "content": (
        f"**TW - {df['tw_date'].iloc[0].strftime('%Y-%m-%d')}**\n\n"
        f"{summary}\n\n"
    )
}

response = requests.post(DISCORD_WEBHOOK_URL, json=payload)

if response.status_code not in (200, 204):
    raise RuntimeError(f"Falha ao enviar para o Discord: {response.text}")

print("Relatório + resumo enviados ao Discord com sucesso!")
