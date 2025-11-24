import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import requests
from google.cloud import bigquery
import google.generativeai as genai


# ----------------------------------------------------
# Configuração de logging
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("tw_report")


load_dotenv()


# ----------------------------------------------------
# Função utilitária para carregar variáveis de ambiente
# ----------------------------------------------------
def load_env_var(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        logger.error(f"Variável de ambiente ausente: {var_name}")
        raise ValueError(f"A variável {var_name} não está definida no .env")
    return value


def df_to_table(df: pd.DataFrame) -> str:
    """Converte um DataFrame em tabela formatada para exibição."""
    return "```\n" + df.to_string(index=False) + "\n```"


def main():

    # ----------------------------------------------------
    # Carregar variáveis de ambiente
    # ----------------------------------------------------
    try:
        BQ_PROJECT_ID = load_env_var("BQ_PROJECT_ID")
        DISCORD_WEBHOOK_URL = load_env_var("DISCORD_WEBHOOK_URL")
        GEMINI_API_KEY = load_env_var("GEMINI_API_KEY")
    except ValueError as e:
        logger.critical(f"Falha ao carregar variáveis de ambiente: {e}")
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar Gemini
    # ----------------------------------------------------
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")
        logger.info("Cliente Gemini inicializado com sucesso.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar Gemini: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar BigQuery Client
    # ----------------------------------------------------
    try:
        client = bigquery.Client()
        logger.info("Cliente BigQuery inicializado com sucesso.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar BigQuery: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Consulta SQL
    # ----------------------------------------------------
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

    try:
        logger.info("Executando consulta no BigQuery...")
        df = client.query(QUERY).to_dataframe()
        logger.info("Consulta concluída com sucesso.")
    except Exception as e:
        logger.critical(f"Erro ao consultar dados no BigQuery: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Validar resultado da consulta
    # ----------------------------------------------------
    if df.empty:
        logger.error("Nenhum dado encontrado no BigQuery!")
        raise SystemExit(1)

    table_str = df_to_table(df)

    # ----------------------------------------------------
    # Criar prompt para Gemini
    # ----------------------------------------------------
    prompt = f"""
You are Crosshair from Star Wars: The Bad Batch.
Keep your tone cold, precise, calm, tactical, and slightly sarcastic.
Short sentences. Direct. Military style.

Analyze the Territory War (SWGOH) performance table below and produce a concise, objective summary containing:

- Highlights of the players with the highest total banners
- Who contributed the most on offense
- Who contributed the most on defense
- Who performed rogue actions
- Any relevant observations or strategic weaknesses
- Clear, sharp, easy to read

TABLE:
{df.to_string(index=False)}
    """

    # ----------------------------------------------------
    # Gerar resumo com Gemini
    # ----------------------------------------------------
    try:
        logger.info("Gerando resumo com Gemini...")
        response = model.generate_content(prompt)
        summary = response.text
        logger.info("Resumo gerado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao gerar resumo com Gemini: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Enviar resumo para Discord
    # ----------------------------------------------------
    tw_date = df["tw_date"].iloc[0].strftime("%Y-%m-%d")

    payload = {
        "content": f"**TW - {tw_date}**\n\n{summary}\n\n"
    }

    try:
        logger.info("Enviando mensagem para Discord...")
        discord_response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if discord_response.status_code not in (200, 204):
            raise RuntimeError(f"Discord retornou erro: {discord_response.text}")
        logger.info("Mensagem enviada ao Discord com sucesso.")
    except Exception as e:
        logger.error(f"Falha ao enviar mensagem ao Discord: {e}", exc_info=True)
        raise SystemExit(1)

    logger.info("Execução concluída com sucesso.")


# ----------------------------------------------------
# Execução
# ----------------------------------------------------
if __name__ == "__main__":
    main()
