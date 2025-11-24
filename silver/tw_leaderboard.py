import os
import re
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery
import utils


# ----------------------------------------------------
# Configuração de logging
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("twleaderboard_to_bq")


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


def main():

    # ----------------------------------------------------
    # Carregar variáveis de ambiente
    # ----------------------------------------------------
    try:
        GCS_BUCKET_NAME = load_env_var("GCS_BUCKET_NAME")
        GUILD_ID = load_env_var("GUILD_ID")
        BQ_PROJECT_ID = load_env_var("BQ_PROJECT_ID")
        BQ_DATASET = "silver"
    except ValueError as e:
        logger.critical(f"Erro ao carregar variáveis de ambiente: {e}")
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar cliente GCS
    # ----------------------------------------------------
    try:
        gcs = utils.GCSClient(GCS_BUCKET_NAME)
        logger.info("Cliente GCS inicializado.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar GCSClient: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Construir caminho do arquivo TW leaderboards
    # ----------------------------------------------------
    now = datetime.now(timezone.utc)
    yesterday = (now - timedelta(days=1)).strftime("%Y%m%d")

    file_path = f"{GUILD_ID}/events/tw/{yesterday}/twleaderboard.json.gz"
    logger.info(f"Carregando arquivo: {file_path}")

    # ----------------------------------------------------
    # Carregar arquivo TW do GCS
    # ----------------------------------------------------
    try:
        tw_l_raw = gcs.load_json_gzip(file_path)
        if tw_l_raw is None:
            raise ValueError("Arquivo retornou None.")
        logger.info("Arquivo TW leaderboard carregado com sucesso.")
    except Exception as e:
        logger.critical(f"Falha ao carregar arquivo TW: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Validar conteúdo
    # ----------------------------------------------------
    try:
        data = tw_l_raw["data"]
    except Exception:
        logger.critical("O campo 'data' não existe no JSON de TW.", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Criar DataFrames das métricas
    # ----------------------------------------------------
    try:
        total = pd.DataFrame(data.get("totalBanners", []), columns=["memberId", "banners"])
        attack = pd.DataFrame(data.get("attackBanners", []), columns=["memberId", "banners"])
        defense = pd.DataFrame(data.get("defenseBanners", []), columns=["memberId", "banners"])
        rogue = pd.DataFrame(data.get("rogueActions", []), columns=["memberId", "rogueActions"])
        logger.info("DataFrames de métricas criados.")
    except Exception as e:
        logger.critical(f"Erro ao criar DataFrames das métricas: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Normalizar nomes das colunas
    # ----------------------------------------------------
    try:
        total = total.rename(columns={"memberId": "player_id", "banners": "total_banners"})
        attack = attack.rename(columns={"memberId": "player_id", "banners": "ofensive_banners"})
        defense = defense.rename(columns={"memberId": "player_id", "banners": "defensive_banners"})
        rogue = rogue.rename(columns={"memberId": "player_id", "rogueActions": "rogue_actions"})
        logger.info("Colunas renomeadas.")
    except Exception as e:
        logger.error(f"Erro ao renomear colunas: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Mesclar todos os DataFrames
    # ----------------------------------------------------
    try:
        df = (
            total.merge(attack, on="player_id", how="outer")
            .merge(defense, on="player_id", how="outer")
            .merge(rogue, on="player_id", how="outer")
        )
        logger.info("DataFrames mesclados com sucesso.")
    except Exception as e:
        logger.critical(f"Erro ao mesclar DataFrames: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Normalizar colunas numéricas
    # ----------------------------------------------------
    try:
        numeric_cols = [
            "total_banners",
            "ofensive_banners",
            "defensive_banners",
            "rogue_actions",
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df[numeric_cols] = df[numeric_cols].fillna(0).astype(int)

        logger.info("Colunas numéricas normalizadas.")
    except Exception as e:
        logger.error(f"Erro ao normalizar valores numéricos: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Extrair data do TW
    # ----------------------------------------------------
    try:
        match = re.search(r"O(\d+)", tw_l_raw.get("territoryMapId", ""))
        if not match:
            raise ValueError("territoryMapId não tem o formato esperado.")

        tw_timestamp_ms = int(match.group(1))
        tw_date = datetime.fromtimestamp(tw_timestamp_ms // 1000, tz=timezone.utc)
        df["tw_date"] = tw_date

        logger.info(f"TW identificado com data UTC: {tw_date.isoformat()}")

    except Exception as e:
        logger.error(f"Erro ao extrair data do TW: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar BigQuery Client
    # ----------------------------------------------------
    try:
        client = bigquery.Client()
        logger.info("Cliente BigQuery inicializado.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar BigQuery: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Gravar dados no BigQuery
    # ----------------------------------------------------
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.tw_leaderboard"

    try:
        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        job.result()
        logger.info(f"Dados gravados com sucesso no BigQuery: {table_id}")
    except Exception as e:
        logger.error(f"Erro ao gravar dados no BigQuery: {e}", exc_info=True)
        raise SystemExit(1)

    logger.info("Execução concluída com sucesso.")


# ----------------------------------------------------
# Execução
# ----------------------------------------------------
if __name__ == "__main__":
    main()
