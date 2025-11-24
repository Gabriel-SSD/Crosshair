import os
import re
import logging
from datetime import datetime, timezone
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
logger = logging.getLogger("guild_daily_to_bq")


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
        BQ_DATASET = "silver"  # dataset fixo
    except ValueError as e:
        logger.critical(f"Falha ao carregar variáveis de ambiente: {e}")
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar cliente GCS
    # ----------------------------------------------------
    try:
        gcs = utils.GCSClient(GCS_BUCKET_NAME)
        logger.info("Cliente GCS inicializado.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar cliente GCS: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Construir caminho do arquivo
    # ----------------------------------------------------
    now = datetime.now(timezone.utc)
    file_path = f"{GUILD_ID}/daily/{now.year}/{now.month:02}/{now.day:02}/guild.json.gz"

    logger.info(f"Carregando arquivo: {file_path}")

    # ----------------------------------------------------
    # Carregar dados da guild do GCS
    # ----------------------------------------------------
    try:
        guild_raw = gcs.load_json_gzip(file_path)
        if guild_raw is None:
            raise ValueError("Arquivo retornou None.")
        logger.info("Dados da guild carregados com sucesso.")
    except Exception as e:
        logger.critical(f"Falha ao carregar arquivo do GCS: {e}", exc_info=True)
        raise SystemExit(1)

    members = guild_raw.get("member", [])
    logger.info(f"{len(members)} membros encontrados na guild.")

    # ----------------------------------------------------
    # Processar contribuições
    # ----------------------------------------------------
    try:
        type_map = {
            "CONTRIBUTION_TYPE_TRIBUTE": "ticket",
            "CONTRIBUTION_TYPE_COMMENDATION": "token",
            "CONTRIBUTION_TYPE_DONATION": "donation",
        }

        df_contribut = pd.DataFrame(
            [
                {"player_id": m["playerId"], **c}
                for m in members
                for c in m.get("memberContribution", [])
            ]
        )

        if df_contribut.empty:
            logger.warning("Nenhuma contribuição encontrada.")

        df_contribut["datetime"] = now

        # Normalizar nomes das colunas
        df_contribut.columns = [
            re.sub(r"([A-Z]+)", r"_\1", c).replace("-", "_").lower().lstrip("_")
            for c in df_contribut.columns
        ]

        df_contribut["type"] = df_contribut["type"].map(lambda x: type_map.get(x, x))

        logger.info("Dataframe de contribuições processado.")

    except Exception as e:
        logger.error(f"Erro ao processar contribuições: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Processar membros da guild
    # ----------------------------------------------------
    try:
        role_map = {
            "GUILD_LEADER": "leader",
            "GUILD_OFFICER": "officer",
            "GUILD_MEMBER": "member",
        }

        df_guild_members = pd.DataFrame(
            [
                {
                    "player_id": m.get("playerId"),
                    "player_name": m.get("playerName"),
                    "join_time": datetime.fromtimestamp(
                        int(m.get("guildJoinTime") or 0), tz=timezone.utc
                    ),
                    "role": m.get("memberLevel"),
                }
                for m in members
            ]
        )

        df_guild_members["datetime"] = now
        df_guild_members["role"] = df_guild_members["role"].map(lambda x: role_map.get(x, x))

        logger.info("Dataframe de membros da guild processado.")

    except Exception as e:
        logger.error(f"Erro ao processar membros da guild: {e}", exc_info=True)
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
    # Inserir df_guild_members (WRITE_TRUNCATE)
    # ----------------------------------------------------
    try:
        table_members = f"{BQ_PROJECT_ID}.{BQ_DATASET}.guild_members"
        job_members = client.load_table_from_dataframe(
            df_guild_members,
            table_members,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        )
        job_members.result()
        logger.info(f"Membros gravados com sucesso em {table_members}")
    except Exception as e:
        logger.error(f"Erro ao gravar guild_members no BigQuery: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inserir df_contribut (WRITE_APPEND)
    # ----------------------------------------------------
    try:
        table_contrib = f"{BQ_PROJECT_ID}.{BQ_DATASET}.guild_contributions"
        job_contrib = client.load_table_from_dataframe(
            df_contribut,
            table_contrib,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        job_contrib.result()
        logger.info(f"Contribuições gravadas com sucesso em {table_contrib}")
    except Exception as e:
        logger.error(
            f"Erro ao gravar guild_contributions no BigQuery: {e}",
            exc_info=True,
        )
        raise SystemExit(1)

    logger.info("Execução concluída com sucesso.")


# ----------------------------------------------------
# Execução
# ----------------------------------------------------
if __name__ == "__main__":
    main()
