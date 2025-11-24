import os
import re
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from mhanndalorian_bot import API, EndPoint
import utils


# ----------------------------------------------------
# Configuração de logging
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("tbleaderboard")

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
        API_KEY = load_env_var("MHANN_APIKEY")
        ALLYCODE = load_env_var("ALLYCODE")
        GUILD_ID = load_env_var("GUILD_ID")
        GCS_BUCKET_NAME = load_env_var("GCS_BUCKET_NAME")
    except ValueError as e:
        logger.critical(f"Falha ao carregar variáveis de ambiente: {e}")
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar API
    # ----------------------------------------------------
    try:
        mbot = API(api_key=API_KEY, allycode=ALLYCODE)
        logger.info("Cliente mhanndalorian_bot inicializado com sucesso.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar API: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Consultar TW Leaderboard
    # ----------------------------------------------------
    try:
        logger.info("Consultando endpoint TB...")
        resp = mbot.fetch_data(endpoint=EndPoint.TB, enums=True)
        logger.info("Dados obtidos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao buscar dados do TB: {e}", exc_info=True)
        raise SystemExit(1)

    """import json
    with open("resp_debug2.json", "w") as f:
        json.dump(resp, f, indent=2)
    exit(0)"""
    # ----------------------------------------------------
    # Validar retorno da API
    # ----------------------------------------------------
    try:
        if not isinstance(resp, dict):
            raise ValueError("Resposta da API não é um JSON válido.")

        if "territoryBattleStatus" not in resp:
            raise ValueError("Resposta da API não contém o campo 'territoryBattleStatus'.")

        logger.info("Validação do retorno concluída – resposta OK.")
    except Exception as e:
        logger.critical(f"Resposta inválida da API mhann: {e}", exc_info=True)
        raise SystemExit(1)

    resp = resp.get("territoryBattleStatus")
    # ----------------------------------------------------
    # Extrair timestamp do TW
    # ----------------------------------------------------
    try:
        match = re.search(r"O(\d+)", resp.get("instanceId"))
        if not match:
            raise ValueError(f"Erro inesperado no instanceId: {resp.get("instanceId")}")

        tb_timestamp_ms = int(match.group(1))
        tb_date = datetime.fromtimestamp(tb_timestamp_ms // 1000, tz=timezone.utc)
        logger.info(f"TW detectado na data: {tb_date.isoformat()}")

    except Exception as e:
        logger.error(f"Erro ao processar instanceId: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar cliente GCS
    # ----------------------------------------------------
    try:
        gcs = utils.GCSClient(GCS_BUCKET_NAME)
        logger.info("Cliente GCS inicializado com sucesso.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar o cliente GCS: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Construir caminho de upload
    # ----------------------------------------------------
    folder_path = f"{GUILD_ID}/events/tb/{tb_date.strftime('%Y%m%d')}"
    file_path = f"{folder_path}/tbleaderboard.json.gz"

    logger.info(f"Caminho final para upload: {file_path}")

    # ----------------------------------------------------
    # Upload para o GCS
    # ----------------------------------------------------
    try:
        success = gcs.upload_json_gzip(resp, file_path)
        if success:
            logger.info(f"Upload realizado: gs://{GCS_BUCKET_NAME}/{file_path}")
        else:
            logger.error("Falha ao enviar arquivo para o GCS.")
            raise SystemExit(1)
    except Exception as e:
        logger.error(f"Erro inesperado ao realizar upload: {e}", exc_info=True)
        raise SystemExit(1)

    logger.info("Execução concluída com sucesso.")


# ----------------------------------------------------
# Execução
# ----------------------------------------------------
if __name__ == "__main__":
    main()
