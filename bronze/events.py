import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from mhanndalorian_bot import API, EndPoint
import utils
import warnings


warnings.filterwarnings(
    "ignore",
    message="As the c extension couldn't be imported, `google-crc32c`"
    " is using a pure python implementation",
    category=RuntimeWarning,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("events")

load_dotenv()


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
    # Buscar dados
    # ----------------------------------------------------
    try:
        logger.info("Consultando endpoint EVENTS...")
        resp = mbot.fetch_data(endpoint=EndPoint.EVENTS, enums=True)
        logger.info("Dados obtidos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao buscar dados dos eventos: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Validar retorno da API
    # ----------------------------------------------------
    try:
        if not isinstance(resp, dict):
            raise ValueError("Resposta da API não é um JSON válido.")

        if "code" not in resp:
            raise ValueError("Resposta da API não contém o campo 'code'.")

        if resp["code"] != 0:
            raise RuntimeError(
                f"API error: code={resp['code']}, Message={resp.get('message')}"
            )

        logger.info("Validação do campo 'code' concluída – resposta OK.")

    except Exception as e:
        logger.critical(f"Resposta inválida da API mhann: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar cliente GCS
    # ----------------------------------------------------
    try:
        gcs = utils.GCSClient(GCS_BUCKET_NAME)
        logger.info("Cliente GCS inicializado.")
    except Exception as e:
        logger.critical(
            f"Erro ao inicializar o cliente GCS: {e}", exc_info=True
        )
        raise SystemExit(1)

    # ----------------------------------------------------
    # Caminho do arquivo
    # ----------------------------------------------------
    now = datetime.now(timezone.utc)
    folder_path = f"calendar/{now.year}/{now.month:02}/{now.day:02}"
    file_path = f"{folder_path}/calendar.json.gz"

    logger.info(f"Caminho final para upload: {file_path}")

    # ----------------------------------------------------
    # Upload
    # ----------------------------------------------------
    try:
        success = gcs.upload_json_gzip(resp, file_path)
        if success:
            logger.info(
                f"Upload realizado: gs://{GCS_BUCKET_NAME}/{file_path}"
            )
        else:
            logger.error("Falha ao enviar arquivo para o GCS.")
            raise SystemExit(1)
    except Exception as e:
        logger.error(f"Erro inesperado ao realizar upload: {e}", exc_info=True)
        raise SystemExit(1)

    logger.info("Execução concluída com sucesso.")


if __name__ == "__main__":
    main()
