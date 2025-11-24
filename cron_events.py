import os
import logging
import subprocess
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import utils


# ----------------------------------------------------
# Configuração de logging
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("event_scheduler")


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


# ----------------------------------------------------
# Atualizar cron
# ----------------------------------------------------
def update_cron(job_name: str, cron_expr: str, script_path: str):
    """
    Atualiza o crontab para adicionar ou substituir um job identificado por job_name.
    """

    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        lines = result.stdout.splitlines() if result.returncode == 0 else []
        logger.info(f"Crontab atual carregado ({len(lines)} linhas).")
    except Exception as e:
        logger.error(f"Erro ao ler o crontab: {e}", exc_info=True)
        return False

    try:
        # Remover entradas antigas deste job
        lines = [l for l in lines if f"# {job_name}" not in l]

        # Adicionar entrada nova
        entry = f"{cron_expr} {
            os.getenv('BIN_PATH')} {script_path} # {job_name}"
        lines.append(entry)

        new_cron = "\n".join(lines) + "\n"
        process = subprocess.run(["crontab", "-"], input=new_cron, text=True)

        if process.returncode == 0:
            logger.info(f"Cron atualizado para job {job_name}: {cron_expr}")
            return True
        else:
            logger.error(f"Falha ao atualizar cron para {job_name}")
            return False

    except Exception as e:
        logger.error(f"Erro inesperado ao atualizar cron: {e}", exc_info=True)
        return False


# ----------------------------------------------------
# Função para extrair horário do evento
# ----------------------------------------------------
def get_event_schedule(event_type: str, gcs_client: utils.GCSClient, file_path: str):
    """
    Retorna a expressão cron correspondente ao evento.
    """

    try:
        events_raw = gcs_client.load_json_gzip(file_path)
        if not events_raw:
            logger.warning(f"Arquivo vazio ou inexistente: {file_path}")
            return None
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo {file_path}: {e}", exc_info=True)
        return None

    events = events_raw.get("events", [])
    target_event = next((e for e in events if e.get("type") == event_type), None)

    if not target_event:
        logger.info(f"Nenhum evento encontrado para {event_type}")
        return None

    try:
        instance_list = target_event.get("instance", [])
        if not instance_list:
            logger.info(f"Nenhuma instância encontrada para {event_type}")
            return None

        end_time_ms = int(instance_list[0].get("endTime"))
        end_datetime = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc)

        cron_datetime = end_datetime - timedelta(minutes=1)
        cron_expr = f"{
            cron_datetime.minute} {
            cron_datetime.hour} {
            cron_datetime.day} {
                cron_datetime.month} *"

        logger.info(f"Cron para {event_type}: {cron_expr}")

        return cron_expr

    except Exception as e:
        logger.error(f"Erro ao processar evento {event_type}: {e}", exc_info=True)
        return None


def main():

    # ----------------------------------------------------
    # Carregar variáveis de ambiente
    # ----------------------------------------------------
    try:
        GCS_BUCKET_NAME = load_env_var("GCS_BUCKET_NAME")
        RELATIVE_PATH = load_env_var("RELATIVE_PATH")
        BIN_PATH = load_env_var("BIN_PATH")  # usado no cron
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
        logger.critical(f"Erro ao inicializar GCSClient: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Carregar arquivo do calendário
    # ----------------------------------------------------
    now = datetime.now(timezone.utc)
    file_path = f"calendar/{now.year}/{now.month:02}/{now.day:02}/calendar.json.gz"

    logger.info(f"Arquivo de calendário: {file_path}")

    # ----------------------------------------------------
    # TERRITORY WAR
    # ----------------------------------------------------
    logger.info("Processando evento: TERRITORY_WAR_EVENT")
    tw_cron = get_event_schedule("TERRITORY_WAR_EVENT", gcs, file_path)

    if tw_cron:
        script_path = f"{RELATIVE_PATH}/bronze/tw_leaderboard.py"
        update_cron("TW_EVENT", tw_cron, script_path)
    else:
        logger.info("Nenhum evento TW encontrado.")

    # ----------------------------------------------------
    # TERRITORY BATTLE
    # ----------------------------------------------------
    logger.info("Processando evento: TERRITORY_BATTLE_EVENT")
    tb_cron = get_event_schedule("TERRITORY_BATTLE_EVENT", gcs, file_path)

    if tb_cron:
        script_path = f"{RELATIVE_PATH}/bronze/tb_leaderboard.py"
        update_cron("TB_EVENT", tb_cron, script_path)
    else:
        logger.info("Nenhum evento TB encontrado.")

    logger.info("Execução concluída com sucesso.")


# ----------------------------------------------------
# Execução
# ----------------------------------------------------
if __name__ == "__main__":
    main()
