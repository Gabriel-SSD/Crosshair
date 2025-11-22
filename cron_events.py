from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os
import utils
import subprocess

def update_cron(job_name: str, cron_expr: str, script_path: str):
    """
    Atualiza o crontab para adicionar ou substituir um job identificado por job_name.

    Args:
        job_name (str): Identificador único do job (usado como comentário)
        cron_expr (str): Expressão cron (ex: "0 14 23 9 *")
        script_path (str): Caminho do script Python a ser executado
    """
    # Lê crontab atual
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    lines = result.stdout.splitlines() if result.returncode == 0 else []

    # Remove qualquer job antigo com mesmo nome
    lines = [l for l in lines if f"# {job_name}" not in l]

    # Adiciona novo job com comentário identificador
    lines.append(f"{cron_expr} {os.getenv("BIN_PATH")} {script_path} # {job_name}")

    # Atualiza o crontab
    new_cron = "\n".join(lines) + "\n"
    process = subprocess.run(["crontab", "-"], input=new_cron, text=True)
    if process.returncode == 0:
        print(f"Cron atualizado para {job_name}: {cron_expr}")
    else:
        print(f"Falha ao atualizar cron para {job_name}")


def get_event_schedule(event_type: str, gcs_client: utils.GCSClient, file_path: str):
    """
    Retorna a instância do evento, endTime e expressão cron para um tipo de evento.

    Args:
        event_type (str): Ex: "TERRITORY_WAR_EVENT" ou "TERRITORY_BATTLE_EVENT"
        gcs_client (utils.GCSClient): Cliente GCS já inicializado
        file_path (str): Caminho do arquivo calendar.json.gz no GCS

    Returns:
        str | None
    """
    events_raw = gcs_client.load_json_gzip(file_path)
    if not events_raw:
        return None,

    events = events_raw.get("events", [])
    target_event = next((e for e in events if e.get("type") == event_type), None)

    if target_event:
        instance_list = target_event.get("instance", [])
        if instance_list:
            end_time_ms = int(instance_list[0].get("endTime"))
            end_datetime = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc)
            cron_datetime = end_datetime - timedelta(minutes=1)
            cron_expr = f"{cron_datetime.minute} {cron_datetime.hour} {cron_datetime.day} {cron_datetime.month} *"
            return cron_expr

    return None


load_dotenv()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

gcs = utils.GCSClient(GCS_BUCKET_NAME)

now = datetime.now(timezone.utc)
file_path = f"calendar/{now.year}/{now.month:02}/{now.day:02}/calendar.json.gz"

# Territory War
tw_cron = get_event_schedule("TERRITORY_WAR_EVENT", gcs, file_path)
if tw_cron:
    update_cron("TW_EVENT", tw_cron, f"{os.getenv("RELATIVE_PATH")}/bronze/b_twleaderboard.py")
else:
    print("Nenhum evento TW encontrado")

# Territory Battle
tb_cron = get_event_schedule("TERRITORY_BATTLE_EVENT", gcs, file_path)
if tb_cron:
    update_cron("TB_EVENT", tb_cron, f"{os.getenv("RELATIVE_PATH")}/bronze/b_tbleaderboard.py")
else:
    print("Nenhum evento TB encontrado")
