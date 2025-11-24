import subprocess
import logging
import time
import os
from dotenv import load_dotenv
from pathlib import Path

# ----------------------------
# Configuração de logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger("pipeline")


load_dotenv()

# ----------------------------
# Ordem dos pipelines Guild Member
# ----------------------------
SCRIPTS = [
    f"{os.getenv('RELATIVE_PATH')}/bronze/guild_member.py",
    f"{os.getenv('RELATIVE_PATH')}/silver/guild_member.py",
]


# ----------------------------
# Função para executar cada script
# ----------------------------
def run_script(script_path: str):
    script = Path(script_path)
    if not script.exists():
        logger.error(f"Script não encontrado: {script_path}")
        return False

    logger.info(f"\n▶️  Iniciando etapa: {script_path}\n")

    start = time.time()

    result = subprocess.run([os.getenv("BIN_PATH"), str(script)], capture_output=True, text=True)

    duration = round(time.time() - start, 2)

    # Logs
    if result.stdout:
        logger.info(f"[{script.name}] STDOUT:\n{result.stdout}")

    if result.stderr:
        logger.warning(f"[{script.name}] STDERR:\n{result.stderr}")

    if result.returncode != 0:
        logger.error(f"Erro ao executar {script_path} (Código {result.returncode})")
        return False

    logger.info(f"Etapa concluída: {script_path} ({duration}s)\n")
    return True


# ----------------------------
# Execução principal
# ----------------------------
def main():
    logger.info("\n================ DAILY PIPELINE ================\n")
    for script in SCRIPTS:
        if not run_script(script):
            logger.critical("PIPELINE INTERROMPIDA devido ao erro acima.\n")
            exit(1)

    logger.info("PIPELINE FINALIZADA COM SUCESSO!\n")


if __name__ == "__main__":
    main()
