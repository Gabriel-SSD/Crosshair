import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from swgoh_comlink import SwgohComlink
import utils


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("guild_daily")

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
        GUILD_ID = load_env_var("GUILD_ID")
        GCS_BUCKET_NAME = load_env_var("GCS_BUCKET_NAME")
    except ValueError as e:
        logger.critical(f"Falha ao carregar variáveis de ambiente: {e}")
        raise SystemExit(1)

    # ----------------------------------------------------
    # Inicializar clientes
    # ----------------------------------------------------
    try:
        storage = utils.GCSClient(GCS_BUCKET_NAME)
        logger.info("Cliente GCS inicializado.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar cliente GCS: {e}", exc_info=True)
        raise SystemExit(1)

    try:
        comlink = SwgohComlink()
        logger.info("Cliente SwgohComlink inicializado.")
    except Exception as e:
        logger.critical(f"Erro ao inicializar SwgohComlink: {e}", exc_info=True)
        raise SystemExit(1)

    # ----------------------------------------------------
    # Construir caminho de destino
    # ----------------------------------------------------
    now = datetime.now(timezone.utc)
    folder_path = f"{GUILD_ID}/daily/{now.year}/{now.month:02}/{now.day:02}"
    logger.info(f"Caminho final para upload: {folder_path}")

    # ----------------------------------------------------
    # Buscar dados da guild
    # ----------------------------------------------------
    try:
        logger.info("Consultando dados da guild...")
        guild = comlink.get_guild(
            guild_id=GUILD_ID,
            include_recent_guild_activity_info=True,
            enums=True,
        )
        logger.info("Dados da guild obtidos com sucesso.")
    except Exception as e:
        logger.error(f"Falha ao buscar dados da guild: {e}", exc_info=True)
        guild = {}

    # ----------------------------------------------------
    # Upload da guild
    # ----------------------------------------------------
    try:
        guild_path = f"{folder_path}/guild.json.gz"
        success = storage.upload_json_gzip(guild, guild_path)
        if success:
            logger.info(f"Guild salva: gs://{GCS_BUCKET_NAME}/{guild_path}")
        else:
            logger.error("Falha ao fazer upload do arquivo da guild.")
    except Exception as e:
        logger.error(f"Erro inesperado ao salvar guild: {e}", exc_info=True)

    # ----------------------------------------------------
    # Buscar dados dos jogadores
    # ----------------------------------------------------
    players = []
    members = guild.get("member", [])

    logger.info(f"Iniciando coleta: {len(members)} membros encontrados.")

    for member in members:
        player_id = member.get("playerId")
        if not player_id:
            logger.warning("Membro sem playerId encontrado, ignorando.")
            continue

        try:
            player_data = comlink.get_player(player_id=player_id)
            players.append(player_data)
            logger.info(f"Player {player_id} coletado.")
        except Exception as e:
            logger.error(f"Falha ao buscar player {player_id}: {e}", exc_info=True)

    # ----------------------------------------------------
    # Upload dos players
    # ----------------------------------------------------
    try:
        players_path = f"{folder_path}/players.json.gz"
        success = storage.upload_json_gzip(players, players_path)
        if success:
            logger.info(f"Jogadores salvos: gs://{GCS_BUCKET_NAME}/{players_path}")
        else:
            logger.error("Falha ao fazer upload do arquivo de players.")
    except Exception as e:
        logger.error(
            f"Erro inesperado ao salvar dados dos jogadores: {e}",
            exc_info=True,
        )

    logger.info("Execução concluída com sucesso.")


# ----------------------------------------------------
# Execução
# ----------------------------------------------------
if __name__ == "__main__":
    main()
