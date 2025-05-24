from dotenv import load_dotenv
from os import getenv
from mhanndalorian_bot import API, EndPoint
from gcp_utils import GCSManager, BQManager
from utils import safe_init, setup_logging
import logging

load_dotenv()
setup_logging("logs/tw_leaderboard.log")

mbot = safe_init(API, "mbot", api_key=getenv('MHANN_API_KEY'), allycode=getenv('MHANN_API_ALLYCODE'))
gcs = safe_init(GCSManager, "gcs")
bq = safe_init(BQManager, "bq")

if all([mbot, gcs, bq]):
    try:
        tw_leaderboard = mbot.fetch_data(endpoint=EndPoint.TWLEADERBOARD)
        if isinstance(tw_leaderboard, dict):
            if tw_leaderboard['code'] == 0:
                territory_map_id = tw_leaderboard['territoryMapId']
                blob_path = f"gcp_bucket_echo/swgoh/iO-khl_0TVu64OussT1Y7g/tw_leaderboard/{territory_map_id}.json"
                blob_exists = gcs.blob_exists(
                    "gcp_bucket_echo/swgoh/iO-khl_0TVu64OussT1Y7g/tw_leaderboard",
                    territory_map_id
                )

                gcs.save_to_gcs(
                    data=tw_leaderboard,
                    bucket_path="gcp_bucket_echo/swgoh/iO-khl_0TVu64OussT1Y7g/tw_leaderboard",
                    file_name=territory_map_id
                )

                if blob_exists:
                    logging.info(f"Filename {territory_map_id}.json already exists, overwriting.")
                else:
                    logging.info(f"Filename {territory_map_id}.json created.")

                bq.load_data_from_gcs(
                    uri=blob_path,
                    dataset="gcp_echo_bronze",
                    table="bronze_tw_leaderboard",
                    truncate=False
                )
                bq.execute_query(
                    query_filename='sql/insert_tw_leaderboard.sql',
                    query_name="Insert Silver TW Leaderboard"
                )
            else:
                raise ValueError(tw_leaderboard)
    except Exception as e:
        logging.error(f"An error occurred while processing TW leaderboard: {e}")