from mhanndalorian_bot import API, EndPoint
from dotenv import load_dotenv
from datetime import datetime, timezone
import utils
import os, re


load_dotenv()

API_KEY = os.getenv("MHANN_APIKEY")
ALLYCODE = os.getenv("ALLYCODE")
GUILD_ID = os.getenv("GUILD_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

if not API_KEY or not ALLYCODE or not GUILD_ID or not GCS_BUCKET_NAME:
    raise ValueError("Certifique-se que MHANN_APIKEY, ALLYCODE, GUILD_ID e GCS_BUCKET_NAME estão no .env")


mbot = API(api_key=API_KEY, allycode=ALLYCODE)

resp = mbot.fetch_data(endpoint=EndPoint.TWLEADERBOARD)

gcs = utils.GCSClient(GCS_BUCKET_NAME)


tw_timestamp_ms = int(
    re.search(r'O(\d+)', resp.get('territoryMapId')).group(1)
)

tw_date = datetime.fromtimestamp(tw_timestamp_ms // 1000, tz=timezone.utc)

folder_path = f"{GUILD_ID}/events/tw/{tw_date.strftime('%Y%m%d')}"
file_path = f"{folder_path}/twleaderboard.json.gz"


success = gcs.upload_json_gzip(resp, file_path)

if success:
    print(f"Upload realizado com sucesso: gs://{GCS_BUCKET_NAME}/{file_path}")
else:
    print("Falha ao enviar arquivo para o GCS.")