from mhanndalorian_bot import API, EndPoint
from dotenv import load_dotenv
from datetime import datetime, timezone
import utils
import os

load_dotenv()

API_KEY = os.getenv("MHANN_APIKEY")
ALLYCODE = os.getenv("ALLYCODE")
GUILD_ID = os.getenv("GUILD_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

if not API_KEY or not ALLYCODE or not GUILD_ID or not GCS_BUCKET_NAME:
    raise ValueError(
        "Certifique-se que MHANN_APIKEY, ALLYCODE, GUILD_ID e GCS_BUCKET_NAME estão no .env"
    )

mbot = API(api_key=API_KEY, allycode=ALLYCODE)

resp = mbot.fetch_data(endpoint=EndPoint.EVENTS, enums=True)

gcs = utils.GCSClient(GCS_BUCKET_NAME)

now = datetime.now(timezone.utc)
folder_path = f"calendar/{now.year}/{now.month:02}/{now.day:02}"
file_path = f"{folder_path}/calendar.json.gz"


success = gcs.upload_json_gzip(resp, file_path)

if success:
    print(f"Upload realizado com sucesso: gs://{GCS_BUCKET_NAME}/{file_path}")
else:
    print("Falha ao enviar arquivo para o GCS.")