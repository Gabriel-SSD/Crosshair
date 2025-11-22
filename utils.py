from __future__ import annotations
import gzip
import json
import logging
from io import BytesIO
from typing import Any, Dict, Optional

from google.cloud import storage

logger = logging.getLogger(__name__)


class GCSClient:
    """
    Wrapper para operações com Google Cloud Storage, incluindo
    upload e download de JSON compactado (gzip).
    """

    def __init__(self, bucket_name: str, client: Optional[storage.Client] = None):
        """
        Args:
            bucket_name (str): Nome do bucket no Google Cloud Storage.
            client (storage.Client, opcional): Cliente customizado.
        """
        self.client = client or storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)

    # ----------------------------------------
    # Internos: JSON <-> GZIP
    # ----------------------------------------
    @staticmethod
    def _json_to_gzip_bytes(data: Dict[str, Any]) -> BytesIO:
        """Converte dict em JSON.gz em memória."""
        json_bytes = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        buffer = BytesIO()

        with gzip.GzipFile(fileobj=buffer, mode="wb") as f:
            f.write(json_bytes)

        buffer.seek(0)
        return buffer

    @staticmethod
    def _gzip_bytes_to_json(data: bytes) -> Dict[str, Any]:
        """Descompacta JSON.gz em dict."""
        with gzip.GzipFile(fileobj=BytesIO(data), mode="rb") as f:
            return json.loads(f.read().decode("utf-8"))

    # ----------------------------------------
    # Upload JSON.gz
    # ----------------------------------------
    def upload_json_gzip(self, data: Dict[str, Any], path: str) -> bool:
        """
        Compacta um dict em JSON.gz e envia para um caminho no bucket.

        Args:
            data (dict): Dados em formato de dicionário.
            path (str): Caminho destino dentro do bucket.

        Returns:
            bool: True se sucesso, False se erro.
        """
        try:
            buffer = self._json_to_gzip_bytes(data)

            blob = self.bucket.blob(path)
            blob.upload_from_file(
                buffer,
                content_type="application/octet-stream",
                client=self.client
            )

            logger.info(f"Upload concluído: gs://{self.bucket_name}/{path}")
            return True

        except Exception as e:
            logger.error(
                f"Falha no upload para GCS (bucket={self.bucket_name}, path={path}): {e}",
                exc_info=True
            )
            return False

    # ----------------------------------------
    # Download JSON.gz
    # ----------------------------------------
    def load_json_gzip(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Lê e descompacta um arquivo JSON.gz do GCS e retorna como dict.

        Args:
            path (str): Caminho do arquivo no bucket.

        Returns:
            dict | None: Dados carregados, ou None em caso de falha.
        """
        try:
            blob = self.bucket.blob(path)

            if not blob.exists():
                logger.warning(f"Arquivo não encontrado: gs://{self.bucket_name}/{path}")
                return None

            data = blob.download_as_bytes()
            return self._gzip_bytes_to_json(data)

        except Exception as e:
            logger.error(
                f"Erro ao carregar JSON.gz do GCS (bucket={self.bucket_name}, path={path}): {e}",
                exc_info=True
            )
            return None
