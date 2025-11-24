import pytest
from unittest.mock import patch, MagicMock
from silver.tw_leaderboard import load_env_var, main

# -------------------------
# Testes de variáveis .env
# -------------------------


def test_load_env_var_success(monkeypatch):
    monkeypatch.setenv("MY_VAR", "123")
    assert load_env_var("MY_VAR") == "123"


def test_load_env_var_missing(monkeypatch):
    monkeypatch.delenv("MY_VAR", raising=False)
    with pytest.raises(ValueError):
        load_env_var("MY_VAR")


# -------------------------
# Fixture de ambiente válido
# -------------------------


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket")
    monkeypatch.setenv("GUILD_ID", "guild123")
    monkeypatch.setenv("BQ_PROJECT_ID", "proj123")


# -------------------------
# Falha ao inicializar GCS
# -------------------------


def test_gcs_initialization_failure(mock_env):
    with patch(
        "silver.tw_leaderboard.utils.GCSClient",
        side_effect=Exception("GCS fail"),
    ):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao carregar arquivo do GCS
# -------------------------


def test_gcs_load_failure(mock_env):
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = None
    with patch("silver.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao acessar campo 'data' no JSON
# -------------------------


def test_missing_data_field(mock_env):
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = {"territoryMapId": "O1690000000000"}
    with patch("silver.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao criar DataFrames
# -------------------------


def test_dataframe_creation_failure(mock_env):
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = {
        "territoryMapId": "O1690000000000",
        "data": None,
    }
    with patch("silver.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao extrair TW timestamp
# -------------------------


def test_tw_timestamp_failure(mock_env):
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = {
        "territoryMapId": "INVALID",
        "data": {},
    }
    with patch("silver.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao inicializar BigQuery
# -------------------------


def test_bigquery_initialization_failure(mock_env):
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = {
        "territoryMapId": "O1690000000000",
        "data": {"totalBanners": []},
    }
    with patch("silver.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with patch(
            "silver.tw_leaderboard.bigquery.Client",
            side_effect=Exception("BQ fail"),
        ):
            with pytest.raises(SystemExit):
                main()


# -------------------------
# Falha ao gravar no BigQuery
# -------------------------


def test_bigquery_load_failure(mock_env):
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = {
        "territoryMapId": "O1690000000000",
        "data": {"totalBanners": []},
    }

    mock_job = MagicMock()
    mock_job.result.side_effect = None

    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.side_effect = Exception("Load fail")

    with patch("silver.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with patch("silver.tw_leaderboard.bigquery.Client", return_value=mock_client):
            with pytest.raises(SystemExit):
                main()


# -------------------------
# Fluxo completo com sucesso
# -------------------------


def test_success_flow(mock_env, caplog):
    caplog.set_level("INFO")
    data = {
        "totalBanners": [["p1", 5]],
        "attackBanners": [["p1", 3]],
        "defenseBanners": [["p1", 2]],
        "rogueActions": [["p1", 1]],
    }
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = {
        "territoryMapId": "O1690000000000",
        "data": data,
    }

    mock_job = MagicMock()
    mock_job.result.return_value = None

    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.return_value = mock_job

    with patch("silver.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with patch("silver.tw_leaderboard.bigquery.Client", return_value=mock_client):
            main()

    assert any("Execução concluída com sucesso" in msg for msg in caplog.text.split("\n"))
