import pytest
from unittest.mock import patch, MagicMock
from bronze.tw_leaderboard import load_env_var, main

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
    """Configura variáveis de ambiente válidas para os testes."""
    monkeypatch.setenv("MHANN_APIKEY", "key")
    monkeypatch.setenv("ALLYCODE", "123")
    monkeypatch.setenv("GUILD_ID", "456")
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket")


# -------------------------
# Falha ao inicializar API
# -------------------------


def test_api_initialization_failure(mock_env):
    with patch("bronze.tw_leaderboard.API", side_effect=Exception("API fail")):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# fetch_data lança erro
# -------------------------


def test_fetch_data_failure(mock_env):
    with patch("bronze.tw_leaderboard.API") as MockAPI:
        mock_api = MockAPI.return_value
        mock_api.fetch_data.side_effect = Exception("Fetch error")
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Retorno não-JSON
# -------------------------


def test_api_returns_invalid_json(mock_env):
    with patch("bronze.tw_leaderboard.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = "not a json"
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falta campo 'territoryMapId'
# -------------------------


def test_api_missing_territoryMapId(mock_env):
    with patch("bronze.tw_leaderboard.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {}
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Formato inválido de territoryMapId
# -------------------------


def test_invalid_territoryMapId_format(mock_env):
    with patch("bronze.tw_leaderboard.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {"territoryMapId": "INVALID"}
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao inicializar GCS
# -------------------------


def test_gcs_initialization_failure(mock_env):
    with patch("bronze.tw_leaderboard.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {"territoryMapId": "O1234567890"}
    with patch(
        "bronze.tw_leaderboard.utils.GCSClient",
        side_effect=Exception("GCS fail"),
    ):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha no upload para o GCS
# -------------------------


def test_gcs_upload_failure(mock_env):
    with patch("bronze.tw_leaderboard.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {"territoryMapId": "O1234567890"}
    mock_gcs = MagicMock()
    mock_gcs.upload_json_gzip.return_value = False
    with patch("bronze.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Fluxo completo com sucesso
# -------------------------


def test_success_flow(mock_env, caplog):
    caplog.set_level("INFO")

    mock_api = MagicMock()
    mock_api.fetch_data.return_value = {"territoryMapId": "O1699999999000"}  # timestamp qualquer

    mock_gcs = MagicMock()
    mock_gcs.upload_json_gzip.return_value = True

    with patch("bronze.tw_leaderboard.API", return_value=mock_api):
        with patch("bronze.tw_leaderboard.utils.GCSClient", return_value=mock_gcs):
            main()

    assert any("Execução concluída com sucesso" in msg for msg in caplog.text.split("\n"))
    assert any("TW detectado na data" in msg for msg in caplog.text.split("\n"))
