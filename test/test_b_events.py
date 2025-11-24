import pytest
from unittest.mock import patch, MagicMock
from bronze.events import load_env_var, main


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
# Cenário: falha ao inicializar API
# -------------------------
def test_api_initialization_failure(mock_env):
    with patch("bronze.events.API", side_effect=Exception("API fail")):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Cenário: fetch_data lança erro
# -------------------------
def test_fetch_data_failure(mock_env):
    with patch("bronze.events.API") as MockAPI:
        mock_api = MockAPI.return_value
        mock_api.fetch_data.side_effect = Exception("Fetch error")
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Cenário: retorno não-JSON
# -------------------------
def test_api_returns_invalid_json(mock_env):
    with patch("bronze.events.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = "not a json"
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Cenário: falta campo 'code'
# -------------------------
def test_api_missing_code_field(mock_env):
    with patch("bronze.events.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {}
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Cenário: API retornou erro (code != 0)
# -------------------------
def test_api_error_code(mock_env):
    with patch("bronze.events.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {
            "code": 500,
            "message": "Internal error",
        }
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao inicializar GCS
# -------------------------
def test_gcs_initialization_failure(mock_env):
    with patch("bronze.events.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {"code": 0}
    with patch("bronze.events.utils.GCSClient", side_effect=Exception("GCS fail")):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha no upload para o GCS
# -------------------------
def test_gcs_upload_failure(mock_env):
    with patch("bronze.events.API") as MockAPI:
        MockAPI.return_value.fetch_data.return_value = {"code": 0}
    mock_gcs = MagicMock()
    mock_gcs.upload_json_gzip.return_value = False
    with patch("bronze.events.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Fluxo completo com sucesso
# -------------------------
def test_success_flow(mock_env, caplog):
    caplog.set_level("INFO")

    mock_fetch = MagicMock(return_value={"code": 0})
    mock_api = MagicMock()
    mock_api.fetch_data = mock_fetch

    mock_gcs = MagicMock()
    mock_gcs.upload_json_gzip.return_value = True

    with patch("bronze.events.API", return_value=mock_api):
        with patch("bronze.events.utils.GCSClient", return_value=mock_gcs):
            main()

    assert any("Execução concluída com sucesso" in msg for msg in caplog.text.split("\n"))
