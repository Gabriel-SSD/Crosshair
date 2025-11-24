import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from silver.guild_member import load_env_var, main
from datetime import datetime, timezone

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
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket")
    monkeypatch.setenv("GUILD_ID", "guild123")
    monkeypatch.setenv("BQ_PROJECT_ID", "proj123")

# -------------------------
# Falha ao inicializar GCS
# -------------------------
def test_gcs_initialization_failure(mock_env):
    with patch("silver.guild_member.utils.GCSClient", side_effect=Exception("GCS fail")):
        with pytest.raises(SystemExit):
            main()

# -------------------------
# Falha ao carregar arquivo do GCS
# -------------------------
def test_gcs_load_failure(mock_env):
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = None
    with patch("silver.guild_member.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()

# -------------------------
# Falha ao processar contribuições
# -------------------------
def test_contribution_processing_failure(mock_env):
    guild_data = {
        "member": [{"playerId": "p1", "memberContribution": [{}]}]  # dados incompletos para forçar erro
    }
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = guild_data
    with patch("silver.guild_member.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()

# -------------------------
# Falha ao processar membros da guild
# -------------------------
def test_guild_members_processing_failure(mock_env):
    guild_data = {
        "member": [{"playerId": None, "playerName": "A", "guildJoinTime": "invalid", "memberLevel": "GUILD_LEADER"}]
    }
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = guild_data
    with patch("silver.guild_member.utils.GCSClient", return_value=mock_gcs):
        with pytest.raises(SystemExit):
            main()

# -------------------------
# Falha ao inicializar BigQuery
# -------------------------
def test_bigquery_initialization_failure(mock_env):
    guild_data = {"member": []}
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = guild_data
    with patch("silver.guild_member.utils.GCSClient", return_value=mock_gcs):
        with patch("silver.guild_member.bigquery.Client", side_effect=Exception("BQ fail")):
            with pytest.raises(SystemExit):
                main()

# -------------------------
# Falha ao gravar guild_members
# -------------------------
def test_bigquery_load_members_failure(mock_env):
    guild_data = {
        "member": [{"playerId": "p1", "playerName": "A", "guildJoinTime": "1690000000", "memberLevel": "GUILD_MEMBER"}]
    }
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = guild_data

    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.side_effect = [Exception("members fail"), MagicMock()]

    with patch("silver.guild_member.utils.GCSClient", return_value=mock_gcs):
        with patch("silver.guild_member.bigquery.Client", return_value=mock_client):
            with pytest.raises(SystemExit):
                main()

# -------------------------
# Falha ao gravar guild_contributions
# -------------------------
def test_bigquery_load_contributions_failure(mock_env):
    guild_data = {
        "member": [
            {"playerId": "p1", "playerName": "A", "guildJoinTime": "1690000000", "memberLevel": "GUILD_MEMBER",
             "memberContribution": [{"type": "CONTRIBUTION_TYPE_TRIBUTE", "amount": 10}]}
        ]
    }
    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = guild_data

    mock_job = MagicMock()
    mock_job.result.return_value = None

    mock_client = MagicMock()
    # Primeiro WRITE_TRUNCATE passa, WRITE_APPEND falha
    mock_client.load_table_from_dataframe.side_effect = [mock_job, Exception("contrib fail")]

    with patch("silver.guild_member.utils.GCSClient", return_value=mock_gcs):
        with patch("silver.guild_member.bigquery.Client", return_value=mock_client):
            with pytest.raises(SystemExit):
                main()

# -------------------------
# Fluxo completo com sucesso
# -------------------------
def test_success_flow(mock_env, caplog):
    caplog.set_level("INFO")
    guild_data = {
        "member": [
            {"playerId": "p1", "playerName": "A", "guildJoinTime": "1690000000", "memberLevel": "GUILD_MEMBER",
             "memberContribution": [{"type": "CONTRIBUTION_TYPE_TRIBUTE", "amount": 10}]}
        ]
    }

    mock_gcs = MagicMock()
    mock_gcs.load_json_gzip.return_value = guild_data

    mock_job = MagicMock()
    mock_job.result.return_value = None

    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.return_value = mock_job

    with patch("silver.guild_member.utils.GCSClient", return_value=mock_gcs):
        with patch("silver.guild_member.bigquery.Client", return_value=mock_client):
            main()

    assert any("Execução concluída com sucesso" in msg for msg in caplog.text.split("\n"))
