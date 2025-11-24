import pytest
from unittest.mock import patch, MagicMock
from bronze.guild_member import load_env_var, main

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
    monkeypatch.setenv("GUILD_ID", "guild123")
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket123")


# -------------------------
# Falha ao inicializar GCS
# -------------------------


def test_gcs_initialization_failure(mock_env):
    with patch(
        "bronze.guild_member.utils.GCSClient",
        side_effect=Exception("GCS fail"),
    ):
        with pytest.raises(SystemExit):
            main()


# -------------------------
# Falha ao inicializar SwgohComlink
# -------------------------


def test_comlink_initialization_failure(mock_env):
    with patch("bronze.guild_member.utils.GCSClient") as MockGCS:
        MockGCS.return_value = MagicMock()
        with patch(
            "bronze.guild_member.SwgohComlink",
            side_effect=Exception("Comlink fail"),
        ):
            with pytest.raises(SystemExit):
                main()


# -------------------------
# Falha ao buscar dados da guild
# -------------------------


def test_guild_fetch_failure(mock_env):
    with patch("bronze.guild_member.utils.GCSClient") as MockGCS:
        MockGCS.return_value = MagicMock()
        with patch("bronze.guild_member.SwgohComlink") as MockComlink:
            mock_comlink = MockComlink.return_value
            mock_comlink.get_guild.side_effect = Exception("Guild fetch fail")
            # Não deve levantar SystemExit, pois o código trata falha da guild
            main()


# -------------------------
# Falha ao buscar dados de um player
# -------------------------


def test_player_fetch_failure(mock_env):
    mock_guild = {"member": [{"playerId": "p1"}]}

    with patch("bronze.guild_member.utils.GCSClient") as MockGCS:
        mock_gcs = MockGCS.return_value
        mock_gcs.upload_json_gzip.return_value = True

        with patch("bronze.guild_member.SwgohComlink") as MockComlink:
            mock_comlink = MockComlink.return_value
            mock_comlink.get_guild.return_value = mock_guild
            mock_comlink.get_player.side_effect = Exception("Player fetch fail")

            # Execução completa; erros de player não abortam o script
            main()


# -------------------------
# Falha no upload da guild
# -------------------------


def test_guild_upload_failure(mock_env):
    mock_guild = {"member": []}

    with patch("bronze.guild_member.utils.GCSClient") as MockGCS:
        mock_gcs = MockGCS.return_value
        mock_gcs.upload_json_gzip.side_effect = [False]  # falha no upload da guild

        with patch("bronze.guild_member.SwgohComlink") as MockComlink:
            mock_comlink = MockComlink.return_value
            mock_comlink.get_guild.return_value = mock_guild

            main()  # Não levanta SystemExit; apenas loga erro


# -------------------------
# Fluxo completo com sucesso
# -------------------------


def test_success_flow(mock_env, caplog):
    caplog.set_level("INFO")
    mock_guild = {"member": [{"playerId": "p1"}, {"playerId": "p2"}]}

    with patch("bronze.guild_member.utils.GCSClient") as MockGCS:
        mock_gcs = MockGCS.return_value
        mock_gcs.upload_json_gzip.return_value = True

        with patch("bronze.guild_member.SwgohComlink") as MockComlink:
            mock_comlink = MockComlink.return_value
            mock_comlink.get_guild.return_value = mock_guild
            mock_comlink.get_player.side_effect = lambda player_id: {"playerId": player_id}

            main()

    # Verifica logs de sucesso
    assert any("Execução concluída com sucesso" in msg for msg in caplog.text.split("\n"))
    assert any("Cliente GCS inicializado" in msg for msg in caplog.text.split("\n"))
    assert any("Cliente SwgohComlink inicializado" in msg for msg in caplog.text.split("\n"))
