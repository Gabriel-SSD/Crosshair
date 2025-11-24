import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from discord.tw_summary import load_env_var, df_to_table, main

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
# Teste df_to_table
# -------------------------
def test_df_to_table():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    table_str = df_to_table(df)
    assert "```\n" in table_str
    assert "a" in table_str and "b" in table_str

# -------------------------
# Fixture de ambiente válido
# -------------------------
@pytest.fixture
def mock_env(monkeypatch):
    """Configura variáveis de ambiente válidas para os testes."""
    monkeypatch.setenv("BQ_PROJECT_ID", "proj123")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.fake/webhook")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini_key")

# -------------------------
# Falha ao inicializar Gemini
# -------------------------
def test_gemini_initialization_failure(mock_env):
    with patch("discord.tw_summary.genai.configure", side_effect=Exception("Gemini fail")):
        with pytest.raises(SystemExit):
            main()

# -------------------------
# Falha ao inicializar BigQuery
# -------------------------
def test_bigquery_initialization_failure(mock_env):
    with patch("discord.tw_summary.genai.configure"):
        with patch("discord.tw_summary.genai.GenerativeModel"):
            with patch("discord.tw_summary.bigquery.Client", side_effect=Exception("BQ fail")):
                with pytest.raises(SystemExit):
                    main()

# -------------------------
# Falha na consulta BigQuery
# -------------------------
def test_bigquery_query_failure(mock_env):
    with patch("discord.tw_summary.genai.configure"):
        with patch("discord.tw_summary.genai.GenerativeModel"):
            with patch("discord.tw_summary.bigquery.Client") as MockClient:
                mock_client = MockClient.return_value
                mock_client.query.side_effect = Exception("Query fail")
                with pytest.raises(SystemExit):
                    main()

# -------------------------
# Consulta retorna DataFrame vazio
# -------------------------
def test_empty_bigquery_result(mock_env):
    with patch("discord.tw_summary.genai.configure"):
        with patch("discord.tw_summary.genai.GenerativeModel"):
            with patch("discord.tw_summary.bigquery.Client") as MockClient:
                mock_client = MockClient.return_value
                mock_client.query.return_value.to_dataframe.return_value = pd.DataFrame()
                with pytest.raises(SystemExit):
                    main()

# -------------------------
# Falha na geração de resumo Gemini
# -------------------------
def test_gemini_generate_failure(mock_env):
    with patch("discord.tw_summary.genai.configure"):
        with patch("discord.tw_summary.genai.GenerativeModel") as MockModel:
            mock_model = MockModel.return_value
            mock_model.generate_content.side_effect = Exception("Generate fail")
            with patch("discord.tw_summary.bigquery.Client") as MockClient:
                mock_client = MockClient.return_value
                mock_client.query.return_value.to_dataframe.return_value = pd.DataFrame({
                    "player_name": ["A"], "total_banners": [1],
                    "ofensive_banners": [0], "defensive_banners": [0],
                    "rogue_actions": [0], "tw_date": [pd.Timestamp("2025-11-24")]
                })
                with pytest.raises(SystemExit):
                    main()

# -------------------------
# Falha ao enviar Discord
# -------------------------
def test_discord_post_failure(mock_env):
    with patch("discord.tw_summary.genai.configure"):
        with patch("discord.tw_summary.genai.GenerativeModel") as MockModel:
            mock_model = MockModel.return_value
            mock_model.generate_content.return_value.text = "summary"

            with patch("discord.tw_summary.bigquery.Client") as MockClient:
                mock_client = MockClient.return_value
                mock_client.query.return_value.to_dataframe.return_value = pd.DataFrame({
                    "player_name": ["A"], "total_banners": [1],
                    "ofensive_banners": [0], "defensive_banners": [0],
                    "rogue_actions": [0], "tw_date": [pd.Timestamp("2025-11-24")]
                })

                with patch("discord.tw_summary.requests.post") as mock_post:
                    mock_post.return_value.status_code = 500
                    with pytest.raises(SystemExit):
                        main()

# -------------------------
# Fluxo completo com sucesso
# -------------------------
def test_success_flow(mock_env, caplog):
    caplog.set_level("INFO")
    df_mock = pd.DataFrame({
        "player_name": ["A"], "total_banners": [10],
        "ofensive_banners": [5], "defensive_banners": [3],
        "rogue_actions": [1], "tw_date": [pd.Timestamp("2025-11-24")]
    })

    with patch("discord.tw_summary.genai.configure"):
        with patch("discord.tw_summary.genai.GenerativeModel") as MockModel:
            mock_model = MockModel.return_value
            mock_model.generate_content.return_value.text = "summary"

            with patch("discord.tw_summary.bigquery.Client") as MockClient:
                mock_client = MockClient.return_value
                mock_client.query.return_value.to_dataframe.return_value = df_mock

                with patch("discord.tw_summary.requests.post") as mock_post:
                    mock_post.return_value.status_code = 200
                    main()

    assert any("Execução concluída com sucesso" in msg for msg in caplog.text.split("\n"))
