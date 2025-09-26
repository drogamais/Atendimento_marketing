# ==============================================================================
# --- ARQUIVO DE CONFIGURAÇÃO CENTRAL ---
# Contém todas as senhas, tokens, nomes de tabelas e configurações globais.
# NÃO COLOQUE LÓGICA DE EXECUÇÃO AQUI.
# ==============================================================================

# --- BANCO DE DADOS MARIADB ---
DB_CONFIG = {
    "host": "10.48.12.20",
    "port": 3306,
    "user": "drogamais",
    "password": "dB$MYSql@2119", 
    "database": "dbSults"
}

# --- NOMES DAS TABELAS ---
BRONZE_TABLE_NAME = "bronze_chamados_movidesk"
BRONZE_TABLE_NAME_SULTS = "bronze_chamados_sults"
SILVER_TABLE_NAME = "prata_chamados_movidesk"
DIM_RESPONSAVEIS_TABLE_NAME = "dim_responsaveis"

# --- APIs ---
# MoviDesk
MOVIDESK_API_TOKEN = "5514b190-8715-4587-8ee7-8ad802c86dcc"
BASE_URL_MOVIDESK = "https://api.movidesk.com/public/v1" # Renomeado para clareza

# Sults
SULTS_API_TOKEN = "O2Ryb2dhbWFpczsxNzQ0ODAzNDc1NjIx"
SULTS_BASE_URL = "https://api.sults.com.br/api/v1"

# Telegram
TELEGRAM_BOT_TOKEN = "8096205039:AAGz3TqmfyXGI__NGdyvf6TnMDNA--pvAWc"
TELEGRAM_CHAT_ID = "7035974555"

# --- PARÂMETROS DE EXECUÇÃO ---
TAMANHO_PAGINA_API = 100

# --- MAPEAMENTOS DE REGRAS DE NEGÓCIO ---
MAPA_SITUACAO_NUMEROS = {
    'Novo': 1,
    'Fechado': 2,
    'Resolvido': 3,
    'Em atendimento': 4,
    'Aguardando': 6,
    'Cancelado': 7
}
MAPA_SITUACAO_TEXTO = {
    'Novo': 'NOVO CHAMADO',
    'Fechado': 'CONCLUÍDO',
    'Resolvido': 'RESOLVIDO',
    'Em atendimento': 'EM ANDAMENTO',
    'Aguardando': 'AGUARDANDO RESPONSÁVEL',
    'Cancelado': 'CANCELADO'
}

# --- NOMES DE TABELAS (Adição para Implantação) ---
BRONZE_IMPLANTACOES_TABLE_NAME = "bronze_implantacao_sults"
SILVER_IMPLANTACOES_TABLE_NAME = "prata_implantacao_sults"

# --- MAPEAMENTOS DE REGRAS DE NEGÓCIO (Adição para Implantação) ---
# Mapeia os códigos de situação das tarefas de implantação para texto
MAPA_SITUACAO_IMPLANTACAO = {
    1: 'Concluído',
    2: 'Aberto',
    3: 'Em Andamento',
    4: 'Aguardando',
    5: 'A Definir'
}