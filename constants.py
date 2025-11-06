# --- NOMES DAS TABELAS ---
# BRONZE_TABLE_NAME = "bronze_movidesk_chamados"
BRONZE_TABLE_NAME_SULTS = "bronze_sults_chamados"
# SILVER_TABLE_NAME = "silver_movidesk_chamados"
DIM_PESSOAS_TABLE_NAME = "dim_pessoas"

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
    'Aguardando': 'AGUARDANDO',
    'Cancelado': 'CANCELADO'
}

# --- NOMES DE TABELAS (Adição para Implantação) ---
BRONZE_IMPLANTACOES_TABLE_NAME = "bronze_sults_implantacao"
SILVER_IMPLANTACOES_TABLE_NAME = "silver_sults_implantacao"

MAPA_SITUACAO_IMPLANTACAO_ID = {
    1: 2,  # Concluído -> 1 (CONCLUÍDO)
    2: 8,  # Aberto -> 8 (ABERTO)
    3: 4,  # Em Andamento -> 4 (EM ATENDIMENTO)
    4: 9,  # Aguardando -> 6 (AGUARDANDO)
    5: 10   # A Definir -> 9 (A DEFINIR)
}

# Mapeia os IDs originais da API para os Nomes padronizados da camada Prata
MAPA_SITUACAO_IMPLANTACAO_NOME = {
    1: 'CONCLUÍDO',
    2: 'ABERTO',
    3: 'EM ATENDIMENTO',
    4: 'AGUARDANDO',
    5: 'A DEFINIR'
}