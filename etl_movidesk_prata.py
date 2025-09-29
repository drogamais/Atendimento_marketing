# ==============================================================================
# --- IMPORTAÇÕES ---
# ==============================================================================

# Bibliotecas de terceiros
import pandas as pd
import numpy as np
import mariadb
import sys
import warnings

# Módulos do nosso projeto
import config  # Importa todas as nossas configurações centralizadas
from utils import enviar_mensagem_telegram  # Importa a função de notificação

#só para parar de ficar dando alerta pra usar o Alchemy
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy.*')

# ==============================================================================
# --- FUNÇÕES DO PROCESSO ETL ---
# ==============================================================================

def extract_data():
    """
    Extrai dados da camada Bronze e da tabela de dimensão de responsáveis.
    Retorna os DataFrames extraídos e os dicionários de mapeamento.
    """
    print("--- INICIANDO ETAPA DE EXTRAÇÃO (EXTRACT) ---")
    conn = None
    try:
        print("Conectando ao banco de dados...")
        conn = mariadb.connect(**config.DB_CONFIG) # Usando a config importada
        
        print(f"Lendo a tabela '{config.BRONZE_TABLE_NAME}'...")
        df_bronze = pd.read_sql(f"SELECT * FROM {config.BRONZE_TABLE_NAME}", conn) # Usando a config importada
        print(f" > Sucesso! {len(df_bronze)} registros lidos da camada Bronze.")

        print(f"Lendo a tabela '{config.DIM_PESSOAS_TABLE_NAME}' para mapeamento...")
        query_responsaveis = f"SELECT id_movidesk, nome_oficial, departamento_nome FROM {config.DIM_PESSOAS_TABLE_NAME}" # Usando a config importada
        df_responsaveis = pd.read_sql(query_responsaveis, conn)
        
        df_responsaveis['id_movidesk'] = pd.to_numeric(df_responsaveis['id_movidesk'], errors='coerce')
        df_responsaveis.dropna(subset=['id_movidesk'], inplace=True)
        
        mapa_responsaveis = pd.Series(df_responsaveis.nome_oficial.values, index=df_responsaveis.id_movidesk).to_dict()
        print(f" > Sucesso! {len(mapa_responsaveis)} responsáveis mapeados.")

        mapa_departamentos = pd.Series(df_responsaveis.departamento_nome.values, index=df_responsaveis.id_movidesk).to_dict()
        print(f" > Sucesso! {len(mapa_departamentos)} departamentos mapeados.")
        
        return df_bronze, mapa_responsaveis, mapa_departamentos

    except mariadb.Error as e:
        erro_msg = f"[FALHA] **FALHA na Extração de Dados!**\n\n- *Erro MariaDB:*\n`{e}`"
        print(f"ERRO: {erro_msg}")
        enviar_mensagem_telegram(erro_msg)
        sys.exit(f"Encerrando o script. Erro de Banco de Dados: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

def transform_data(df_bronze, mapa_responsaveis, mapa_departamentos):
    """
    Aplica as regras de negócio e transformações nos dados extraídos.
    Retorna o DataFrame pronto para ser carregado na camada Silver.
    """
    print("\n--- INICIANDO ETAPA DE TRANSFORMAÇÃO (TRANSFORM) ---")

    # Filtros e limpeza inicial
    df_bronze.dropna(subset=['ownerId'], inplace=True)
    df_bronze['ownerId'] = pd.to_numeric(df_bronze['ownerId'], errors='coerce')
    df_bronze.dropna(subset=['ownerId'], inplace=True)
    df_bronze['ownerId'] = df_bronze['ownerId'].astype(int)

    df_bronze = df_bronze[~df_bronze['ownerName'].str.contains('Luiz Feliphe', na=False, case=False)]
    print(f" > {len(df_bronze)} registros restantes após filtros.")

    mapa_nome_para_id = df_bronze[['ownerName', 'ownerId']].dropna().drop_duplicates('ownerName').set_index('ownerName')['ownerId'].to_dict()

    # Construção do DataFrame Silver
    df_silver = pd.DataFrame()
    df_silver['id_fato_chamado'] = 'movidesk-' + df_bronze['id'].astype(str) + '-' + df_bronze['ownerId'].astype(str)
    df_silver['id_chamado'] = df_bronze['id']
    df_silver['titulo'] = df_bronze['subject']
    df_silver['data_referencia'] = pd.to_datetime(df_bronze['data_referencia_final']).dt.date
    df_silver['assunto_id'] = 0
    df_silver['assunto_nome'] = 'Chamado Movidesk'
    df_silver['situacao'] = df_bronze['status'].map(config.MAPA_SITUACAO_NUMEROS) # Usando a config importada
    df_silver['situacao_nome'] = df_bronze['status'].map(config.MAPA_SITUACAO_TEXTO).fillna(df_bronze['status']) # Usando a config importada
    df_silver['solicitante_id'] = None
    df_silver['solicitante_nome'] = None
    df_silver['departamento_solicitante_nome'] = None
    df_silver['responsavel_id'] = df_bronze['ownerId']
    df_silver['responsavel_nome'] = df_bronze['ownerId'].map(mapa_responsaveis).fillna(df_bronze['ownerName'])
    df_silver['departamento_responsavel_nome'] = df_bronze['ownerId'].map(mapa_departamentos)
    
    df_silver['id_pessoa_apoio'] = df_bronze['adjunto'].map(mapa_nome_para_id)
    df_silver['nome_apoio'] = df_silver['id_pessoa_apoio'].map(mapa_responsaveis)
    df_silver['departamento_apoio_nome'] = df_silver['id_pessoa_apoio'].map(mapa_departamentos)
    df_silver['tipo_origem'] = 'MOVIDESK'

    df_silver = df_silver.replace({np.nan: None, pd.NaT: None})
    print(" > Transformação concluída.")
    return df_silver

def load_data(df_silver):
    """
    Carrega o DataFrame transformado na tabela da camada Silver.
    """
    print("\n--- INICIANDO ETAPA DE CARGA (LOAD) ---")
    conn = None
    try:
        print(f"Conectando ao banco de dados para carregar a tabela '{config.SILVER_TABLE_NAME}'...")
        conn = mariadb.connect(**config.DB_CONFIG) # Usando a config importada
        cur = conn.cursor()

        print(f"Recriando a tabela '{config.SILVER_TABLE_NAME}'...")
        cur.execute(f"DROP TABLE IF EXISTS {config.SILVER_TABLE_NAME}") # Usando a config importada
        create_table_query = f"""
        CREATE TABLE {config.SILVER_TABLE_NAME} (
            id_fato_chamado VARCHAR(255) PRIMARY KEY, id_chamado INT, titulo TEXT,
            data_referencia DATE, assunto_id INT, assunto_nome VARCHAR(255),
            situacao INT, situacao_nome VARCHAR(255), solicitante_id INT,
            solicitante_nome VARCHAR(255), departamento_solicitante_nome VARCHAR(255),
            responsavel_id INT, responsavel_nome VARCHAR(255),
            departamento_responsavel_nome VARCHAR(255), id_pessoa_apoio INT,
            nome_apoio VARCHAR(255), departamento_apoio_nome VARCHAR(255), 
            tipo_origem VARCHAR(100)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci;
        """
        cur.execute(create_table_query)
        print(" > Tabela Silver criada com sucesso.")

        print(f"Inserindo {len(df_silver)} registros na tabela...")
        insert_query = f"INSERT INTO {config.SILVER_TABLE_NAME} VALUES ({', '.join(['?'] * len(df_silver.columns))})"
        data_to_insert = [tuple(row) for row in df_silver.itertuples(index=False)]
        cur.executemany(insert_query, data_to_insert)
        
        conn.commit()
        print(f" > {cur.rowcount} registros inseridos com sucesso!")

    except mariadb.Error as e:
        erro_msg = f"[FALHA] **FALHA ao carregar dados na tabela Silver!**\n\n- *Erro MariaDB:*\n`{e}`"
        print(f"ERRO: {erro_msg}")
        enviar_mensagem_telegram(erro_msg)
        sys.exit(f"Erro de Banco de Dados: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

def main():
    """
    Função principal que orquestra o processo ETL.
    """
    print("=============================================")
    print("INICIANDO PROCESSO ETL: BRONZE -> SILVER")
    print("=============================================")
    
    # 1. Extração
    df_bronze, mapa_resp, mapa_dep = extract_data()

    if df_bronze.empty:
        print("\nNenhum dado encontrado na tabela Bronze para processar. Encerrando.")
        sys.exit()

    # 2. Transformação
    df_silver = transform_data(df_bronze, mapa_resp, mapa_dep)

    # 3. Carga
    load_data(df_silver)
    
    print("\n=============================================")
    print("PROCESSO ETL FINALIZADO COM SUCESSO!")
    print("=============================================")

# --- PONTO DE ENTRADA DO SCRIPT ---
if __name__ == "__main__":
    main()