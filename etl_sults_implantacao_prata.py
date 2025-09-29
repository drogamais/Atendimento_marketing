# ==============================================================================
# --- IMPORTAÇÕES ---
# ==============================================================================
import pandas as pd
import mariadb
import sys
import warnings

# Módulos do nosso projeto
import config  # Importa o seu arquivo de configurações

# Ignora avisos específicos do Pandas
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy.*')

# ==============================================================================
# --- FUNÇÕES DO PROCESSO ETL ---
# ==============================================================================

def extract_data():
    """
    Extrai dados da camada Bronze (tarefas) e da dimensão de responsáveis.
    Retorna o DataFrame principal e os dicionários para mapeamento.
    """
    print("--- INICIANDO ETAPA DE EXTRAÇÃO (EXTRACT) ---")
    conn = None
    try:
        print("Conectando ao banco de dados...")
        # [MODIFICADO] Usa a configuração do arquivo config.py
        conn = mariadb.connect(**config.DB_CONFIG)
        
        print(f"Lendo a tabela de tarefas: '{config.BRONZE_IMPLANTACOES_TABLE_NAME}'...")
        # [MODIFICADO] Usa o nome da tabela do config.py
        df_bronze = pd.read_sql(f"SELECT * FROM {config.BRONZE_IMPLANTACOES_TABLE_NAME}", conn)
        print(f" > Sucesso! {len(df_bronze)} registros lidos da camada Bronze.")

        print(f"Lendo a tabela de dimensão: '{config.DIM_RESPONSAVEIS_TABLE_NAME}'...")
        # ⚠️ ATENÇÃO: Verifique se 'id_sults' é o nome correto da coluna na sua dim_responsaveis
        query_responsaveis = f"SELECT id_sults, nome_oficial, departamento_nome FROM {config.DIM_RESPONSAVEIS_TABLE_NAME}"
        df_responsaveis = pd.read_sql(query_responsaveis, conn)
        
        df_responsaveis['id_sults'] = pd.to_numeric(df_responsaveis['id_sults'], errors='coerce')
        df_responsaveis.dropna(subset=['id_sults'], inplace=True)
        
        mapa_nomes = pd.Series(df_responsaveis.nome_oficial.values, index=df_responsaveis.id_sults).to_dict()
        print(f" > Sucesso! {len(mapa_nomes)} responsáveis mapeados.")

        mapa_departamentos = pd.Series(df_responsaveis.departamento_nome.values, index=df_responsaveis.id_sults).to_dict()
        print(f" > Sucesso! {len(mapa_departamentos)} departamentos mapeados.")
        
        return df_bronze, mapa_nomes, mapa_departamentos

    except mariadb.Error as e:
        print(f"❌ FALHA na Extração de Dados! Erro MariaDB: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

def transform_data(df_bronze, mapa_nomes, mapa_departamentos):
    """
    Aplica as regras de negócio para transformar os dados de implantação.
    Retorna o DataFrame pronto para a camada Silver.
    """
    print("\n--- INICIANDO ETAPA DE TRANSFORMAÇÃO (TRANSFORM) ---")

    df_silver = pd.DataFrame()

    df_silver['id_fato_implantacao'] = 'implantacao-' + df_bronze['id'].astype(str)
    df_silver['id_implantacao'] = df_bronze['id']
    df_silver['titulo'] = df_bronze['nome']
    df_silver['data_referencia'] = pd.to_datetime(df_bronze['dtConclusao'].fillna(df_bronze['dtCriacao'])).dt.date
    df_silver['assunto_id'] = None
    df_silver['assunto_nome'] = 'Implantação de Loja'
    
    # [MODIFICADO] Usa o mapa de situação do arquivo config.py
    df_silver['situacao_id'] = df_bronze['situacao']
    df_silver['situacao_nome'] = df_bronze['situacao'].map(config.MAPA_SITUACAO_IMPLANTACAO).fillna('Desconhecido')

    df_silver['solicitante_id'] = df_bronze['responsavel_id']
    df_silver['solicitante_nome'] = df_bronze['responsavel_id'].map(mapa_nomes).fillna(df_bronze['responsavel_nome'])
    df_silver['departamento_solicitante_nome'] = df_bronze['responsavel_id'].map(mapa_departamentos)
    
    df_silver['responsavel_id'] = df_bronze['responsavel_id']
    df_silver['responsavel_nome'] = df_bronze['responsavel_id'].map(mapa_nomes).fillna(df_bronze['responsavel_nome'])
    df_silver['departamento_responsavel_nome'] = df_bronze['responsavel_id'].map(mapa_departamentos)
    
    df_silver['id_pessoa_apoio'] = None
    df_silver['nome_apoio'] = None
    df_silver['departamento_apoio_nome'] = None
    df_silver['tipo_origem'] = 'IMPLANTACAO SULTS'

    print(" > Transformação concluída.")
    return df_silver

def load_data(df_silver):
    """
    Carrega o DataFrame transformado na tabela da camada Silver.
    """
    print("\n--- INICIANDO ETAPA DE CARGA (LOAD) ---")
    conn = None
    try:
        print(f"Conectando ao banco de dados para carregar a tabela '{config.SILVER_IMPLANTACOES_TABLE_NAME}'...")
        # [MODIFICADO] Usa a configuração do arquivo config.py
        conn = mariadb.connect(**config.DB_CONFIG)
        cursor = conn.cursor()

        print(f"Recriando a tabela '{config.SILVER_IMPLANTACOES_TABLE_NAME}'...")
        # [MODIFICADO] Usa o nome da tabela do config.py
        cursor.execute(f"DROP TABLE IF EXISTS {config.SILVER_IMPLANTACOES_TABLE_NAME}")
        create_table_query = f"""
        CREATE TABLE {config.SILVER_IMPLANTACOES_TABLE_NAME} (
            id_fato_implantacao         VARCHAR(255) PRIMARY KEY, id_implantacao              DECIMAL(38, 0),
            titulo                      TEXT, data_referencia             DATE,
            assunto_id                  INT NULL, assunto_nome                VARCHAR(255),
            situacao                    INT NULL, situacao_nome               VARCHAR(255),
            solicitante_id              DECIMAL(38, 0) NULL, solicitante_nome            VARCHAR(255) NULL,
            departamento_solicitante_nome VARCHAR(255) NULL, responsavel_id              DECIMAL(38, 0) NULL,
            responsavel_nome            VARCHAR(255) NULL, departamento_responsavel_nome VARCHAR(255) NULL,
            id_pessoa_apoio             DECIMAL(38, 0) NULL, nome_apoio                  VARCHAR(255) NULL,
            departamento_apoio_nome     VARCHAR(255) NULL, tipo_origem                 VARCHAR(100)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
        """
        cursor.execute(create_table_query)
        print(" > Tabela Silver criada com sucesso.")

        print(f"Inserindo {len(df_silver)} registros na tabela...")
        df_para_inserir = df_silver.astype(object).where(pd.notna(df_silver), None)
        dados_para_inserir = [tuple(row) for row in df_para_inserir.itertuples(index=False)]
        
        insert_query = f"INSERT INTO {config.SILVER_IMPLANTACOES_TABLE_NAME} VALUES ({', '.join(['?'] * len(df_silver.columns))})"
        cursor.executemany(insert_query, dados_para_inserir)
        
        conn.commit()
        print(f" > {cursor.rowcount} registros inseridos com sucesso!")

    except mariadb.Error as e:
        print(f"❌ FALHA ao carregar dados na tabela Silver! Erro MariaDB: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

def main():
    """
    Função principal que orquestra o processo ETL.
    """
    print("="*55)
    print("INICIANDO PROCESSO ETL: Bronze -> Silver (Implantação Sults)")
    print("="*55)
    
    df_bronze, mapa_nomes, mapa_departamentos = extract_data()

    if df_bronze.empty:
        print("\nNenhum dado encontrado na tabela Bronze para processar. Encerrando.")
        return

    df_silver = transform_data(df_bronze, mapa_nomes, mapa_departamentos)
    load_data(df_silver)
    
    print("\n" + "="*55)
    print("PROCESSO ETL FINALIZADO COM SUCESSO!")
    print("="*55)

# --- PONTO DE ENTRADA DO SCRIPT ---
if __name__ == "__main__":
    main()