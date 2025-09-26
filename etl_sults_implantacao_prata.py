import pandas as pd
import mariadb
import sys

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
# 🚨 PREENCHA COM AS SUAS INFORMAÇÕES (devem ser as mesmas do outro script) 🚨
CONFIGURACOES_BANCO = {
    "user": "drogamais",
    "password": "dB$MYSql@2119",
    "host": "10.48.12.20",
    "port": 3306,
    "database": "dbSults"
}

# --- Nomes das Tabelas ---
TABELA_BRONZE = "bronze_implantacao_sults" # Tabela de onde vamos LER
TABELA_PRATA = "prata_implantacao_sults"   # Tabela onde vamos ESCREVER

def transformar_bronze_para_prata():
    """
    Conecta no banco, lê da camada Bronze, transforma os dados
    e salva na camada Prata.
    """
    conn = None
    try:
        # --- 1. EXTRAÇÃO (Ler dados da camada Bronze) ---
        print(f"Conectando ao banco de dados para ler da tabela '{TABELA_BRONZE}'...")
        conn = mariadb.connect(**CONFIGURACOES_BANCO)
        
        # Carrega todos os dados da tabela bronze para um DataFrame do Pandas
        df_bronze = pd.read_sql(f"SELECT * FROM {TABELA_BRONZE}", conn)
        
        if df_bronze.empty:
            print("A tabela bronze está vazia. Nenhum dado para transformar.")
            return

        print(f"{len(df_bronze)} registros lidos da camada bronze.")

        # --- 2. TRANSFORMAÇÃO (Mapear e criar as colunas da camada Prata) ---
        print("Iniciando a transformação dos dados para o modelo Prata...")
        
        df_prata = pd.DataFrame()

        # Mapeamento de-para entre as colunas Bronze -> Prata
        df_prata['id_fato_implantacao'] = 'implantacao-' + df_bronze['id'].astype(str)
        df_prata['id_implantacao'] = df_bronze['id']
        df_prata['titulo'] = df_bronze['nome']  # Usando o nome da tarefa como título
        df_prata['data_referencia'] = df_bronze['dtCriacao']
        
        # Colunas com valores fixos ou nulos, pois não existem na origem de implantação
        df_prata['assunto_id'] = None
        df_prata['assunto_nome'] = 'Implantação de Loja' # Valor fixo para identificar a origem
        
        # Mapeamento de situação (Exemplo, ajuste conforme seus dados)
        # 1– Concluído, 2- Aberto, 3- Em Andamento, 4- Aguardando Predecessora, 5- Definir
        situacao_map = {1: 'Concluído', 2: 'Aberto', 3: 'Em Andamento', 4: 'Aguardando', 5: 'A Definir'}
        df_prata['situacao_id'] = df_bronze['situacao']
        df_prata['situacao_nome'] = df_bronze['situacao'].map(situacao_map).fillna('Desconhecido')

        # Mapeamento de pessoas (solicitante não existe na tarefa, usaremos o responsável)
        df_prata['solicitante_id'] = df_bronze['responsavel_id'] 
        df_prata['solicitante_nome'] = df_bronze['responsavel_nome']
        df_prata['departamento_solicitante_nome'] = None # Não temos essa informação
        
        df_prata['responsavel_id'] = df_bronze['responsavel_id']
        df_prata['responsavel_nome'] = df_bronze['responsavel_nome']
        df_prata['departamento_responsavel_nome'] = None # Não temos essa informação
        
        # Colunas de apoio (não existem na origem)
        df_prata['id_pessoa_apoio'] = None
        df_prata['nome_apoio'] = None
        df_prata['departamento_apoio_nome'] = None
        
        # Coluna de origem (muito importante para a VIEW)
        df_prata['tipo_origem'] = 'Implantacao'

        print("Transformação concluída.")

        # --- 3. CARGA (Salvar os dados transformados na camada Prata) ---
        cursor = conn.cursor()
        
        print(f"Apagando a tabela antiga '{TABELA_PRATA}' (se existir)...")
        cursor.execute(f"DROP TABLE IF EXISTS `{TABELA_PRATA}`")
        
        print(f"Criando a nova estrutura da tabela '{TABELA_PRATA}'...")
        sql_create_table = f"""
        CREATE TABLE `{TABELA_PRATA}` (
            `id_fato_implantacao`         VARCHAR(255) PRIMARY KEY,
            `id_implantacao`              DECIMAL(38, 0),
            `titulo`                      TEXT,
            `data_referencia`             DATETIME,
            `assunto_id`                  INT NULL,
            `assunto_nome`                VARCHAR(255),
            `situacao_id`                 INT NULL,
            `situacao_nome`               VARCHAR(255),
            `solicitante_id`              DECIMAL(38, 0) NULL,
            `solicitante_nome`            VARCHAR(255) NULL,
            `departamento_solicitante_nome` VARCHAR(255) NULL,
            `responsavel_id`              DECIMAL(38, 0) NULL,
            `responsavel_nome`            VARCHAR(255) NULL,
            `departamento_responsavel_nome` VARCHAR(255) NULL,
            `id_pessoa_apoio`             DECIMAL(38, 0) NULL,
            `nome_apoio`                  VARCHAR(255) NULL,
            `departamento_apoio_nome`     VARCHAR(255) NULL,
            `tipo_origem`                 VARCHAR(100)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        cursor.execute(sql_create_table)
        
        # Preparar e inserir os dados
        df_para_inserir = df_prata.astype(object).where(pd.notna(df_prata), None)
        dados_para_inserir = [tuple(x) for x in df_para_inserir.to_numpy()]
        
        sql_insert = f"INSERT INTO `{TABELA_PRATA}` ({', '.join([f'`{c}`' for c in df_prata.columns])}) VALUES ({', '.join(['?' for _ in df_prata.columns])})"
        
        print(f"Inserindo {len(dados_para_inserir)} registros na tabela '{TABELA_PRATA}'...")
        cursor.executemany(sql_insert, dados_para_inserir)
        
        conn.commit()
        print(f"✅ Sucesso! Tabela '{TABELA_PRATA}' criada e populada.")

    except mariadb.Error as e:
        print(f"❌ Erro ao interagir com o MariaDB: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    transformar_bronze_para_prata()