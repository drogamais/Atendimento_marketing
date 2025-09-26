# ==============================================================================
# --- IMPORTAÇÕES ---
# ==============================================================================

# Bibliotecas de terceiros
import requests
import pandas as pd
import time
import re
import numpy as np
import mariadb
import sys

# Módulos do nosso projeto
import config  # Importa todas as nossas configurações centralizadas
from utils import enviar_mensagem_telegram  # Importa a função de notificação

# ==============================================================================
# --- FUNÇÕES AUXILIARES (Específicas deste ETL) ---
# ==============================================================================

def processar_clientes(row):
    """Processa a string de clientes para extrair adjunto, número e nome da loja."""
    client_string = row['clientsName']
    adjunto = ''
    string_para_processar_loja = client_string

    if not isinstance(client_string, str) or not client_string:
        return adjunto, pd.NA, client_string

    if ',' in client_string:
        partes = client_string.split(',', 1)
        if 'drogamais' in partes[0].lower() or re.search(r'^\s*\d+\s*-', partes[0]):
            string_para_processar_loja = partes[0].strip()
            adjunto = partes[1].strip()
        else:
            string_para_processar_loja = partes[1].strip()
            adjunto = partes[0].strip()

    loja_numero, nome_loja_final = pd.NA, string_para_processar_loja

    string_limpa = string_para_processar_loja
    if string_para_processar_loja.lower().startswith('drogamais'):
        string_limpa = string_para_processar_loja[len('drogamais'):].strip()

    match = re.search(r'(\d+)\s*-\s*(.*)', string_limpa)
    if match:
        loja_numero = int(match.group(1))
        nome_loja_final = f"Drogamais {match.group(2).strip()}"

    return adjunto, loja_numero, nome_loja_final


# ==============================================================================
# --- 3. FUNÇÕES DO PROCESSO ETL ---
# ==============================================================================

def extract_movidesk_data():
    """
    Extrai todos os tickets da API do MoviDesk usando paginação.
    Retorna uma lista de dicionários, cada um representando um ticket.
    """
    print("--- INICIANDO ETAPA 1: EXTRAÇÃO (EXTRACT) ---")
    todos_os_tickets = []
    pagina_atual = 1
    params = {
        'token': config.MOVIDESK_API_TOKEN,  # Usando a config importada
        '$select': 'id,subject,status,owner,clients,createdDate,resolvedIn',
        '$expand': 'owner($select=id,businessName),clients($select=businessName)',
        '$top': config.TAMANHO_PAGINA_API,  # Usando a config importada
        '$skip': 0
    }

    while True:
        try:
            print(f"Buscando página {pagina_atual}...")
            response = requests.get(f"{config.BASE_URL_MOVIDESK}/tickets", params=params) # Usando a config importada
            response.raise_for_status()
            tickets_da_pagina = response.json()

            if not tickets_da_pagina:
                print("\nBusca finalizada. Não há mais tickets para buscar.")
                break

            todos_os_tickets.extend(tickets_da_pagina)
            print(f"  > Sucesso! Total acumulado: {len(todos_os_tickets)}")

            params['$skip'] += config.TAMANHO_PAGINA_API # Usando a config importada
            pagina_atual += 1
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            erro_msg = f"❌ *FALHA na extração do MoviDesk!*\n\n- *Erro de Rede:*\n`{e}`"
            print(f"\nERRO: {erro_msg}")
            enviar_mensagem_telegram(erro_msg)
            return None

    print("-" * 50)
    return todos_os_tickets

def transform_data(raw_tickets):
    """
    Recebe a lista de tickets brutos e aplica as transformações necessárias.
    Retorna um DataFrame Pandas limpo e pronto para ser carregado.
    """
    print("--- INICIANDO ETAPA 2: TRANSFORMAÇÃO (TRANSFORM) ---")
    print(f"Total de {len(raw_tickets)} tickets para transformar...")
    df = pd.DataFrame(raw_tickets)

    # 1. Limpeza e Extração Inicial
    df['ownerId'] = df['owner'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
    df['ownerName'] = df['owner'].apply(lambda x: x.get('businessName', '').strip() if isinstance(x, dict) else '')
    df['clientsName'] = df['clients'].apply(lambda l: ', '.join([c['businessName'] for c in l if c.get('businessName')]) if isinstance(l, list) and l else '')

    # 2. Conversão e Tratamento de Datas
    df['resolvedIn'] = pd.to_datetime(df['resolvedIn'], errors='coerce').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    df['createdDate'] = pd.to_datetime(df['createdDate'], errors='coerce').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    df['data_referencia_final'] = df['resolvedIn'].fillna(df['createdDate'])

    # 3. Extração de detalhes do cliente
    df[['adjunto', 'loja_numero', 'clientsName_final']] = df.apply(processar_clientes, axis=1, result_type='expand')
    df['tipo_origem'] = 'MOVIDESK'

    # 4. Seleção e preparação final das colunas
    colunas_finais = [
        'id', 'subject', 'status', 'createdDate', 'resolvedIn', 'data_referencia_final',
        'ownerId', 'ownerName', 'clientsName_final', 'loja_numero', 'adjunto',
        'clientsName', 'tipo_origem'
    ]
    df_para_exportar = df[colunas_finais].copy()

    for col in ['createdDate', 'resolvedIn', 'data_referencia_final']:
        if pd.api.types.is_datetime64_any_dtype(df_para_exportar[col]):
            df_para_exportar[col] = df_para_exportar[col].dt.tz_localize(None)

    df_para_exportar = df_para_exportar.replace({np.nan: None, pd.NaT: None})

    print(" > Transformação concluída.")
    print("-" * 50)
    return df_para_exportar

def load_data_to_mariadb(dataframe):
    """
    Carrega o DataFrame fornecido para a tabela de destino no MariaDB.
    A tabela é recriada a cada execução.
    """
    print("--- INICIANDO ETAPA 3: CARGA (LOAD) ---")
    conn = None
    try:
        print(f"Conectando ao banco de dados '{config.DB_CONFIG['database']}'...")
        conn = mariadb.connect(**config.DB_CONFIG) # Usando a config importada
        cur = conn.cursor()
        print(" > Conexão bem-sucedida!")

        print(f"Recriando a tabela '{config.BRONZE_TABLE_NAME}'...")
        cur.execute(f"DROP TABLE IF EXISTS {config.BRONZE_TABLE_NAME}") # Usando a config importada
        create_table_query = f"""
        CREATE TABLE {config.BRONZE_TABLE_NAME} (
            id INT PRIMARY KEY, subject TEXT, status VARCHAR(100),
            createdDate DATETIME, resolvedIn DATETIME, data_referencia_final DATETIME,
            ownerId VARCHAR(255), ownerName VARCHAR(255), clientsName_final VARCHAR(255),
            loja_numero INT, adjunto VARCHAR(255), clientsName TEXT, tipo_origem VARCHAR(50)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci;
        """
        cur.execute(create_table_query)
        print(" > Tabela criada com sucesso.")

        print(f"Inserindo {len(dataframe)} registros na tabela...")
        insert_query = f"""
        INSERT INTO {config.BRONZE_TABLE_NAME} (
            id, subject, status, createdDate, resolvedIn, data_referencia_final,
            ownerId, ownerName, clientsName_final, loja_numero, adjunto, clientsName, tipo_origem
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        data_to_insert = [tuple(row) for row in dataframe.itertuples(index=False)]
        cur.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f" > {cur.rowcount} registros inseridos com sucesso!")

        sucesso_msg = (
            f"✅ *ETL MoviDesk Concluído!* \n\n"
            f"- A tabela `{config.BRONZE_TABLE_NAME}` foi atualizada com sucesso.\n"
            f"- Total de registros processados: {len(dataframe)}"
        )
        enviar_mensagem_telegram(sucesso_msg)

    except mariadb.Error as e:
        erro_msg = f"❌ **FALHA ao carregar dados no Banco de Dados!**\n\n- *Erro MariaDB:*\n`{e}`"
        print(f"\nERRO: {erro_msg}")
        enviar_mensagem_telegram(erro_msg)
        sys.exit("Script finalizado devido a erro no banco de dados.")
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

# ==============================================================================
# --- 4. FUNÇÃO PRINCIPAL (ORQUESTRADOR) ---
# ==============================================================================

def main():
    """
    Função principal que orquestra a execução do processo ETL.
    """
    print("=============================================")
    print("INICIANDO PROCESSO ETL: MOVIDESK -> BRONZE")
    print("=============================================")

    # 1. Extração
    raw_tickets_data = extract_movidesk_data()

    # 2. Validação e Transformação
    if not raw_tickets_data:
        print("Nenhum ticket foi encontrado ou ocorreu um erro na extração. Encerrando.")
        enviar_mensagem_telegram("ℹ️ *ETL Movidesk Concluído*\n\nNenhum ticket novo foi encontrado na extração.")
        return

    transformed_dataframe = transform_data(raw_tickets_data)

    # 3. Carga
    load_data_to_mariadb(transformed_dataframe)

    print("\n=============================================")
    print("PROCESSO ETL FINALIZADO COM SUCESSO!")
    print("=============================================")

# ==============================================================================
# --- 5. PONTO DE ENTRADA DO SCRIPT ---
# ==============================================================================

if __name__ == "__main__":
    main()