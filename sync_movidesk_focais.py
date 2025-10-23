import requests
import json
import pandas as pd
import time
import re
import numpy as np
from datetime import datetime
import hashlib                  # <-- NOVA IMPORTAÇÃO
import mysql.connector          # <-- NOVA IMPORTAÇÃO

# --- CONFIGURAÇÕES API---
MOVIDESK_API_TOKEN = "5514b190-8715-4587-8ee7-8ad802c86dcc"
BASE_URL = "https://api.movidesk.com/public/v1"
TAMANHO_PAGINA = 100

# --- CONFIGURAÇÕES BANCO DE DADOS (Estilo Dicionário) ---
DB_CONFIG = {
    "user": "drogamais",
    "password": "dB$MYSql@2119",
    "host": "10.48.12.20",
    "port": 3306,
    "database": "dbSults",
    "collation": "utf8mb4_general_ci"
}
TABLE_NAME = 'tb_atendimentos'
# ---------------------------------------------------------

# --- PARÂMETROS DA REQUISIÇÃO (sem alterações) ---
params = {
    'token': MOVIDESK_API_TOKEN,
    '$select': 'id,subject,status,owner,clients,createdDate,resolvedIn',
    '$orderby': 'resolvedIn desc',
    '$filter': f"(status eq 'Fechado' or status eq 'Resolvido') and resolvedIn ge 2025-07-01T00:00:00Z",
    '$expand': 'owner($select=id,businessName),clients($select=businessName)',
    '$top': TAMANHO_PAGINA,
    '$skip': 0
}

# --- CONFIGURAÇÕES TELEGRAM (sem alterações) ---
TELEGRAM_BOT_TOKEN = "8096205039:AAGz3TqmfyXGI__NGdyvf6TnMDNA--pvAWc"
TELEGRAM_CHAT_ID = "7035974555"

# --- FUNÇÕES AUXILIARES (sem alterações) ---
def processar_clientes(row):
    client_string = row['clientsName']
    adjunto = ''
    string_para_processar_loja = client_string
    if not isinstance(client_string, str) or not client_string: return adjunto, pd.NA, client_string
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

def enviar_mensagem_telegram(mensagem):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': mensagem, 'parse_mode': 'Markdown'}
    try:
        requests.get(url, params=params, timeout=10)
        print("Mensagem de notificação enviada para o Telegram com sucesso.")
    except Exception as e:
        print(f"Falha ao enviar notificação para o Telegram: {e}")

# --- LOOP DE PAGINAÇÃO (sem alterações) ---
# (O código de busca na API continua exatamente o mesmo)
todos_os_tickets = []
pagina_atual = 1
print("Iniciando a busca de dados do MoviDesk...")
while True:
    try:
        print(f"Buscando página {pagina_atual}...")
        response = requests.get(f"{BASE_URL}/tickets", params=params)
        response.raise_for_status()
        tickets_da_pagina = response.json()
        if not tickets_da_pagina: print("\nBusca finalizada."); break
        todos_os_tickets.extend(tickets_da_pagina)
        print(f"  > Sucesso! Total acumulado: {len(todos_os_tickets)}")
        params['$skip'] += TAMANHO_PAGINA
        pagina_atual += 1
        time.sleep(1)
    except Exception as e:
        print(f"\nOcorreu um erro na busca: {e}"); break
print("-" * 40)

# --- TRANSFORMAÇÃO E EXPORTAÇÃO DOS DADOS ---
if not todos_os_tickets:
    print("Nenhum ticket foi encontrado com os critérios especificados.")
    enviar_mensagem_telegram(" *ETL Movidesk Concluído*\n\nNenhum ticket novo encontrado com os critérios especificados.")
else:
    print(f"Total de {len(todos_os_tickets)} tickets baixados. Iniciando transformação...")
    df = pd.DataFrame(todos_os_tickets)

    # --- Etapas 1 e 2: Limpeza, Mapeamento e Padronização (sem alterações) ---
    df['resolvedIn'] = pd.to_datetime(df['resolvedIn'], errors='coerce').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    df['ownerId'] = df['owner'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
    df['ownerName'] = df['owner'].apply(lambda x: x.get('businessName', '').strip() if isinstance(x, dict) else '')
    df['clientsName'] = df['clients'].apply(lambda l: ', '.join([c['businessName'] for c in l]) if isinstance(l, list) and l else '')
    df[['adjunto', 'loja_numero', 'clientsName_final']] = df.apply(processar_clientes, axis=1, result_type='expand')
    df = df[~df['ownerName'].str.contains('Luiz Feliphe', na=False, case=False)]
    
    mapa_nomes_oficiais = {'926300168': 'LÍVIA VICTORIA LOURENÇO', '19700723': 'DIEGO CORRENTE TANACA', '1216058519': 'NATÁLIA BICHOFF'}
    mapa_funcoes = {'926300168': 'Focal_1', '1216058519': 'Focal_2', '19700723': 'Coordenador'}
    
    df_final = pd.DataFrame()
    df_final['chave_id'] = df['id'].astype(str)
    df_final['id_planilha'] = None
    df_final['responsavel'] = df['ownerId'].map(mapa_nomes_oficiais).fillna(df['ownerName'])
    df_final['funcao'] = df['ownerId'].map(mapa_funcoes).fillna('N/A')
    df_final['data'] = df['resolvedIn'].dt.strftime('%Y-%m-%d')
    df_final['tarefa'] = 'ATENDIMENTO GERAL'
    df_final['loja'] = df['loja_numero'].astype('Int64').fillna(0)
    df_final['adjunto'] = df['adjunto']
    df_final['tipo'] = 'MOVIDESK'
    df_final['acao'] = 'PASSIVO'
    df_final['assunto'] = df['subject']

    df_final['responsavel'] = df_final['responsavel'].str.upper()
    df_final['adjunto'] = df_final['adjunto'].str.upper()
    mapa_padronizacao_nomes = {
        'LÍVIA LOURENÇO': 'LÍVIA VICTORIA LOURENÇO','SUSANA BRAZ': 'SUSANA DE OLIVEIRA BRAZ',
        'DIEGO CORRENTE T.': 'DIEGO CORRENTE TANACA','FÁBIO PADUÁ' : 'FÁBIO HENRIQUE DE PÁDUA'}
    
    df_final['responsavel'] = df_final['responsavel'].replace(mapa_padronizacao_nomes)
    df_final['adjunto'] = df_final['adjunto'].replace(mapa_padronizacao_nomes)
    adjuntos_validos = ['LÍVIA VICTORIA LOURENÇO', 'NATÁLIA BICHOFF', 'DIEGO CORRENTE TANACA']
    condicao = df_final['adjunto'].fillna('').str.strip().isin(adjuntos_validos)
    df_final['adjunto'] = np.where(condicao, df_final['adjunto'], np.nan)
    
    # --- NOVO: GERAÇÃO DO HASH DE CONTEÚDO ---
    print("Gerando hash de conteúdo para verificação de mudanças...")
    colunas_para_hash = ['responsavel', 'funcao', 'data', 'tarefa', 'loja', 'adjunto', 'tipo', 'acao', 'assunto']

    df_final['conteudo_hash'] = None
    df_final['status'] = None
    # ---------------------------------------------

    # --- NOVO: LÓGICA COMPLETA DE SINCRONIZAÇÃO COM O BANCO DE DADOS (SEM HASH)---
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        
        print("\nBuscando chaves existentes no banco de dados...")
        cursor = connection.cursor(dictionary=True)
        # Buscar APENAS o ID para verificar se já existe. O hash é ignorado.
        cursor.execute(f"SELECT chave_id FROM {TABLE_NAME} WHERE tipo = 'MOVIDESK'")
        db_chaves_existentes = {row["chave_id"] for row in cursor.fetchall()}
        cursor.close()
        print(f" Encontrados {len(db_chaves_existentes)} registros do MoviDesk no banco.")

        print("Preparando lotes para INSERÇÃO e ATUALIZAÇÃO...")
        para_inserir, para_atualizar = [], []
        
        # O hash é ignorado na verificação, todos os registros existentes serão ATUALIZADOS.
        for index, row in df_final.iterrows():
            chave = row["chave_id"]
            if chave not in db_chaves_existentes:
                para_inserir.append(row)
            else:
                para_atualizar.append(row) # Força a atualização de todos os existentes
        
        print(f" Verificação concluída: {len(para_inserir)} para INSERIR, {len(para_atualizar)} para ATUALIZAR.")

        if not para_inserir and not para_atualizar:
            print("Nenhum ticket novo encontrado. Banco de dados já está sincronizado.")
            enviar_mensagem_telegram(f" *Sincronização MoviDesk Concluída*\n\nNenhum ticket novo encontrado.")
        else:
            # 1. INSERÇÃO (Novos Registros)
            if para_inserir:
                df_inserir = pd.DataFrame(para_inserir).replace({np.nan: None, pd.NaT: None})
                print(f"\nInserindo {len(df_inserir)} novos registros...")
                cursor = connection.cursor()
                
                # Adapta a lista de colunas - REMOVEMOS 'conteudo_hash' daqui pois não estamos gerando.
                # Se o banco preenche o hash automaticamente (TRIGGER), remova a coluna da query.
                # Se o banco ACEITA NULL para o hash, podemos mantê-lo, mas ele será NULL.
                
                # *** Se a coluna 'conteudo_hash' deve ser NULL na inserção: ***
                colunas_db = ["chave_id", "id_planilha", "responsavel", "funcao", "data", "tarefa", "loja", "adjunto", "tipo", "acao", "assunto", "conteudo_hash", "status"]
                placeholders = ", ".join(["%s"] * len(colunas_db))
                query_insert = f"INSERT INTO {TABLE_NAME} ({', '.join(f'`{c}`' for c in colunas_db)}) VALUES ({placeholders})"
                
                # Certificamos que o hash e status são None/NULL
                df_inserir['conteudo_hash'] = None 
                df_inserir['status'] = None
                
                dados_inserir = [tuple(r) for r in df_inserir[colunas_db].to_numpy()]
                cursor.executemany(query_insert, dados_inserir)
                cursor.close()

            # 2. ATUALIZAÇÃO (Registros Existentes)
            if para_atualizar:
                df_atualizar = pd.DataFrame(para_atualizar).replace({np.nan: None, pd.NaT: None})
                print(f"\nAtualizando {len(df_atualizar)} registros existentes...")
                cursor = connection.cursor()
                
                # Adaptamos a lista de colunas - REMOVEMOS 'conteudo_hash' do SET de atualização.
                # Se o banco preenche o hash, não queremos sobrescrevê-lo com NULL.
                update_cols = ["id_planilha", "responsavel", "funcao", "data", "tarefa", "loja", "adjunto", "tipo", "acao", "assunto", "status"] # hash REMOVIDO
                update_set = ", ".join([f"`{col}`=%s" for col in update_cols])
                query_update = f"UPDATE {TABLE_NAME} SET {update_set} WHERE `chave_id` = %s"
                
                for _, row in df_atualizar.iterrows():
                    # Status também precisa ser setado para NULL/None se não estiver sendo preenchido
                    row['status'] = None 
                    
                    valores = list(row[update_cols]) + [row["chave_id"]]
                    cursor.execute(query_update, valores)
                cursor.close()

            connection.commit()
            sucesso_msg = f" *Sincronização MoviDesk concluída!*\n\n- *Novos Registros:* {len(para_inserir)}\n- *Registros Atualizados:* {len(para_atualizar)} (Todos os existentes)"
            print("\n" + sucesso_msg.replace('*', ''))
            enviar_mensagem_telegram(sucesso_msg)
            
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nConexão com o banco de dados fechada.")