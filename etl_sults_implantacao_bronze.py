import requests
import pandas as pd
import json
from datetime import datetime
from bs4 import BeautifulSoup
import re
import html
import mariadb
import sys

# --- Configurações da API ---
API_TOKEN = "O2Ryb2dhbWFpczsxNzQ0ODAzNDc1NjIx" 
BASE_URL_PROJETOS = "https://api.sults.com.br/api/v1/implantacao/projeto"
BASE_URL_TAREFAS = "https://api.sults.com.br/api/v1/implantacao/projeto/{projetoId}/tarefa"

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
# 🚨 PREENCHA COM AS SUAS INFORMAÇÕES 🚨
CONFIGURACOES_BANCO = {
    "user": "drogamais",
    "password": "dB$MYSql@2119",
    "host": "10.48.12.20",
    "port": 3306,
    "database": "dbSults"
}

# --- Cabeçalhos da Requisição ---
headers = { "Authorization": API_TOKEN, "Content-Type": "application/json;charset=UTF-8" }

# --- Funções Auxiliares e de Extração (sem alterações) ---
def limpar_html(texto_html):
    if not isinstance(texto_html, str): return ""
    texto_decodificado = html.unescape(texto_html)
    soup = BeautifulSoup(texto_decodificado, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def buscar_todos_os_projetos():
    todos_os_projetos = []
    start = 0
    limit = 100
    print("ETAPA 1: Buscando todos os projetos para obter os IDs...")
    while True:
        params = {'start': start, 'limit': limit}
        try:
            response = requests.get(BASE_URL_PROJETOS, headers=headers, params=params)
            response.raise_for_status()
            dados_da_pagina = response.json().get("data", [])
            if not dados_da_pagina: break
            todos_os_projetos.extend(dados_da_pagina)
            start += 1
        except Exception as e:
            print(f"  -> Erro ao buscar projetos: {e}")
            break
    print(f"-> Encontrado(s) {len(todos_os_projetos)} projeto(s).")
    return todos_os_projetos

def buscar_todas_as_tarefas(lista_projetos):
    todas_as_tarefas = []
    total_projetos = len(lista_projetos)
    print(f"\nETAPA 2: Buscando tarefas para os {total_projetos} projetos encontrados...")
    for i, projeto in enumerate(lista_projetos):
        projeto_id = projeto.get('id')
        if not projeto_id: continue
        print(f"  ({i+1}/{total_projetos}) Buscando tarefas do projeto ID: {projeto_id}...", end='')
        tarefas_do_projeto = []
        start = 0
        limit = 50
        while True:
            params = {'start': start, 'limit': limit}
            url = BASE_URL_TAREFAS.format(projetoId=projeto_id)
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                dados_da_pagina = response.json().get("data", [])
                if not dados_da_pagina: break
                for tarefa in dados_da_pagina:
                    tarefa['projeto_id'] = projeto_id
                tarefas_do_projeto.extend(dados_da_pagina)
                start += 1
            except requests.exceptions.HTTPError as http_err:
                print(f" Erro HTTP {http_err.response.status_code}.")
                try: erro_detalhado = http_err.response.json(); print(f"    -> Detalhes do erro: {erro_detalhado}")
                except json.JSONDecodeError: print(f"    -> Resposta do servidor (não-JSON): {http_err.response.text}")
                break
            except Exception as e:
                print(f"  -> Erro inesperado ao buscar tarefas para o projeto {projeto_id}: {e}")
                break
        print(f" {len(tarefas_do_projeto)} tarefas encontradas.")
        todas_as_tarefas.extend(tarefas_do_projeto)
    return todas_as_tarefas

# --- Função de Carga no Banco de Dados ---
def tratar_e_salvar_tarefas(tarefas_json, nome_tabela):
    if not tarefas_json:
        print("\nNenhuma tarefa encontrada para processar.")
        return

    print("\n--- INICIANDO TRATAMENTO DOS DADOS DAS TAREFAS ---")
    df = pd.json_normalize(tarefas_json, max_level=1)
    df.columns = [col.replace('.', '_') for col in df.columns]
    if 'descricaoHtml' in df.columns:
        df['descricaoHtml'] = df['descricaoHtml'].apply(limpar_html)
    
    colunas_numericas_id = ['id', 'projeto_id', 'fase_id', 'responsavel_id', 'funcao_id']
    for col in colunas_numericas_id:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    colunas_de_data = ['dtCriacao', 'dtInicio', 'dtFim', 'dtConclusao']
    for col in colunas_de_data:
        if col in df.columns:
            s = pd.to_datetime(df[col], errors='coerce')
            df[col] = s.dt.tz_localize(None)
    
    print("--- TRATAMENTO FINALIZADO ---")

    # --- [NOVO] Bloco de Verificação Final ---
    print("\n--- VERIFICANDO DADOS ANTES DA INSERÇÃO ---")
    if 'porcentagemConclusao' in df.columns:
        print("Resumo da coluna 'porcentagemConclusao' após o tratamento:")
        # value_counts() nos mostra exatamente quais valores existem na coluna e quantos de cada
        print(df['porcentagemConclusao'].value_counts(dropna=False))
    else:
        print("Coluna 'porcentagemConclusao' não encontrada no DataFrame final.")
    # ----------------------------------------------

    conn = None
    try:
        conn = mariadb.connect(**CONFIGURACOES_BANCO)
        cursor = conn.cursor()
        print(f"\nApagando a tabela antiga '{nome_tabela}' (se existir)...")
        cursor.execute(f"DROP TABLE IF EXISTS `{nome_tabela}`")
        print("Criando a nova estrutura da tabela para tarefas...")
        sql_create_table = f"""
        CREATE TABLE `{nome_tabela}` (
            `id` DECIMAL(38, 0) NOT NULL PRIMARY KEY, `projeto_id` DECIMAL(38, 0) NULL,
            `nome` VARCHAR(255) NULL, `codigo` VARCHAR(50) NULL, `descricaoHtml` TEXT NULL,
            `prioridade` INT NULL, `situacao` INT NULL, `dtCriacao` DATETIME NULL,
            `dtInicio` DATETIME NULL, `dtFim` DATETIME NULL, `dtConclusao` DATETIME NULL,
            `porcentagemConclusao` DECIMAL(5, 2) NULL, `fase_id` DECIMAL(38, 0) NULL,
            `fase_nome` VARCHAR(255) NULL, `fase_codigo` VARCHAR(50) NULL,
            `responsavel_id` DECIMAL(38, 0) NULL, `responsavel_nome` VARCHAR(255) NULL,
            `funcao_id` DECIMAL(38, 0) NULL, `funcao_nome` VARCHAR(255) NULL,
            `data_atualizacao` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        cursor.execute(sql_create_table)
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{CONFIGURACOES_BANCO['database']}' AND TABLE_NAME = '{nome_tabela}'")
        colunas_da_tabela = [row[0] for row in cursor.fetchall()]
        df_final = df[[col for col in colunas_da_tabela if col in df.columns]]
        df_para_inserir = df_final.astype(object).where(pd.notna(df_final), None)
        dados_para_inserir = [tuple(x) for x in df_para_inserir.to_numpy()]
        sql_insert = f"INSERT INTO `{nome_tabela}` ({', '.join([f'`{c}`' for c in df_final.columns])}) VALUES ({', '.join(['?' for _ in df_final.columns])})"
        print(f"Inserindo {len(dados_para_inserir)} registros na tabela de tarefas...")
        cursor.executemany(sql_insert, dados_para_inserir)
        conn.commit()
        print(f"[OK] Sucesso! Dados inseridos na tabela '{nome_tabela}'.")
    except mariadb.Error as e:
        print(f"[FALHA] Erro ao interagir com o MariaDB: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    lista_de_projetos = buscar_todos_os_projetos()
    if lista_de_projetos:
        lista_de_tarefas = buscar_todas_as_tarefas(lista_de_projetos)
        tratar_e_salvar_tarefas(lista_de_tarefas, "bronze_sults_implantacao")
    else:
        print("\nNenhum projeto foi encontrado para iniciar a busca por tarefas.")