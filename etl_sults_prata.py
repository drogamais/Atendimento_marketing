# run_prata.py

import os
import mariadb
import sys
# Garanta que você tenha um arquivo config.py com o dicionário DB_CONFIG
from config import DB_CONFIG

def executar_scripts_da_pasta(path_da_pasta, cursor):
    """
    Lê e executa todos os arquivos .sql de uma pasta, em ordem alfabética/numérica.
    """
    print(f"\n--- EXECUTANDO SCRIPTS SQL DA PASTA: {path_da_pasta} ---")
    try:
        scripts = sorted([f for f in os.listdir(path_da_pasta) if f.endswith('.sql')])
        
        if not scripts:
            print(f"AVISO: Nenhum script .sql encontrado em '{path_da_pasta}'.")
            return

        for script_file in scripts:
            filepath = os.path.join(path_da_pasta, script_file)
            print(f"-> Executando: {script_file}...")
            
            with open(filepath, 'r', encoding='utf-8') as sql_file:
                # O .split(';') é crucial para rodar múltiplos comandos dentro de um mesmo arquivo
                sql_commands = sql_file.read().split(';')
                for command in sql_commands:
                    # Garante que não executamos comandos vazios resultantes do split
                    if command.strip():
                        cursor.execute(command)
        
        print("[OK] Todos os scripts SQL foram executados com sucesso.")

    except mariadb.Error as e:
        print(f"[FALHA] ERRO ao executar o script '{script_file}'. A execução foi interrompida.")
        raise e # Propaga o erro para o bloco 'main'
    except FileNotFoundError:
        print(f"[FALHA] ERRO: A pasta de scripts '{path_da_pasta}' não foi encontrada.")
        raise

def main():
    """
    Função principal para executar as transformações SQL da camada Prata.
    """
    print("==================================================")
    print("== INICIANDO PROCESSO DE CRIAÇÃO DA CAMADA PRATA ==")
    print("==================================================")
    
    conn = None
    SQL_SCRIPTS_FOLDER = "sql_scripts" 

    try:
        print("\nConectando ao banco de dados para iniciar as transformações...")
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Executa todos os scripts da pasta na ordem correta
        executar_scripts_da_pasta(SQL_SCRIPTS_FOLDER, cursor)
        
        # Confirma todas as transações SQL
        conn.commit()
        print("\n[OK] Transformações SQL commitadas no banco de dados.")
        print("\n[SUCESSO] PROCESSO DA CAMADA PRATA CONCLUÍDO COM SUCESSO! ")

    except Exception as e:
        print(f"\n[FALHA] ERRO CRÍTICO NO PROCESSO DA CAMADA PRATA.")
        print(f"   -> Detalhe do Erro: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()