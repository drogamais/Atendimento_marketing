# run.py

import subprocess
from utils import enviar_mensagem_telegram

def run_script(script_name):
    try:
        print(f"--- Iniciando a execução de: {script_name} ---")
        subprocess.run(
            ["python", script_name],
            check=True,
            text=True,
            capture_output=True
        )
        print(f"--- {script_name} executado com sucesso! ---\n")
        return True
    except FileNotFoundError:
        print(f"ERRO: O arquivo '{script_name}' não foi encontrado.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"ERRO ao executar o script: {script_name}")
        print(f"Código de saída: {e.returncode}")
        print("\n--- Saída Padrão (stdout) ---")
        print(e.stdout)
        print("\n--- Saída de Erro (stderr) ---")
        print(e.stderr)
        print(f"--- Execução interrompida devido a um erro em {script_name} ---\n")
        return False
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao tentar executar {script_name}: {e}")
        return False


def main():
    scripts_para_rodar = [
        "etl_movidesk_bronze.py",
        "etl_movidesk_prata.py",
        "etl_sults_implantacao_bronze.py",
        "etl_sults_implantacao_prata.py",
        "etl_sults_bronze.py",
        "etl_sults_prata.py"
    ]

    print(">>> Iniciando o processo de ETL completo <<<\n")

    for script in scripts_para_rodar:
        if not run_script(script):
            print(">>> Processo de ETL finalizado com erro. <<<")
            break
    else:
        # --- 2. ADICIONE A MENSAGEM DE SUCESSO AQUI ---
        # Este bloco só executa se o 'for' completar sem 'break' (sem erros)
        print(">>> Todos os scripts foram executados com sucesso! <<<")
        mensagem_final = (
            "✅ *Processo de ETL Completo Finalizado!*\n\n"
            "Todos os scripts foram executados com sucesso e os dados foram atualizados."
        )
        enviar_mensagem_telegram(mensagem_final)
        # ----------------------------------------------------


if __name__ == "__main__":
    main()