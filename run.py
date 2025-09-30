import subprocess
import sys
import os
from utils import enviar_mensagem_telegram

def get_python_executable():
    """Encontra o caminho correto para o executável do Python dentro do venv."""
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        executable = os.path.join(sys.prefix, 'Scripts', 'python.exe')
        if os.path.exists(executable):
            return executable
    return sys.executable

def run_script(script_name, python_executable):
    """Executa um script Python e deixa sua saída ir direto para o console."""
    try:
        print(f"--- Iniciando a execução de: {script_name} ---")
        
        # MUDANÇA PRINCIPAL: Removemos a captura de output.
        # As mensagens do script filho irão direto para o console.
        result = subprocess.run(
            [python_executable, script_name],
            check=True, # Garante que o script pare se houver um erro interno.
            text=True
        )
        
        print(f"--- {script_name} executado com sucesso! ---\n")
        return True
    except subprocess.CalledProcessError as e:
        # Este bloco só será executado se um dos scripts de ETL falhar de verdade.
        print(f"\nERRO CRÍTICO ao executar o script: {script_name}")
        print(f"O processo foi interrompido.")
        # O erro detalhado já terá sido exibido no console pelo próprio script que falhou.
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

    python_exe = get_python_executable()

    for script in scripts_para_rodar:
        if not run_script(script, python_exe):
            print(">>> Processo de ETL finalizado com erro. <<<")
            enviar_mensagem_telegram("❌ *Processo de ETL Falhou!*\n\nUm erro crítico ocorreu. Verifique o console para detalhes.")
            break
    else:
        print(">>> Todos os scripts foram executados com sucesso! <<<")
        mensagem_final = (
            "✅ *Processo de ETL Completo Finalizado!*\n\n"
            "Todos os scripts foram atualizados com sucesso."
        )
        enviar_mensagem_telegram(mensagem_final)

if __name__ == "__main__":
    main()