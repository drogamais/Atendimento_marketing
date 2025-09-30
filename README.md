# Navegue até o diretório onde você quer instalar o projeto (ex: /home/user/projetos)
cd /caminho/para/instalar

# Clone o repositório
git clone https://github.com/drogamais/Atendimento_marketing

# Entre na pasta do projeto
cd Atendimento_marketing

# Crie o venv
python -m venv venv

# Ative o venv
# No Linux:
source venv/bin/activate

# No Windows Server:
.\venv\Scripts\activate

# Instalar as bibliotecas no venv
pip install -r requirements.txt

# Para rodar o codigo
python run.py
# OU
.\atendimento_marketing.bat