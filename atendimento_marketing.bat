@echo off
REM ============================================================================
REM == Script para execução do processo de ETL com ambiente virtual (venv)    ==
REM ============================================================================

echo.
echo [INICIO] Iniciando o processo de ETL completo...
echo.

REM --- 1. Ativa o ambiente virtual ---
echo Ativando o ambiente virtual...
call .\venv\Scripts\activate

REM --- 2. Executa o script principal do Python ---
echo Executando o script principal (run.py)...
python run.py

REM --- 3. Desativa o ambiente virtual (opcional, mas boa pratica) ---
echo Desativando o ambiente virtual...
call deactivate

echo.
echo [FIM] Processo de ETL finalizado.
echo.