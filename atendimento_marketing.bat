@echo off
REM ============================================================================
REM == Script para execucao do processo de ETL com ambiente virtual (venv)    ==
REM == O output sera salvo em 'execution_log.txt', sobrescrevendo o anterior. ==
REM ============================================================================

echo.
echo [INICIO] Iniciando o processo de ETL completo...

REM --- Cria o cabecalho do log com data e hora ---
echo ============================================================================ > execution_log.txt
echo == LOG DE EXECUCAO DO ETL                                                 == >> execution_log.txt
echo ==                                                                        == >> execution_log.txt
echo == Data: %date%                                                       == >> execution_log.txt
echo == Hora: %time%                                                       == >> execution_log.txt
echo ============================================================================ >> execution_log.txt
echo. >> execution_log.txt

echo A saida do console esta sendo gravada em execution_log.txt...
echo.

REM --- 1. Ativa o ambiente virtual ---
call .\venv\Scripts\activate

REM --- 2. Executa o script principal e anexa a saida ao log ---
REM O simbolo '>>' anexa o conteudo ao final do arquivo, em vez de apaga-lo.
python run.py >> execution_log.txt 2>&1

REM --- 3. Desativa o ambiente virtual ---
call deactivate

echo.
echo [FIM] Processo de ETL finalizado. Verifique o arquivo execution_log.txt para o resultado.
echo.

pause