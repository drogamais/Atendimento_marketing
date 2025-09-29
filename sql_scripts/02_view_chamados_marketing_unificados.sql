-- Atendimento_marketing/sql_scripts/02_view_chamados_marketing_unificados.sql

CREATE OR REPLACE VIEW view_sults_marketing AS
SELECT
    id_fato_chamado,
    id_chamado,
    titulo,
    data_referencia,
    assunto_id,
    assunto_nome,
    situacao,
    situacao_nome,
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 0
        ELSE solicitante_id
    END AS solicitante_id,
    'MOVIDESK' AS solicitante_nome,
    'MOVIDESK' AS departamento_solicitante_nome,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 0
        ELSE responsavel_id
    END AS responsavel_id,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE responsavel_nome
    END AS responsavel_nome,
    departamento_responsavel_nome,
    CASE
        WHEN departamento_apoio_nome <> 'MARKETING' THEN 0
        ELSE id_pessoa_apoio
    END AS id_pessoa_apoio,
    CASE
        WHEN departamento_apoio_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE nome_apoio
    END AS nome_apoio,
    departamento_apoio_nome,
    tipo_origem
FROM
    silver_movidesk_chamados
WHERE
    departamento_responsavel_nome = 'MARKETING'
   OR departamento_apoio_nome = 'MARKETING'

UNION ALL

SELECT
    id_fato_chamado,
    id_chamado,
    titulo,
    data_referencia,
    assunto_id,
    assunto_nome,
    situacao,
    situacao_nome,
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 0
        ELSE solicitante_id
    END AS solicitante_id,
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE solicitante_nome
    END AS solicitante_nome,
    departamento_solicitante_nome,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 0
        ELSE responsavel_id
    END AS responsavel_id,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE responsavel_nome
    END AS responsavel_nome,
    departamento_responsavel_nome,
    CASE
        WHEN departamento_apoio_nome <> 'MARKETING' THEN 0
        ELSE id_pessoa_apoio
    END AS id_pessoa_apoio,
    CASE
        WHEN departamento_apoio_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE nome_apoio
    END AS nome_apoio,
    departamento_apoio_nome,
    tipo_origem
FROM
    silver_sults_chamados
WHERE
    departamento_responsavel_nome = 'MARKETING'
   OR departamento_solicitante_nome = 'MARKETING'
   OR departamento_apoio_nome = 'MARKETING'
   
UNION ALL

SELECT
    id_fato_implantacao AS id_fato_chamado,
    id_implantacao AS id_chamado,
    titulo,
    data_referencia,
    assunto_id,
    assunto_nome,
    situacao, -- SIMPLIFICADO: Apenas seleciona a coluna já tratada
    situacao_nome, -- SIMPLIFICADO: Apenas seleciona a coluna já tratada
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 0
        ELSE solicitante_id
    END AS solicitante_id,
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE solicitante_nome
    END AS solicitante_nome,
    departamento_solicitante_nome,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 0
        ELSE responsavel_id
    END AS responsavel_id,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE responsavel_nome
    END AS responsavel_nome,
    departamento_responsavel_nome,
    CASE
        WHEN departamento_apoio_nome <> 'MARKETING' THEN 0
        ELSE id_pessoa_apoio
    END AS id_pessoa_apoio,
    CASE
        WHEN departamento_apoio_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE nome_apoio
    END AS nome_apoio,
    departamento_apoio_nome,
    tipo_origem
FROM
    silver_sults_implantacao
WHERE
    departamento_responsavel_nome = 'MARKETING'
   OR departamento_solicitante_nome = 'MARKETING'
   OR departamento_apoio_nome = 'MARKETING';