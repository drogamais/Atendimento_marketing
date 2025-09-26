CREATE OR REPLACE VIEW vw_chamados_marketing AS
SELECT
    id_fato_chamado,
    id_chamado,
    titulo,
    data_referencia,
    assunto_id,
    assunto_nome,
    situacao,
    situacao_nome,
    -- Lógica para Solicitante (ID preservado)
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 0
        ELSE solicitante_id
    END AS solicitante_id,
    -- Campos de solicitante com valor fixo para Movidesk
    'MOVIDESK' AS solicitante_nome,
    'MOVIDESK' AS departamento_solicitante_nome,
    -- Lógica para Responsável
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 0
        ELSE responsavel_id
    END AS responsavel_id,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE responsavel_nome
    END AS responsavel_nome,
    departamento_responsavel_nome,
    -- Lógica para Apoio
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
    prata_chamados_movidesk
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
    -- Lógica para Solicitante
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 0
        ELSE solicitante_id
    END AS solicitante_id,
    CASE
        WHEN departamento_solicitante_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE solicitante_nome
    END AS solicitante_nome,
    departamento_solicitante_nome,
    -- Lógica para Responsável
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 0
        ELSE responsavel_id
    END AS responsavel_id,
    CASE
        WHEN departamento_responsavel_nome <> 'MARKETING' THEN 'OUTRO'
        ELSE responsavel_nome
    END AS responsavel_nome,
    departamento_responsavel_nome,
    -- Lógica para Apoio
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
    prata_chamados_sults
WHERE
    departamento_responsavel_nome = 'MARKETING'
   OR departamento_solicitante_nome = 'MARKETING'
   OR departamento_apoio_nome = 'MARKETING';