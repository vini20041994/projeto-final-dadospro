SELECT motivo_rejeicao,
       count(*)                        AS eventos,
       count(DISTINCT event_name)      AS tipos_de_evento,
       min(event_name)                 AS exemplo_evento,
       count(DISTINCT arquivo_origem)  AS arquivos_origem
FROM silver.eventos_rejeitados
GROUP BY motivo_rejeicao
ORDER BY eventos DESC;
