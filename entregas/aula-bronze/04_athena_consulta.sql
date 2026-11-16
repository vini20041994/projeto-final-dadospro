SELECT event_name,
       count(*)                        AS linhas,
       count(DISTINCT arquivo_origem)  AS arquivos_origem,
       min(year || '-' || month || '-' || day) AS primeiro_dia,
       max(year || '-' || month || '-' || day) AS ultimo_dia,
       max(ingestao_ts)                AS ingestao_ts
FROM bronze.eventos
GROUP BY event_name
ORDER BY linhas DESC;
