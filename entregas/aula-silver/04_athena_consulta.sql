SELECT event_name,
       count(*)                        AS linhas,
       count(DISTINCT session_id)      AS sessoes,
       count(DISTINCT user_pseudo_id)  AS visitantes,
       count(user_id)                  AS com_user_id,
       min(year || '-' || month || '-' || day) AS primeiro_dia,
       max(year || '-' || month || '-' || day) AS ultimo_dia
FROM silver.eventos
GROUP BY event_name
ORDER BY linhas DESC;
