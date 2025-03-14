select * from `gch-prod-dwh01.Stock.possales_rl`
WHERE BizDate = DATE_SUB(CURRENT_DATE('+08:00'), INTERVAL 1 DAY)
AND dept like ('1%')
