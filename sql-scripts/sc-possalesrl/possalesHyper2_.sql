SELECT Bizdate, Location, 
CASE WHEN dept like '1%' THEN '1 - GROCERY'
WHEN dept like '2%' THEN '2 - FRESH'
WHEN dept like '3%' THEN '3 - PERISHABLE'
WHEN dept like '4%' THEN '4 - NON FOODS'
WHEN dept like '5%' THEN '5 - HEALTH & BEA'
WHEN dept like '6%' THEN '6 - GMS'
WHEN dept like '7%' THEN '7 - SERVICES'
WHEN dept like '8%' THEN '8 - NON TRADE'
ELSE '9 - OTHER'
END Division,
Dept, Itemcode, Description, Banner, Qty, AvgDailySales, ADS_RL30, ADS_RL90
FROM `gch-prod-dwh01.Stock.possales_rl` 
WHERE 
BizDate = DATE_SUB(CURRENT_DATE('+08:00'), INTERVAL 1 DAY)
AND BANNER LIKE ('%Hyper%') AND dept like ('2%')