SELECT "2025-02-28" as BizDate, DSB.branch, BM.branch_name, 
CASE WHEN DSB.dept like '1%' THEN '1 - GROCERY'
WHEN DSB.dept like '2%' THEN '2 - FRESH'
WHEN DSB.dept like '3%' THEN '3 - PERISHABLE'
WHEN DSB.dept like '4%' THEN '4 - NON FOODS'
WHEN DSB.dept like '5%' THEN '5 - HEALTH & BEA'
WHEN DSB.dept like '6%' THEN '6 - GMS'
WHEN DSB.dept like '7%' THEN '7 - SERVICES'
WHEN DSB.dept like '8%' THEN '8 - NON TRADE'
ELSE '9 - OTHER'
END Division,
DSB.Dept, DSB.deptdesc, DSB.Subdept, DSB.Subdeptdesc, DSB.Category, DSB.Categorydesc, DSB.Itemcode, DSB.Description, DSB.QOH, DSB.LastCost
FROM `gch-prod-dwh01.Stock.daily_stock_balance` DSB
LEFT JOIN `gch-prod-dwh01.daily_summary.Banner_Master_1` BM on DSB.branch = BM.branch_code
WHERE Extract_Day = "2025-03-01" 
and BM.Banner LIKE '%Upscale%' -- Super, Upscale, Mini
order by extract_day, branch, dept
