select BM.Banner, IBS.branch, BM.branch_name,
CASE WHEN IM.dept like '1%' THEN '1 - GROCERY'
WHEN IM.dept like '2%' THEN '2 - FRESH'
WHEN IM.dept like '3%' THEN '3 - PERISHABLE'
WHEN IM.dept like '4%' THEN '4 - NON FOODS'
WHEN IM.dept like '5%' THEN '5 - HEALTH & BEA'
WHEN IM.dept like '6%' THEN '6 - GMS'
WHEN IM.dept like '7%' THEN '7 - SERVICES'
WHEN IM.dept like '8%' THEN '8 - NON TRADE'
ELSE '9 - OTHER'
END Division,
IM.Dept, IM.SubDept, IM.Itemcode, IM.Description, IM.AverageCost, IBS.QOH, IM.FIFOCOST, IM.LastCost, IBS.ib_status
from `gch-prod-dwh01.backend.itemmaster` IM
LEFT JOIN `gch-prod-dwh01.backend.itemmaster_branch_stock` IBS on IM.Itemcode = IBS.itemcode
LEFT JOIN `gch-prod-dwh01.daily_summary.Banner_Master_Copy` BM on IBS.branch = BM.branch_code
WHERE (IM.dept LIKE '1%' OR IM.dept LIKE '3%') AND IBS.ib_status = 0 
AND BM.Banner Like '%Super%' -- Hyper, Upscale, Super, Mini
order by branch, Itemcode
