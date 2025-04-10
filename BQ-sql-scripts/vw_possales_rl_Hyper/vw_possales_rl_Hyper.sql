WITH 
BM AS (
  SELECT * FROM `gch-prod-dwh01.daily_summary.Banner_Master`
),
ITEM AS(
  SELECT extract_day, branch, itemcode, AvgDailySales 
  from `gch-prod-dwh01.Stock.daily_stock_balance`
  -- where extract_day = '2025-03-06'--733987 -- 78054
  group by extract_day, branch, Itemcode, AvgDailySales --7955841
),
ITEM2 AS (
  SELECT ITEM.extract_day, IM.Dept, ITEM.Itemcode, ITEM.AvgDailySales, IM.Description, ITEM.branch, BM.Banner
  from ITEM
  left outer join BM on ITEM.branch = BM.branch_code --78045
  left outer join `gch-prod-dwh01.backend.itemmaster` IM on ITEM.Itemcode = IM.Itemcode
),
SRC_TBL AS (
  SELECT 
	PCD.BizDate, PCD.Location, PCD.Itemcode, PCD.Qty,
  FROM `gch-prod-dwh01.gchbi.poschild_d` PCD 
  -- where bizdate = '2025-03-01'
  -- UNION ALL
  -- SELECT 
  --   AGG_GR.YearMonthDay, CAST(AGG_GR.SITE AS STRING) Location, CAST(AGG_GR.ARTICLE AS STRING) Itemcode, AGG_GR.SALES_QTY_SALES_UOM as Qty,
  -- FROM `gch-prod-dwh01.sap.aggsales` AGG_GR
  -- UNION ALL
  -- SELECT 
  --   SRP_PS.Date, SRP_PS.branch_code, SRP_PS.PLU_Number Itemcode,SRP_PS.Quantity,
  -- FROM `gch-prod-dwh01.srp.possales` SRP_PS
),
TBL1 AS(
  SELECT 
  CASE WHEN PCD.BizDate is NULL THEN DATE_SUB(ITEM2.extract_day, INTERVAL 1 DAY) ELSE PCD.BizDate END AS BizDate,
  ITEM2.Dept, ITEM2.Itemcode, ITEM2.Description, ITEM2.branch, ITEM2.Banner, COALESCE(SUM(PCD.QTY), 0) Qty, ITEM2.AvgDailySales
  from ITEM2
  left join `gch-prod-dwh01.gchbi.poschild_d` PCD on DATE_SUB(ITEM2.extract_day, INTERVAL 1 DAY) = PCD.BizDate AND ITEM2.Itemcode = PCD.ITEMCODE AND ITEM2.branch = PCD.Location
  -- where branch in ('1001','1006') and extract_day = '2025-01-10'
  group by ITEM2.extract_day, PCD.BizDate, ITEM2.Dept, ITEM2.Itemcode, ITEM2.Description, ITEM2.branch, ITEM2.Banner, ITEM2.AvgDailySales -- 78045
),
date_calc AS (
	SELECT
		TBL1.BizDate AS date,
		CONCAT('Yr',FORMAT_DATE('%Y', TBL1.BizDate)) as `Calendar Year`,
		CONCAT(FORMAT_DATE('%m', TBL1.BizDate), '. ', FORMAT_DATE('%b', TBL1.BizDate)) as `Calendar Month`,

		CASE
		WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(TBL1.BizDate, YEAR)) = 1
		THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM TBL1.BizDate), '.', EXTRACT(YEAR FROM TBL1.BizDate))
		ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM TBL1.BizDate) + 1, '.', EXTRACT(YEAR FROM TBL1.BizDate))
		END AS WeekYear,

		CONCAT('Wk', 
				LPAD(
					CAST(
						SPLIT(
							CASE
								WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(TBL1.BizDate, YEAR)) = 1
								THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM TBL1.BizDate), '.', EXTRACT(YEAR FROM TBL1.BizDate))
								ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM TBL1.BizDate) + 1, '.', EXTRACT(YEAR FROM TBL1.BizDate))
							END,
						'.')[OFFSET(0)]
					AS STRING),
				2, '0')
			) AS Wk,

			CONCAT(
				LPAD(CAST(MOD(EXTRACT(DAYOFWEEK FROM TBL1.BizDate) + 5, 7) + 1 AS STRING), 2, '0'), 
				'.', 
				FORMAT_DATE('%a', TBL1.BizDate)
	) AS `Calendar Day Week`

	FROM TBL1
	GROUP BY BizDate
)

----------------
-- main query --
----------------

SELECT
-- main date: TBL1.BizDate
date_calc.`Calendar Year`,
date_calc.`Calendar Month`,
date_calc.WeekYear,
date_calc.Wk,
date_calc.`Calendar Day Week`,
TBL1.BizDate, TBL1.branch Location, 
CASE WHEN dept like '1%' THEN '1 - GROCERY'
WHEN dept like '2%' THEN '2 - FRESH'
WHEN dept like '3%' THEN '3 - PERISHABLE'
WHEN dept like '4%' THEN '4 - NON FOODS'
WHEN dept like '5%' THEN '5 - HEALTH & BEA'
WHEN dept like '6%' THEN '6 - GMS'
WHEN dept like '7%' THEN '7 - SERVICES'
WHEN dept like '8%' THEN '8 - NON TRADE'
ELSE '9 - OTHER' END AS Division, 
TBL1.Dept, TBL1.Itemcode, TBL1.Description, TBL1.Banner, TBL1.Qty, TBL1.AvgDailySales,
	(SUM(COALESCE(TBL1.Qty, 0)) OVER (PARTITION BY TBL1.branch, TBL1.Itemcode ORDER BY UNIX_DATE(TBL1.BizDate) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW))/30 AS ADS_RL30,
		(SUM(COALESCE(TBL1.Qty, 0)) OVER (PARTITION BY TBL1.branch, TBL1.Itemcode ORDER BY UNIX_DATE(TBL1.BizDate) RANGE BETWEEN 89 PRECEDING AND CURRENT ROW))/90 AS ADS_RL90
	FROM TBL1
	INNER JOIN date_calc ON date_calc.date = TBL1.BizDate
-- WHERE TBL1.BizDate = '2025-01-01'
	-- where banner LIKE ('%Hyper%') and TBL1.BizDate = Current_Date()
	-- where extract_day between '2025-01-01' and '2025-02-28' and branch in ('1001', '1006') and itemcode in ('4104952', '4211676')
	-- and extract_day = '2024-12-31' and branch in ('1006') and itemcode in ('4211676')