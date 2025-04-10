WITH H_IM AS (
  select Itemcode, ItemLink, Description, IM.Dept, H_Dept.deptdesc, IM.SubDept, H_SubDept.subdeptdesc, IM.Category, H_Category.categorydesc, consign, costmarginvalue
  from `gch-prod-dwh01.backend.itemmaster` IM 
  inner join (select dept, deptdesc from `gch-prod-dwh01.gchbi.hierarchy` group by dept, deptdesc) H_Dept on trim(IM.dept) = trim(H_Dept.dept) 
  inner join (select subdept, subdeptdesc from `gch-prod-dwh01.gchbi.hierarchy` group by subdept, subdeptdesc) H_SubDept on trim(IM.subdept) = trim(H_SubDept.subdept)
  inner join (select category, categorydesc from `gch-prod-dwh01.gchbi.hierarchy` group by category, categorydesc) H_Category on trim(IM.category) = trim(H_Category.category)
),
BGT AS(
  select sales_budget, parse_date('%Y%m%d',Date) AS YearMonthDay, site, category 
  from `gch-prod-dwh01.daily_summary.2024_Budget` BU
),
PSC AS(
  select itemcode, branch, fifocost, lastcost from `gch-prod-dwh01.gchbi.possales_cost`
  group by itemcode, branch, fifocost, lastcost
),
VDR AS( -- temporary vendor table
  SELECT TRIM(CAST(Site_Key AS STRING)) as Location, TRIM(Site) as Location_Name, TRIM(CAST(`Article _Key`AS STRING)) as Itemcode, TRIM(Article) as Description, TRIM(Primary_Vendor_Key) as Code, TRIM(Primary_Vendor) as Name
  FROM `gch-prod-dwh01.sap.vendor` VR
  WHERE Primary_Vendor_Key != '#'
  GROUP BY Site_Key, Site, `Article _Key`, Article, Primary_Vendor_Key, Primary_Vendor
),
VDR1 AS ( -- for current vendor, wait for panda to fix then use this.
  SELECT IBS.Itemcode, IBS.branch as Location, VR.Code, VR.Name, IMS.priority_vendor 
  FROM `gch-prod-dwh01.backend.itemmaster_branch_stock` IBS
  LEFT JOIN `gch-prod-dwh01.backend.supcus` VR
  ON IBS.ib_sup_code = VR.Code  
  LEFT JOIN `gch-prod-dwh01.backend.itemmastersupcode`IMS
  ON IBS.ib_sup_code = IMS.Code
  where priority_vendor = 0  
  group by IBS.Itemcode, IBS.branch, VR.Code, VR.Name, IMS.priority_vendor
),
DATE_CALC AS(
  SELECT 
  BizDate, FORMAT_DATE('%b %Y', PCD.BizDate) as Month,
	CASE
	  WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(PCD.BizDate, YEAR)) = 1
	  THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM PCD.BizDate), '.', EXTRACT(YEAR FROM PCD.BizDate))
	  ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM PCD.BizDate) + 1, '.', EXTRACT(YEAR FROM PCD.BizDate))
	END AS WeekYear
  FROM `gch-prod-dwh01.gchbi.poschild_d` PCD
  GROUP BY FORMAT_DATE('%b %Y', PCD.BizDate), CONCAT(EXTRACT(WEEK(MONDAY) FROM PCD.BizDate), '.', EXTRACT(YEAR FROM PCD.BizDate)), CONCAT(EXTRACT(WEEK(MONDAY) FROM PCD.BizDate) + 1, '.', EXTRACT(YEAR FROM PCD.BizDate)), PCD.BizDate
),
DATE_CALC2 AS(
  SELECT 
  YearMonthDay, FORMAT_DATE('%b %Y', AGG_GR.YearMonthDay) as Month,
	CASE
	  WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(AGG_GR.YearMonthDay, YEAR)) = 1
	  THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM AGG_GR.YearMonthDay), '.', EXTRACT(YEAR FROM AGG_GR.YearMonthDay))
	  ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM AGG_GR.YearMonthDay) + 1, '.', EXTRACT(YEAR FROM AGG_GR.YearMonthDay))
	END AS WeekYear
  FROM `gch-prod-dwh01.sap.aggsales` AGG_GR
  GROUP BY FORMAT_DATE('%b %Y', AGG_GR.YearMonthDay), CONCAT(EXTRACT(WEEK(MONDAY) FROM AGG_GR.YearMonthDay), '.', EXTRACT(YEAR FROM AGG_GR.YearMonthDay)), CONCAT(EXTRACT(WEEK(MONDAY) FROM AGG_GR.YearMonthDay) + 1, '.', EXTRACT(YEAR FROM AGG_GR.YearMonthDay)), AGG_GR.YearMonthDay
),
DATE_CALC3 AS(
  SELECT 
  SRP_PS.Date, FORMAT_DATE('%b %Y', SRP_PS.Date) as Month,
	CASE
	  WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(SRP_PS.Date, YEAR)) = 1
	  THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM SRP_PS.Date), '.', EXTRACT(YEAR FROM SRP_PS.Date))
	  ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM SRP_PS.Date) + 1, '.', EXTRACT(YEAR FROM SRP_PS.Date))
	END AS WeekYear
  FROM `gch-prod-dwh01.srp.possales` SRP_PS
  GROUP BY FORMAT_DATE('%b %Y', SRP_PS.Date), CONCAT(EXTRACT(WEEK(MONDAY) FROM SRP_PS.Date), '.', EXTRACT(YEAR FROM SRP_PS.Date)), CONCAT(EXTRACT(WEEK(MONDAY) FROM SRP_PS.Date) + 1, '.', EXTRACT(YEAR FROM SRP_PS.Date)), SRP_PS.Date
),
BM AS (
  SELECT * FROM `gch-prod-dwh01.daily_summary.Banner_Master`
)

---------------------
-- main query here --
---------------------

select 
CONCAT('Yr',FORMAT_DATE('%Y', PCD.BizDate)) as `Calendar Year`,
CONCAT(FORMAT_DATE('%m', PCD.BizDate), '. ', FORMAT_DATE('%b', PCD.BizDate)) as `Calendar Month`,
WeekYear,
CONCAT('Wk', 
		LPAD(
			CAST(SPLIT(WeekYear, '.')[OFFSET(0)] AS STRING), 2, '0')
	) AS Wk,
CONCAT(FORMAT_DATE('%m', PCD.BizDate), '. ', FORMAT_DATE('%d', PCD.BizDate)) as `Calendar Day Month`,
CONCAT(
		CASE
			WHEN FORMAT_DATE('%a', PCD.BizDate) = 'Sun' THEN '07'
			WHEN FORMAT_DATE('%a', PCD.BizDate) = 'Mon' THEN '01'
			WHEN FORMAT_DATE('%a', PCD.BizDate) = 'Tue' THEN '02'
			WHEN FORMAT_DATE('%a', PCD.BizDate) = 'Wed' THEN '03'
			WHEN FORMAT_DATE('%a', PCD.BizDate) = 'Thu' THEN '04'
			WHEN FORMAT_DATE('%a', PCD.BizDate) = 'Fri' THEN '05'
			WHEN FORMAT_DATE('%a', PCD.BizDate) = 'Sat' THEN '06'
		END,
		'.',
		FORMAT_DATE('%a', PCD.BizDate)
  ) AS `Calendar Day Week`,
PCD.BizDate,
BM.Banner, PCD.Location, BM.Branch_Name,
CASE WHEN PCD.dept like '1%' THEN '1 - GROCERY'
WHEN PCD.dept like '2%' THEN '2 - FRESH'
WHEN PCD.dept like '3%' THEN '3 - PERISHABLE'
WHEN PCD.dept like '4%' THEN '4 - NON FOODS'
WHEN PCD.dept like '5%' THEN '5 - HEALTH & BEA'
WHEN PCD.dept like '6%' THEN '6 - GMS'
WHEN PCD.dept like '7%' THEN '7 - SERVICES'
WHEN PCD.dept like '8%' THEN '8 - NON TRADE'
ELSE '9 - OTHER'
END Division,
CONCAT(PCD.dept, '-', H_IM.deptdesc) AS Dept, 
CONCAT(PCD.SubDept, '-', H_IM.subdeptdesc) AS SubDept, 
CONCAT(PCD.Category, '-', H_IM.categorydesc) AS Category, 
PCD.Itemcode, PCD.description, H_IM.ItemLink, CONCAT(VDR.Code, '-', VDR.Name) as Vendor, sum(PCD.Qty) as Qty, sum(PCD.sumgrosssales) GrossSales_ext_Tax, sum(PCD.amount_after_tax) as Sales_with_Tax, sum(PCD.sumdisc) Discount, sum(PCD.Amount) NetSales_ext_Tax, sum(PCD.cost) Cost, sum(Amount)-sum(PCD.cost) ScanMargin, COUNT(DISTINCT PCD.RefNo) TransCount, sum(Amount)/COUNT(DISTINCT PCD.RefNo) AvgBasket, 
sum(case when PCD.consign = 0 then PCD.Qty else 0 end) Out_Qty, 
sum(case when PCD.consign = 0 then PCD.Amount else 0 end) Out_NS_E_T, 
sum(case when PCD.consign = 1 then PCD.Qty else 0 end) Con_Qty,
sum(case when PCD.consign = 1 then PCD.Amount else 0 end) Con_NS_E_T,
SUM(PCD.Amount) NS_E_T,
PSC.LastCost as LastCost, PSC.FIFOCost as FIFOCost, ifnull(BGT.sales_budget,0) as Budget,
CASE WHEN H_IM.ItemLink = PCD.Itemcode THEN TRUE ELSE FALSE END AS Parent_Item,
"Panda" as POSType
FROM `gch-prod-dwh01.gchbi.poschild_d` PCD 
left outer join  H_IM on PCD.itemcode = H_IM.Itemcode
left outer join PSC -- cost 
on PCD.itemcode = PSC.itemcode AND PCD.Location = PSC.branch
-- create a scheduled snapshot table to pull the data. link the cost to that table. can set the update schedule to be daily etc
left outer join BGT on BGT.YearMonthDay = PCD.BizDate AND BGT.site = PCD.Location AND PCD.SubDept = BGT.category -- Budget
left outer join VDR on PCD.Location = VDR.Location AND PCD.Itemcode = VDR.Itemcode
left outer join DATE_CALC on PCD.BizDate = DATE_CALC.BizDate
left outer join BM on PCD.Location = BM.Branch_Code
-- where PCD.Bizdate = '2024-12-20' --177920
-- where (H_IM.Dept LIKE ('1%') OR H_IM.Dept LIKE ('2%') OR H_IM.Dept LIKE ('3%') OR H_IM.Dept LIKE ('4%') OR H_IM.Dept LIKE ('5%') OR H_IM.Dept LIKE ('6%'))
group by PCD.BizDate, WeekYear, BM.Banner, Location, BM.Branch_Name, PCD.dept, H_IM.deptdesc, PCD.SubDept, H_IM.subdeptdesc, PCD.Category, H_IM.categorydesc, H_IM.ItemLink, PCD.Itemcode, PCD.description, VDR.Code, VDR.Name, PCD.consign, H_IM.costmarginvalue, PSC.LastCost, PSC.FIFOCost, BGT.sales_budget

union all 

-- SRP
SELECT
CONCAT('Yr',FORMAT_DATE('%Y', SRP_PS.Date)) as `Calendar Year`,
CONCAT(FORMAT_DATE('%m', SRP_PS.Date), '. ', FORMAT_DATE('%b', SRP_PS.Date)) as `Calendar Month`,
WeekYear,
CONCAT('Wk',
		LPAD(
			CAST(SPLIT(WeekYear, '.')[OFFSET(0)] AS STRING), 2, '0')
	) AS Wk,
CONCAT(FORMAT_DATE('%m', SRP_PS.Date), '. ', FORMAT_DATE('%d', SRP_PS.Date)) as `Calendar Day Month`,
CONCAT(
	CASE
		WHEN FORMAT_DATE('%a', SRP_PS.Date) = 'Sun' THEN '07'
		WHEN FORMAT_DATE('%a', SRP_PS.Date) = 'Mon' THEN '01'
		WHEN FORMAT_DATE('%a', SRP_PS.Date) = 'Tue' THEN '02'
		WHEN FORMAT_DATE('%a', SRP_PS.Date) = 'Wed' THEN '03'
		WHEN FORMAT_DATE('%a', SRP_PS.Date) = 'Thu' THEN '04'
		WHEN FORMAT_DATE('%a', SRP_PS.Date) = 'Fri' THEN '05'
		WHEN FORMAT_DATE('%a', SRP_PS.Date) = 'Sat' THEN '06'
	END,
	'.',
	FORMAT_DATE('%a', SRP_PS.Date)
) AS `Calendar Day Week`,
SRP_PS.Date,
BM.Banner, SRP_PS.Branch_Code, BM.Branch_Name,
CASE WHEN H_IM.dept like '1%' THEN '1 - GROCERY'
WHEN H_IM.dept like '2%' THEN '2 - FRESH'
WHEN H_IM.dept like '3%' THEN '3 - PERISHABLE'
WHEN H_IM.dept like '4%' THEN '4 - NON FOODS'
WHEN H_IM.dept like '5%' THEN '5 - HEALTH & BEA'
WHEN H_IM.dept like '6%' THEN '6 - GMS'
WHEN H_IM.dept like '7%' THEN '7 - SERVICES'
WHEN H_IM.dept like '8%' THEN '8 - NON TRADE'
ELSE '9 - OTHER'
END Division,
CONCAT(H_IM.Dept, '-', H_IM.deptdesc) AS Dept, 
CONCAT(H_IM.SubDept, '-', H_IM.subdeptdesc) AS SubDept, 
CONCAT(H_IM.Category, '-', H_IM.categorydesc) AS Category, 
PLU_Number, Descriptor, Itemlink, CONCAT(VDR.Code, '-', VDR.Name) as Vendor, sum(Quantity) Qty, sum(Item_Paid_Price) GrossPrice, 0.0 as Price_with_tax, ifnull(sum(Discount),0)+ifnull(sum(Staff_Discount),0) Discount, 
CASE WHEN ifnull(sum(Item_Paid_Price),0)-(ifnull(sum(Discount),0)+ifnull(sum(Staff_Discount),0)) > 0 THEN ifnull(sum(Item_Paid_Price),0)-(ifnull(sum(Discount),0)+ifnull(sum(Staff_Discount),0)) else 0 end as NetPrice,
0.0 as Cost, ifnull(sum(Item_Paid_Price),0)-(ifnull(sum(Discount),0)+ifnull(sum(Staff_Discount),0))-sum(0.0) ScanMargin, COUNT(DISTINCT Receipt_Number) TransCount, sum(Item_Paid_Price)/COUNT(DISTINCT Receipt_Number) AvgBasket, 
sum(case when H_IM.consign = 0 then SRP_PS.Quantity else 0 end) Out_Qty, 
sum(case when H_IM.consign = 0 then SRP_PS.Item_Paid_Price else 0 end) Out_NS_E_T, 
sum(case when H_IM.consign = 1 then SRP_PS.Quantity else 0 end) Con_Qty,
sum(case when H_IM.consign = 1 then SRP_PS.Item_Paid_Price else 0 end) Con_NS_E_T,
SUM(SRP_PS.Item_Paid_Price) NS_E_T,-- SRP has its own row for consignment cost, dont need to do any calculations
0.0 LastCost, 0.0 FIFOCost, ifnull(BGT.sales_budget,0), NULL as Parent_Item ,"SRP" as POSType
from `gch-prod-dwh01.srp.possales` SRP_PS 
left outer join H_IM on SRP_PS.PLU_Number = H_IM.Itemcode
left outer join BGT on BGT.YearMonthDay = SRP_PS.Date AND BGT.site = SRP_PS.Branch_Code AND H_IM.SubDept = BGT.category
left outer join VDR on SRP_PS.Branch_Code = VDR.Location AND SRP_PS.PLU_NUMBER = VDR.Itemcode
left outer join DATE_CALC3 on SRP_PS.Date = DATE_CALC3.Date
left outer join BM on SRP_PS.Branch_Code = BM.Branch_Code
-- where (H_IM.Dept LIKE ('1%') OR H_IM.Dept LIKE ('2%') OR H_IM.Dept LIKE ('3%') OR H_IM.Dept LIKE ('4%') OR H_IM.Dept LIKE ('5%') OR H_IM.Dept LIKE ('6%'))
group by SRP_PS.Date, WeekYear, BM.Banner, SRP_PS.Branch_Code, BM.Branch_Name, H_IM.dept, H_IM.deptdesc, H_IM.SubDept, H_IM.subdeptdesc, H_IM.Category, H_IM.categorydesc, ItemLink, PLU_Number, Descriptor, VDR.Code, VDR.Name, H_IM.consign, BGT.sales_budget

union all
-- SAP
SELECT  
CONCAT('Yr',FORMAT_DATE('%Y', AGG_GR.YearMonthDay)) as `Calendar Year`,
CONCAT(FORMAT_DATE('%m', AGG_GR.YearMonthDay), '. ', FORMAT_DATE('%b', AGG_GR.YearMonthDay)) as `Calendar Month`,
WeekYear,
CONCAT('Wk', 
		LPAD(
			CAST(SPLIT(WeekYear, '.')[OFFSET(0)] AS STRING), 2, '0')
	) AS Wk,
CONCAT(FORMAT_DATE('%m', AGG_GR.YearMonthDay), '. ', FORMAT_DATE('%d', AGG_GR.YearMonthDay)) as `Calendar Day Month`,
CONCAT(
	CASE
		WHEN FORMAT_DATE('%a', AGG_GR.YearMonthDay) = 'Sun' THEN '07'
		WHEN FORMAT_DATE('%a', AGG_GR.YearMonthDay) = 'Mon' THEN '01'
		WHEN FORMAT_DATE('%a', AGG_GR.YearMonthDay) = 'Tue' THEN '02'
		WHEN FORMAT_DATE('%a', AGG_GR.YearMonthDay) = 'Wed' THEN '03'
		WHEN FORMAT_DATE('%a', AGG_GR.YearMonthDay) = 'Thu' THEN '04'
		WHEN FORMAT_DATE('%a', AGG_GR.YearMonthDay) = 'Fri' THEN '05'
		WHEN FORMAT_DATE('%a', AGG_GR.YearMonthDay) = 'Sat' THEN '06'
	END,
	'.',
	FORMAT_DATE('%a', AGG_GR.YearMonthDay)
) AS `Calendar Day Week`,
AGG_GR.YearMonthDay as Date,
BM.Banner,
CAST(AGG_GR.SITE AS STRING) as Location, BM.Branch_Name,
CASE WHEN H_IM.dept like '1%' THEN '1 - GROCERY'
WHEN H_IM.dept like '2%' THEN '2 - FRESH'
WHEN H_IM.dept like '3%' THEN '3 - PERISHABLE'
WHEN H_IM.dept like '4%' THEN '4 - NON FOODS'
WHEN H_IM.dept like '5%' THEN '5 - HEALTH & BEA'
WHEN H_IM.dept like '6%' THEN '6 - GMS'
WHEN H_IM.dept like '7%' THEN '7 - SERVICES'
WHEN H_IM.dept like '8%' THEN '8 - NON TRADE'
ELSE '9 - OTHER'
END Division,
CONCAT(H_IM.Dept, '-', H_IM.deptdesc) AS Dept, 
CONCAT(H_IM.SubDept, '-', H_IM.subdeptdesc) AS SubDept, 
CONCAT(H_IM.Category, '-', H_IM.categorydesc) AS Category,
CAST(AGG_GR.Article as STRING) as Itemcode,
H_IM.Description, H_IM.Itemlink, CONCAT(VDR.Code, '-', VDR.Name) as Vendor, sum(AGG_GR.SALES_QTY_SALES_UOM) as Qty, sum(AGG_GR.SALES_EXCLUDE_TAX + AGG_GR.PROMOTION_SALES_DISCOUNT_VALUE) as Sales_ex_Tax, sum(AGG_GR.SALES_INCLUDE_TAX) as Sales_in_Tax, SUM(PROMOTION_SALES_DISCOUNT_VALUE) as Discount, sum(AGG_GR.SALES_EXCLUDE_TAX) as Net_Sales_ex_Tax, sum(AGG_GR.COGS_L3) as Cost, sum(AGG_GR.SCAN_MARGIN_L4) as Scan_Margin, 0.0 as TransCount, 0.0 as AvgBasket,
-- sum(AGG_GR.SALES_EXCLUDE_TAX)/Trans_Count as AvgBasket,
SUM(AGG_GR.OUTRIGHT_SALES_QTY_BASE_UOM) Out_Qty,
SUM(AGG_GR.OUTRIGHT_SALES_EXCLUDE_TAX) Outright, -- Outright
SUM(AGG_GR.CONSIGNMENT_SALES_QTY_BASE_UOM) Con_Qty,
sum(AGG_GR.CONSIGNMENT_SALES_EXCLUDE_TAX) Consignment, -- Consignment
SUM(AGG_GR.SALES_EXCLUDE_TAX) Net_Sales,
0.0 LastCost, 0.0 FIFOCost, ifnull(BGT.sales_budget,0) Budget, NULL as Parent_Item, "SAP" as POS_TYPE
from `gch-prod-dwh01.sap.aggsales` AGG_GR
-- left join SAP_Transcount on AGG_GR.YearMonthDay = SAP_Transcount.YearMonthDay
left outer join H_IM on CAST(AGG_GR.Article as STRING) = H_IM.Itemcode
left outer join BGT on AGG_GR.YearMonthDay = BGT.YearMonthDay AND CAST(AGG_GR.SITE AS STRING) = BGT.site AND H_IM.SubDept = BGT.category
left outer join VDR on CAST(AGG_GR.SITE AS STRING) = VDR.Location AND CAST(AGG_GR.Article as STRING) = VDR.Itemcode
left outer join DATE_CALC2 on AGG_GR.YearMonthDay = DATE_CALC2.YearMonthDay
left outer join BM on CAST(AGG_GR.SITE AS STRING) = BM.Branch_Code
-- where (H_IM.Dept LIKE ('1%') OR H_IM.Dept LIKE ('2%') OR H_IM.Dept LIKE ('3%') OR H_IM.Dept LIKE ('4%') OR H_IM.Dept LIKE ('5%') OR H_IM.Dept LIKE ('6%'))
-- where AGG_GR.YearMonthDay > '2023-12-31'
group by AGG_GR.YearMonthDay, WeekYear, BM.Banner, AGG_GR.Site, BM.Branch_Name, AGG_GR.Article, H_IM.dept, H_IM.deptdesc, H_IM.SubDept, H_IM.subdeptdesc, H_IM.Category, H_IM.categorydesc, H_IM.Description, H_IM.ItemLink, VDR.Code, VDR.Name, BGT.sales_budget
