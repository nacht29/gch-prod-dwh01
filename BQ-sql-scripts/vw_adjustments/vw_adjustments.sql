SELECT 
CONCAT('Yr',FORMAT_DATE('%Y', a.docdate)) as `Calendar Year`,
CONCAT(FORMAT_DATE('%m', a.docdate), '. ', FORMAT_DATE('%b', a.docdate)) as `Calendar Month`,
CASE
  WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(a.docdate, YEAR)) = 1
  THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM a.docdate), '.', EXTRACT(YEAR FROM a.docdate))
  ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM a.docdate) + 1, '.', EXTRACT(YEAR FROM a.docdate))
END AS WeekYear,

CONCAT('Wk', 
		LPAD(
			CAST(
				SPLIT(
					CASE
						WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(a.docdate, YEAR)) = 1
						THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM a.docdate), '.', EXTRACT(YEAR FROM a.docdate))
						ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM a.docdate) + 1, '.', EXTRACT(YEAR FROM a.docdate))
					END,
				'.')[OFFSET(0)]
			AS STRING),
		2, '0')
	) AS Wk,

CONCAT(FORMAT_DATE('%m', a.docdate), '. ', FORMAT_DATE('%d', a.docdate)) as `Calendar Day Month`,

CONCAT(
		LPAD(CAST(MOD(EXTRACT(DAYOFWEEK FROM a.docdate) + 5, 7) + 1 AS STRING), 2, '0'), 
		'.', 
		FORMAT_DATE('%a', a.docdate)
	) AS `Calendar Day Week`,

a.docdate as Date,
b.Location,
-- c.group_code as Division_Code,
c.group_desc as Division,
CONCAT(b.Dept, '-', c.deptdesc) AS Dept,
CONCAT(b.SubDept, '-', c.subdeptdesc) AS SubDept,
CONCAT(b.Category, '-', c.categorydesc) AS Category,
b.Itemcode,
b.Description,
b.AdjType,
a.Reason,
a.RefNo,
b.UM as sales_unit_uom,
-- UMBulk,
-- BulkQty,
CASE WHEN b.adjType = 'OUT' THEN b.Qty * -1 ELSE b.Qty END AS Qty,
CASE WHEN b.adjType = 'OUT' THEN b.UnitPrice * -1 ELSE b.UnitPrice END AS UnitPrice,
CASE WHEN b.adjType = 'OUT' THEN b.TotalPrice * -1 ELSE b.TotalPrice END AS TotalPrice
-- unit of measure, unit of measure qty, bulk UM, bulk UM qty
FROM `gch-prod-dwh01.backend.adjustmain` a
left join `gch-prod-dwh01.backend.adjustchild` b on a.RefNo = b.RefNo
inner join `gch-prod-dwh01.gchbi.hierarchy` c on b.category = c.category
  -- DSB.UM as sales_unit_uom,
  -- DSB.UMBulk as order_unit,
  -- DSB.BulkQty as casepack,
-- 9005, 9010, 9018