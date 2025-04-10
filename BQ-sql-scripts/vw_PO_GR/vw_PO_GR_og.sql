WITH
  GR AS (
  SELECT
	GRM.RefNo GRM_RefNo,
	Location GRM_Location,
	DONo GRM_DONo,
	InvNo GRM_InvNo,
	DocDate GRM_DocDate,
	Code GRM_Code,
	Name GRM_Name,
	Receivedby GRM_ReceivedBy,
	Remark GRM_Remark,
	Total GRM_Total,
	GRC.RefNo GRC_RefNo,
	GRC.PORefNo GRC_PORefNo,
	Line GRC_Line,
	Itemcode GRC_Itemcode,
	Description GRC_Desc,
	Qty GRC_Qty,
	POQty GRC_POQty,
	BulkQty GRC_BulkQty,
	UMBulk GRC_UMBulk,
	BQty GRC_BQty,
	UnitPrice GRC_UnitPrice,
	TotalPrice GRC_TotalPrice,
	PriceType GRC_Price_Type, 
	ibt GRM_IBT,
	--GRC.POLine GRC_POLine
  FROM
	`gch-prod-dwh01.backend.grmain` GRM
  INNER JOIN
	`gch-prod-dwh01.backend.grchild` GRC
  ON
	GRM.RefNo = GRC.RefNo ),
  PO AS (
  SELECT
	POM.RefNo POM_RefNo,
	PODate POM_PODate,
	DeliverDate POM_DeliveryDate,
	expiry_date POM_ExpiryDate,
	BillStatus POM_BillStatus,
	IssuedBy POM_IssuedBy,
	Location POM_Location,
	SCode POM_SCode,
	SName POM_SName,
	Remark POM_Remark,
	cancel POM_Cancel,
	Closed POM_Closed,
	Completed POM_Completed,
	Total POM_Total,
	ibt POM_IBT,
	order_type POM_OrderType,
	POC.RefNo POC_RefNo,
	Line POC_Line,
	EntryType POC_EntryType,
	PriceType POC_PriceType,
	Itemcode POC_Itemcode,
	Description POC_Desc,
	UM POC_UM,
	Qty POC_Qty,
	BulkQty POC_BulkQty,
	UMBulk POC_UMBulk,
	BQty POC_BQty,
	UnitPrice POC_UnitPrice,
	TotalPrice POC_TotalPrice,
	PriceType POC_Price_Type
  FROM
	`gch-prod-dwh01.backend.pomain` POM
  INNER JOIN
	`gch-prod-dwh01.backend.pochild` POC
  ON
	POM.RefNo = POC.RefNo ),
  
Item as (select IM.itemcode, concat(IM.Dept,'-',H_DeptDesc) Dept, concat(IM.subDept,'-',H_SubDeptDesc) SubDept, concat(IM.Category,'-',H_Cat.H_CatDesc) Category
from `gch-prod-dwh01.backend.itemmaster` IM
inner join (select dept, deptdesc H_DeptDesc from `gch-prod-dwh01.gchbi.hierarchy` group by dept, deptdesc) H_Dept on IM.Dept = H_Dept.Dept
inner join (select subdept, subdeptdesc H_SubDeptDesc from `gch-prod-dwh01.gchbi.hierarchy` group by subdept, subdeptdesc) H_SubDept on IM.subDept = H_SubDept.subdept
inner join (select category, categorydesc H_CatDesc from `gch-prod-dwh01.gchbi.hierarchy` group by category, categorydesc) H_Cat on IM.category = H_Cat.category),

Banner as (select branch_code, Banner from `gch-prod-dwh01.daily_summary.Banner_Master`),

IM_BS as (select itemcode, branch, ads IM_BS_ads from `gch-prod-dwh01.backend.itemmaster_branch_stock`)

SELECT
  POM_RefNo,
  CONCAT('Yr',FORMAT_DATE('%Y', POM_PODate)) as `Calendar Year`,
  CONCAT(FORMAT_DATE('%m', POM_PODate), '. ', FORMAT_DATE('%b', POM_PODate)) as `Calendar Month`,
  CASE
	WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(POM_PODate, YEAR)) = 1
	THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM POM_PODate), '.', EXTRACT(YEAR FROM POM_PODate))
	ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM POM_PODate) + 1, '.', EXTRACT(YEAR FROM POM_PODate))
  END AS WeekYear,
  CONCAT(FORMAT_DATE('%m', POM_PODate), '. ', FORMAT_DATE('%d', POM_PODate)) as `Calendar Day Month`,
  POM_PODate,
  POM_DeliveryDate,
  concat(LPAD(CAST(EXTRACT(MONTH from POM_DeliveryDate) as string),2,'0'),'.',FORMAT_DATE('%b', POM_DeliveryDate)) DeliveryMonth,
  concat('Wk',LPAD(cast(EXTRACT(WEEK FROM DATE_TRUNC(POM_DeliveryDate, WEEK(MONDAY))) as string),2,'0')) Calendar_WkNum,
  concat(LPAD(CAST(EXTRACT(DAYOFWEEK FROM DATE_SUB(POM_DeliveryDate, INTERVAL 1 DAY)) as string),2,'0'),'.',FORMAT_DATE('%a', POM_DeliveryDate)) Day_Week,
  POM_ExpiryDate,
  POM_BillStatus,
  POM_IssuedBy,
  POM_Location,
  Banner.Banner,
  POM_SCode,
  POM_SName,
  POM_Remark,
  POM_IBT,
  CASE
	WHEN POM_OrderType = 0 THEN '00-DSP/Con'
	WHEN POM_OrderType = 1 THEN '01-Flow-through'
	WHEN POM_OrderType = 9 THEN '09-NT-DSP'
	WHEN POM_OrderType = 99 THEN '99-NT'
  END
  POM_OrderType,
  POM_Cancel,
  POM_Closed,
  POM_Completed,
  POM_Total,
  POC_Line,
  POC_EntryType,
  POC_PriceType,
  POC_Itemcode,
  Item.Dept,
  Item.SubDept,
  Item.Category,
  POC_Desc,
  POC_UM,
  POC_Qty,
  POC_BulkQty,
  POC_UMBulk,
  POC_BQty,
  POC_UnitPrice,
  IM_BS_ads,
  POC_TotalPrice,
  GRM_RefNo,
  GRM_Location,
  GRM_DONo,
  GRM_InvNo,
  GRM_DocDate,
  GRM_Code,
  GRM_Name,
  GRM_ReceivedBy,
  GRM_Remark,
  GRM_Total,
  GRM_IBT,
  GRC_Line,
  GRC_Itemcode,
  GRC_Desc,
  GRC_Qty,
  GRC_POQty,
  GRC_BulkQty,
  GRC_UMBulk,
  GRC_BQty,
  GRC_UnitPrice,
  GRC_TotalPrice,
  /*IF ((GRC_Qty < GRC_POQty or ifnull(GRC_Qty,0) < ifnull(POC_Qty,0)) and PO.POM_Completed <> 1, 'Not Complete', 'Completed') Receiving_Status,*/
  CASE WHEN GRM_IBT = 0 THEN (GRC_TotalPrice) ELSE 0
  END AS `Final GR Value`,
  CASE WHEN POM_IBT = 0 THEN (POC_TotalPrice) ELSE 0
  END AS `Final PO Value`,
  CASE
	WHEN ((GRC_Qty > GRC_POQty) OR (IFNULL(GRC_Qty,0) > IFNULL(POC_Qty,0))) THEN 'Over Delivery' -- ok
	WHEN ((GRC_Qty = GRC_POQty) OR (IFNULL(GRC_Qty,0) = IFNULL(POC_Qty,0))) THEN 'Full Delivery' -- ok
	WHEN (CURRENT_DATE('+8:00') <= PO.POM_ExpiryDate) AND ((GRC_Qty < GRC_POQty) OR (IFNULL(GRC_Qty,0) < IFNULL(POC_Qty,0))) AND ((GRC_Qty > 0) OR (IFNULL(GRC_Qty,0) > 0)) THEN 'Partial Delivery' --ok
	WHEN (CURRENT_DATE('+8:00') <= PO.POM_ExpiryDate) AND ((IFNULL(GRC_Qty,0) = 0)) THEN 'No Delivery' -- ok
	WHEN (CURRENT_DATE('+8:00') > PO.POM_ExpiryDate) AND ((GRC_Qty >= 0) OR (IFNULL(GRC_Qty,0) >= 0) OR ((GRC_Qty < GRC_POQty) OR (IFNULL(GRC_Qty,0) < IFNULL(POC_Qty,0)))) THEN 'Expired' 
	ELSE 'Others'
END
  Delivery_Status,
  IFNULL(GRC_Qty,0)/IFNULL(POC_Qty,0) Completion_Percentage
FROM
  PO
LEFT OUTER JOIN
  GR
ON
  PO.POM_RefNo = GR.GRC_PORefNo
  AND PO.POC_Itemcode = GR.GRC_Itemcode
  AND PO.POC_PriceType = GR.GRC_Price_Type
  --AND PO.POC_Line = GR.GRC_POLine
INNER JOIN Item on PO.POC_Itemcode = Item.itemcode
LEFT OUTER JOIN Banner on PO.POM_Location = Banner.branch_code
LEFT OUTER JOIN IM_BS on PO.POC_Itemcode = IM_BS.itemcode and PO.POM_Location = IM_BS.branch
  --where POM_RefNo = 'HQPO24123435'
  ;