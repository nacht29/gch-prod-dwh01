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

	CONCAT(
		LPAD(CAST(MOD(EXTRACT(DAYOFWEEK FROM PODate) + 5, 7) + 1 AS STRING), 2, '0'), 
		'.', 
		FORMAT_DATE('%a', PODate)
	) AS `Calendar Day Week`,


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
	POM.RefNo = POC.RefNo),
  
  DC_Req AS (
  SELECT
	DCR.TRANS_GUID DCR_TransGUID,
	DCR.RefNo DCR_RefNo,
	DCR.TRANS_TYPE DCR_TRANS_Typ,
	DCR.DocDate DCR_DocDate,
	DCR.DeliverDate DCR_DeliveryDate,
	CONCAT(DCR.LocFrom,"-",DCR.LocFromName) DCR_LocFrom,
	CONCAT(DCR.LocTo,"-",DCR.LocToName) DCR_LocTo,
	post_status DCR_post_status,
	expiry_date DCR_expiry_date,
	cross_ref DCR_cross_ref,
	cross_ref_module DCR_cross_ref_module,
	doc_type DCR_doc_type,
	FT_sup_code DCR_FT_sup_code,
	ibt DCR_IBT,
	ibt_status DCR_IBT_Status,
	--post_status DCR_PostStatus,
	--qty_ibt_grn DCR_GR_Qty, 
	DCR_C.line DCRC_line,
	DCR_C.Itemcode DCRC_Itemcode,
	DCR_C.Itemlink DCRC_Itemlink,
	DCR_C.Description DCRC_Description,
	DCR_C.Entry_Bulk DCRC_Entry_Bulk,
	DCR_C.BulkSize DCRC_BulkSize,
	DCR_C.BulkUM DCRC_BulkUM,
	DCR_C.UM DCRC_UM,
	DCR_C.Qty DCRC_Qty,
	DCR_C.Qty_Outstanding DCRC_Qty_Outstanding,
	Qty_Actual DCRC_Actual_Qty,
	Qty_Amend DCRC_Amend_Qty,
	DCR_C.UnitPrice DCRC_UnitPrice,
	DCR_C.NetUnitPrice DCRC_NetUnitPrice,
	DCR_C.NetAmount DCRC_NetAmount,
	DCR_C.PackSize DCRC_PackSize,
	DCR_C.Dept DCRC_Dept,
	DCR_C.SubDept DCRC_SubDept,
	DCR_C.Category DCRC_Category,
	DCR_C.Brand DCRC_Brand
  FROM
	`gch-prod-dwh01.backend.dc_req` DCR
  INNER JOIN
	`gch-prod-dwh01.backend.dc_req_child` DCR_C
  ON
	DCR.TRANS_GUID = DCR_C.TRANS_GUID),
  
  POCFT as (
	select 
	  POCFT.all_ft_po_refno POCFT_RefNo, 
	  POCFT.all_ft_po_line POCFT_LineNo, 
	  POCFT.all_ft_loc_code POCFT_Location,
	  POCFT.all_ft_itemcode POCFT_ItemCode, 
	  POCFT.all_ft_qty POCFT_Qty, 
	  POCFT.all_ft_case_qty POCFT_CaseQty,  
	  POCFT.all_ft_cross_ref  POCFT_CrossRef
	from `gch-prod-dwh01.backend.pochild_all_ft` POCFT 
  ),
  
Item as (select IM.itemcode, concat(IM.Dept,'-',H_DeptDesc) Dept, concat(IM.subDept,'-',H_SubDeptDesc) SubDept, concat(IM.Category,'-',H_Cat.H_CatDesc) Category
from `gch-prod-dwh01.backend.itemmaster` IM
inner join (select dept, deptdesc H_DeptDesc from `gch-prod-dwh01.gchbi.hierarchy` group by dept, deptdesc) H_Dept on IM.Dept = H_Dept.Dept
inner join (select subdept, subdeptdesc H_SubDeptDesc from `gch-prod-dwh01.gchbi.hierarchy` group by subdept, subdeptdesc) H_SubDept on IM.subDept = H_SubDept.subdept
inner join (select category, categorydesc H_CatDesc from `gch-prod-dwh01.gchbi.hierarchy` group by category, categorydesc) H_Cat on IM.category = H_Cat.category),

Banner_POM as (select branch_code, branch_name, Banner from `gch-prod-dwh01.daily_summary.Banner_Master`),

Banner_POCFT as (select branch_code, branch_name, Banner from `gch-prod-dwh01.daily_summary.Banner_Master`),

IM_BS as (select itemcode, branch, ads IM_BS_ads from `gch-prod-dwh01.backend.itemmaster_branch_stock`)

SELECT
  POM_RefNo,
  POCFT_CrossRef,
  CONCAT('Yr',FORMAT_DATE('%Y', POM_PODate)) as `Calendar Year`,
  CONCAT(FORMAT_DATE('%m', POM_PODate), '. ', FORMAT_DATE('%b', POM_PODate)) as `Calendar Month`,
  CASE
	WHEN EXTRACT(WEEK(MONDAY) FROM DATE_TRUNC(POM_PODate, YEAR)) = 1
	THEN CONCAT(EXTRACT(WEEK(MONDAY) FROM POM_PODate), '.', EXTRACT(YEAR FROM POM_PODate))
	ELSE CONCAT(EXTRACT(WEEK(MONDAY) FROM POM_PODate) + 1, '.', EXTRACT(YEAR FROM POM_PODate))
  END AS WeekYear,
  CONCAT(FORMAT_DATE('%m', POM_PODate), '. ', FORMAT_DATE('%d', POM_PODate)) as `Calendar Day Month`,
  POM_PODate,
  `Calendar Day Week`,
  POM_DeliveryDate,
  concat(LPAD(CAST(EXTRACT(MONTH from POM_DeliveryDate) as string),2,'0'),'.',FORMAT_DATE('%b', POM_DeliveryDate)) DeliveryMonth,
  concat('Wk',LPAD(cast(EXTRACT(WEEK FROM DATE_TRUNC(POM_DeliveryDate, WEEK(MONDAY))) as string),2,'0')) Calendar_WkNum,
  concat(LPAD(CAST(EXTRACT(DAYOFWEEK FROM DATE_SUB(POM_DeliveryDate, INTERVAL 1 DAY)) as string),2,'0'),'.',FORMAT_DATE('%a', POM_DeliveryDate)) Day_Week,
  POM_ExpiryDate,
  POM_BillStatus,
  POM_IssuedBy,
  concat(POM_Location," - ",Banner_POM.branch_name) POM_Location,
  Banner_POM.Banner POM_Banner,
  concat(POCFT_Location," - ",Banner_POCFT.branch_name) POCFT_Location,
  Banner_POCFT.Banner POCFT_Banner,
  POM_SCode,
  POM_SName,
  POM_Remark,
  POM_IBT,
  CASE
	WHEN POM_OrderType = 0 THEN '00-DSP/Con'
	WHEN POM_OrderType = 1 THEN '01-Flow-through'
	WHEN POM_OrderType = 9 THEN '09-NT-DSP'
	WHEN POM_OrderType = 99 THEN '99-NT'
  END POM_OrderType,
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
  POCFT_Qty,
  POCFT_CaseQty,
  POC_BulkQty,
  POC_UMBulk,
  POC_BQty,
  POC_UnitPrice,
  IM_BS_ads,
  POC_TotalPrice,
  GRM_RefNo,
  GRM_Location,
  GRM_DocDate,
  GRM_Code,
  GRC_PORefNo,
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
  DCR_RefNo,
  --DCR_PostStatus,
  DCR_IBT,
  DCR_IBT_Status,
  --DCR_GR_Qty,
  DCR_LocTo,
  DCR_DocDate,
  DCR_LocFrom,
  DCRC_line,
  DCRC_Itemcode,
  DCRC_Description,
  DCRC_Qty_Outstanding,
  DCRC_Qty,
  DCRC_Actual_Qty,
  DCRC_Amend_Qty,
  DCRC_BulkSize,
  DCRC_BulkUM,
  DCRC_PackSize,
  DCRC_UnitPrice,
  DCRC_NetAmount
FROM
  PO
LEFT OUTER JOIN
  POCFT
ON
  PO.POM_RefNo = POCFT.POCFT_RefNo
  AND PO.POC_Line = POCFT.POCFT_LineNo
  AND PO.POC_Itemcode = POCFT.POCFT_ItemCode
LEFT OUTER JOIN
  GR
ON
  PO.POM_RefNo = GR.GRC_PORefNo
  AND PO.POC_Itemcode = GR.GRC_Itemcode
  AND PO.POC_PriceType = GR.GRC_Price_Type
  --AND PO.POC_Line = GR.GRC_POLine
LEFT OUTER JOIN 
  DC_Req
ON
  POCFT.POCFT_RefNo = DC_Req.DCR_cross_ref
  and POCFT.POCFT_ItemCode = DC_Req.DCRC_Itemcode
  and POCFT.POCFT_Location = left(DC_Req.DCR_LocTo,4)

INNER JOIN Item on PO.POC_Itemcode = Item.itemcode
LEFT OUTER JOIN Banner_POM on PO.POM_Location = Banner_POM.branch_code
LEFT OUTER JOIN Banner_POCFT on POCFT.POCFT_Location = Banner_POCFT.branch_code
LEFT OUTER JOIN IM_BS on PO.POC_Itemcode = IM_BS.itemcode and PO.POM_Location = IM_BS.branch
  --where POM_RefNo = 'HQPO250107684'
  ;