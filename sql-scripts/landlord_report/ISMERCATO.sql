-- Panda Data
select 
CONCAT('ISMERCATO_', FORMAT_DATE('%Y%m%d', PCD.BizDate)) as EOD_ID,
'ISMERCATO' as OUTLET_NAME,
'ISMERCATO_001' as POS_NAME,
FORMAT_DATE('%d-%m-%Y', PCD.BizDate) as EOD_DATE,
round(sum(PCD.Amount), 2)as TOTAL_SALES,
-- round(sum(PCD.sumgrosssales),2) as GROSS_SALES,
round(sum(PCD.Amount) + sum(PCD.sumdisc), 2) as GROSS_SALES,
count(DISTINCT(RefNo)) as TRAN_COUNT,
0 as VOID_TRAN_COUNT,
0.00 as SERVICE_CHARGE,
round(sum(PCD.sumdisc),2) as DISCOUNT_AMT, 
0.00 as VAT_TAX_AMT,
0.00 as SVC_TAX_AMT,
0.00 as ROUNDING_ADJ
from `gch-prod-dwh01.gchbi.poschild_d` PCD
left outer join (select Itemcode, Description, dept 
from `gch-prod-dwh01.backend.itemmaster` IM) H_IM on PCD.itemcode = H_IM.Itemcode
left outer join `gch-prod-dwh01.backend_new.cp_set_branch` NCSB
ON PCD.Location = NCSB.branch_code
where H_IM.dept NOT LIKE '7%' and H_IM.dept NOT LIKE '8%' AND
PCD.BizDate = cur_date AND PCD.Location = '1079'
group by PCD.BizDate, PCD.Location
