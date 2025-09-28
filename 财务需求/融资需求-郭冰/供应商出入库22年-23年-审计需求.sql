select 
substr(sdt,1,6),
company_code
,company_name
,province_code
,province_name
,supplier_code
,dc_code
,dc_name
,supplier_name
,supplier_tax_code
,supplier_purchase_level_name
,sum(receive_amt) receive_amt
,sum(shipped_amt) shipped_amt
,sum(no_tax_receive_amt) no_tax_receive_amt
,sum(no_tax_return_amt)no_tax_return_amt
,supplier_classify_name
,valuation_category_name

from  csx_analyse.csx_analyse_fr_scm_supplier_in_out_report_di  where sdt>='20220101' and sdt<='20231231'
group by substr(sdt,1,6),
company_code
,company_name
,province_code
,province_name
,supplier_code
,dc_code
,dc_name
,supplier_name
,supplier_tax_code
,supplier_classify_name
,valuation_category_name
,supplier_purchase_level_name