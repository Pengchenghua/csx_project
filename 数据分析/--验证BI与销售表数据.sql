--验证BI与销售表数据
select mon,supplier_code,sum(bi_sale),sum(dw_sale) dw_sale ,sum(bi_sale)-sum(dw_sale) diff_sale from (
select substr(sdt,1,6)mon,supplier_code,supplier_name,sum(sale_amt) bi_sale,0 dw_sale  from csx_analyse.csx_analyse_bi_sale_detail_di where sdt>='20221201' and sdt<='20240301'  group by substr(sdt,1,6),supplier_code,supplier_name
union all 
select substr(sdt,1,6)mon,supplier_code,supplier_name,0 bi_sale,sum(sale_amt) dw_sale  from csx_dws.csx_dws_sale_detail_di where sdt>='20221201' and sdt<='20240301' group by substr(sdt,1,6),supplier_code,supplier_name
) a group by mon,supplier_code
 having sum(bi_sale)-sum(dw_sale) !=0