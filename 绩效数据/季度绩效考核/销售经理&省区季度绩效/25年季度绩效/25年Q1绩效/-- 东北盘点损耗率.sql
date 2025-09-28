-- 东北盘点损耗率
with post_data as 	(
select 
	a.create_time,   --创建时间 
	a.wms_order_time,   --订单时间
	a.posting_time,   --记账日期
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	a.location_name,   --DC名称
	a.goods_code,   --商品编码
	(case when direction_flag ='-'  then -1*a.qty else a.qty end) as pd_qty,   --盘点数量
	(case when direction_flag ='-'  then -1*a.amt_no_tax else a.amt_no_tax end) as pd_amt_no_tax,   --盘点不含税金额
	(case when direction_flag ='-'  then -1*a.amt else a.amt end) as pd_amt,   --盘点含税金额
	0 loss_qty,
	0 loss_amt_no_tax,
	0 loss_amt
from
		-- csx_ods.source_cas_r_d_accounting_credential_item 
		-- csx_ods.csx_ods_csx_b2b_accounting_accounting_credential_item_di
         csx_dws.csx_dws_cas_credential_detail_di a
	where 
		sdt>='20250101' -- 过账日期
        and sdt<='20250331'
		-- and posting_time >= add_months(trunc(date_sub(current_date,0),'MM'),-1)
		-- and posting_time < trunc(date_sub(current_date,0),'MM')
		and substr(move_type_code,1,3) in ('115','116')   -- 115 盘盈 116 盘亏 117 报损
		and amt_no_tax<>'0'	
	--	and substr(company_code,1,1)<>'C' -- 20221101新加 公司代码 限制不为城市服务商
	    and company_code='2312'
union all 
select 
	a.create_time,   --创建时间 
	a.wms_order_time,   --订单时间
	a.posting_time,   --记账日期
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	a.location_name,   --DC名称
	a.goods_code,   --商品编码
	0 pd_qty,
	0 pd_amt_no_tax,
	0 pd_amt,
	(case when direction_flag ='-'  then -1*a.qty else a.qty end) as loss_qty,   --盘点数量
	(case when direction_flag ='-'  then -1*a.amt_no_tax else a.amt_no_tax end) as loss_amt_no_tax,   --盘点不含税金额
	(case when direction_flag ='-'  then -1*a.amt else a.amt end) as loss_amt   --盘点含税金额
from
		-- csx_ods.source_cas_r_d_accounting_credential_item 
		-- csx_ods.csx_ods_csx_b2b_accounting_accounting_credential_item_di
         csx_dws.csx_dws_cas_credential_detail_di a
	where 
		sdt>='20250101' -- 过账日期
        and sdt<'20250401'
		-- and posting_time >= add_months(trunc(date_sub(current_date,0),'MM'),-1)
		-- and posting_time < trunc(date_sub(current_date,0),'MM')
		and substr(move_type_code,1,3) in ('117')   -- 115 盘盈 116 盘亏 117 报损
		and amt_no_tax<>'0'	
	--	and substr(company_code,1,1)<>'C' -- 20221101新加 公司代码 限制不为城市服务商
	    and company_code='2312'
	),
-- 销售额
sale as 
(select performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        company_code,
		company_name,
        inventory_dc_code,
        inventory_dc_name,
        classify_large_code,
        classify_large_name,
        classify_middle_name,
        sale_qty,
        sale_amt,
        sale_amt_no_tax

from     csx_dws.csx_dws_sale_detail_di 
where sdt>='20250101' 
    and sdt<='20250331'
    and company_code='2312'
)
select 
        performance_province_name,
        performance_city_name,
        company_code,
		company_name,
        inventory_dc_code,
		inventory_dc_name,
		division_name,
        classify_large_name,
        classify_middle_name,
		sum(sale_qty) as sale_qty,
        sum(sale_amt) as sale_amt,
        sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(pd_qty) pd_qty,						-- 盘点数量
		sum(pd_amt_no_tax) pd_amt_no_tax,	--盘点未税金额
		sum(pd_amt) pd_amt,			-- 盘点金额
		sum(loss_qty) as loss_qty,   --报损数量
		sum(loss_amt_no_tax) as loss_amt_no_tax,   --报损不含税金额
		sum(loss_amt) as loss_amt,   --报损含税金额
		sum(pd_amt_no_tax+loss_amt_no_tax)/sum(sale_amt_no_tax) total_loss_amt,
		sum(pd_amt_no_tax+loss_amt_no_tax)/sum(sale_amt_no_tax) as loss_rate
from
(	
select 
        performance_province_name,
        performance_city_name,
        company_code,
		company_name,
        inventory_dc_code,
		inventory_dc_name,
		if(classify_large_code in ('B01','B02','B03'),'生鲜','食百') division_name,
        classify_large_name,
        classify_middle_name,
        sale_qty,
        sale_amt,
        sale_amt_no_tax,
		0  as pd_qty,						-- 盘点数量
	    0  as pd_amt_no_tax,	--盘点未税金额
	    0  as pd_amt,			-- 盘点金额
	    0  as loss_qty,   --报损数量
	    0  as loss_amt_no_tax,   --报损不含税金额
	    0  as loss_amt   --报损含税金额
from sale 
union all 
select performance_province_name,
	performance_city_name,
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	shop_name,
	if(classify_large_code in ('B01','B02','B03'),'生鲜','食百') division_name,
	classify_large_name,
	classify_middle_name,
	0 as sale_qty,
    0 as sale_amt,
    0 as sale_amt_no_tax,
	sum(pd_qty) pd_qty,						-- 盘点数量
	sum(pd_amt_no_tax) pd_amt_no_tax,	--盘点未税金额
	sum(pd_amt) pd_amt,			-- 盘点金额
	sum(loss_qty) as loss_qty,   --报损数量
	sum(loss_amt_no_tax) as loss_amt_no_tax,   --报损不含税金额
	sum(loss_amt) as loss_amt   --报损含税金额
 from post_data a 
left join 
		(
		select 
			shop_code,
			shop_name,
			performance_province_code,
        	performance_province_name,
        	performance_city_code,
        	performance_city_name
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m
			csx_dim.csx_dim_shop
		where 
			sdt = 'current'
		) b on b.shop_code=a.location_code
left join 
		(
		select 
			*  
		from  
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where sdt = 'current'
		)c on c.goods_code = a.goods_code
	group by performance_province_name,
	performance_city_name,
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	shop_name,
	classify_large_name,
	classify_middle_name,
	if(classify_large_code in ('B01','B02','B03'),'生鲜','食百') 
) a 

GROUP BY performance_province_name,
        performance_city_name,
        company_code,
		company_name,
        inventory_dc_code,
		inventory_dc_name,
		division_name,
        classify_large_name,
        classify_middle_name
	