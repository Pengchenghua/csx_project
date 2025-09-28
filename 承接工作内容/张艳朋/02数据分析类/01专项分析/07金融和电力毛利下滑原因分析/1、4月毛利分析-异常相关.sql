-- ------------------------------------------------------------
-- ----异常客户品类层级汇总数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_56;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_56 as 

select 
	substr(a.sdt,1,6) as month,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	a.customer_name,
	d.first_business_sale_date,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end) as if_zs_dc,
	nvl(sum(a.profit),0) as all_profit,
	nvl(sum(a.sale_amt),0) as all_sale_amt,
	nvl(sum(case when a.delivery_type_code=2 then a.profit end),0) as zs_profit,
	nvl(sum(case when a.delivery_type_code=2 then a.sale_amt end),0) as zs_sale_amt,
	nvl(sum(case when a.order_channel_code=6 then a.profit end),0) as tj_profit,
	nvl(sum(case when a.order_channel_code=6 then a.sale_amt end),0) as tj_sale_amt,
	nvl(sum(case when a.order_channel_code=4 then a.profit end),0) as fl_profit,
	nvl(sum(case when a.order_channel_code=4 then a.sale_amt end),0) as fl_sale_amt,
	nvl(sum(case when a.refund_order_flag=1 then a.profit end),0) as td_profit,
	nvl(sum(case when a.refund_order_flag=1 then a.sale_amt end),0) as td_sale_amt,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.profit end),0) as cq_profit,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.sale_amt end),0) as cq_sale_amt 
from 
	(select * 
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>='20230301' and sdt<'20230501' 
	and channel_code <> '2' and substr(customer_code, 1, 1) <> 'S' -- 只取大客户的数据
	and performance_region_code not in ('5','6','7','100') -- 只取省区数据
	) a 
	left join 
	-- 直送仓数据
	(select * 
	from csx_dim.csx_dim_shop 
	where sdt='current' 
	and shop_low_profit_flag=1 
	) b 
	on a.inventory_dc_code=b.shop_code 
	left join 
	-- 商品码表
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c 
	on a.goods_code=c.goods_code 
	left join 
	-- 客户首次签收日期
	(select * 
	from csx_dws.csx_dws_crm_customer_business_active_di 
	where sdt='current'
	) d 
	on a.customer_code=d.customer_code and a.business_type_code=d.business_type_code 
	left join 
	-- 客户行业数据
	(select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
	) e 
	on a.customer_code=e.customer_code 
group by 
	substr(a.sdt,1,6),
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	a.customer_name,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end),
	d.first_business_sale_date 
;

-- ------------------------------------------------------------
-- ----异常省区层级汇总数据
select 
	month as `月份`,
	performance_region_code as `大区编码`,
	performance_region_name as `大区名称`,
	performance_province_code as `省区编码`,
	performance_province_name as `省区名称`,
	performance_city_code as `城市编码`,
	performance_city_name as `城市名称`,
	business_type_code as `业务类型编码`,
	business_type_name as `业务类型`,
	classify_large_code as `管理大类编码`,
	classify_large_name as `管理大类名称`,
	classify_middle_code as `管理中类编码`,
	classify_middle_name as `管理中类名称`,
	customer_code as `客户编码`,
	customer_name as `客户名称`,
	first_business_sale_date as `此业务客户首次签收时间`,
	first_category_name as `一级客户分类`,
	second_category_name as `二级客户分类`,
	third_category_name as `三级客户分类`,
	inventory_dc_code as `仓`,
	if_zs_dc as `是否是直送仓`,
	all_profit as `总毛利额`,
	all_sale_amt as `总销售额`,
	zs_profit as `直送毛利额`,
	zs_sale_amt as `直送销售额`,
	tj_profit as `调价毛利额`,
	tj_sale_amt as `调价销售额`,
	fl_profit as `返利毛利额`,
	fl_sale_amt as `返利销售额`,
	td_profit as `退单毛利额`,
	td_sale_amt as `退单销售额`,
	cq_profit as `出清毛利额`,
	cq_sale_amt  as `出清销售额` 
from csx_analyse_tmp.csx_analyse_tmp_sale_detail_56 