drop table csx_analyse_tmp.csx_analyse_tmp_1; 
create table csx_analyse_tmp.csx_analyse_tmp_1 as 
select
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	b.location_code,  -- 地点编码
	c.shop_name as location_name,  -- 地点名称
	-- a.product_id,  -- 市调商品id	
	a.customer_code,  -- 客户号
	e.customer_name,  -- 客户名称
	b.product_code,
	--b.product_name,
	regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 	
	d.standard,
	d.unit_name,
	a.price_date,  -- 市调日期
	-- a.source_type_code,  -- 市调地点类型一级:4:一批,5:二批,6:终端
	-- a.source_type_two_level_code,  -- 市调地点类型二级:1:一批批发市场,2:一批网站,3:二批批发市场,4:二批网站
	(case when a.source_type_code=2 then '网站' 
		  when a.source_type_code=3 then '批发市场' 
		  when a.source_type_code=4 then '一批' 
		  when a.source_type_code=5 then '二批' 
		  when a.source_type_code=6 then '终端' end) as market_source_type_name,

	(case when a.source_type_two_level_code=1 then '一批批发市场' 
		  when a.source_type_two_level_code=2 then '一批网站' 
		  when a.source_type_two_level_code=3 then '二批批发市场' 
		  when a.source_type_two_level_code=4 then '二批网站' end) as source_type_two_level_name,		  
	a.market_code,  -- 市调对象编码
	a.market_name,  -- 市调对象名称
	a.price,  -- 市调价格
	a.min_price,  -- 最低价
	a.max_price,  -- 最高价
	a.market_price_wave,  -- 市调价波动=（当天市调价-最近一次市调价）/最近一次市调价
	a.estimated_pricing_gross_margin,  -- 预估定价毛利率=（市调价-库存平均价）/市调价
	a.price_begin_time,  -- 生效开始时间
	a.price_end_time,  -- 生效结束时间
	case 
	when a.source=0 then '市调导入'
	when a.source=1 then '小程序'
	when a.source=2 then '小程序-pc端'
	when a.source=3 then '线上网站市调'
	when a.source=4 then '通用市调系统生成'
	when a.source=5 then '市调图片导入'
	when a.source=6 then '系统自动爬取'	
	when a.source=7 then '市调价格录入'	
	else a.source end as source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调 4 通用市调系统生成
	-- a.status,  -- 状态(1有效 0无效)
	regexp_replace(a.remark,'\n|\t|\r|\,|\"|\\\\n','') as remark,  -- 备注
	-- a.create_by,  -- 创建人
	-- a.create_time,  -- 创建时间
	a.update_by,  -- 更新人
	a.update_time,  -- 更新时间
	-- a.sdt  -- 创建时间分区{\"FORMAT\":\"yyyymmdd\"}
	b.one_product_category_code,
	b.one_product_category_name,
	b.two_product_category_code,
	b.two_product_category_name,
	b.three_product_category_code,
	b.three_product_category_name,
	e.customer_type_code
from 
	(
	select *
	from 
		(
		select *,
			row_number()over(partition by customer_code,goods_id order by price_date desc,create_time desc) as rn
		-- from csx_dwd.csx_dwd_price_market_customer_research_price_di
		from csx_dwd.csx_dwd_price_market_customer_research_price_effective_di
		where status=1
		and substr(price_begin_time,1,10)<=current_date
		and substr(price_end_time,1,10)>=current_date
		and shipper_code='YHCSX'
		-- and price_date>='2023-11-01'
		)a
		-- where rn=1
	)a
		left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
				where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')) b on a.goods_id=b.id 
		left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
left join 
(
	select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
	-- and customer_type_code=4
	and shipper_code='YHCSX'
) e on a.customer_code=e.customer_code 		
where c.performance_region_name in('华南大区','华西大区','华北大区','华东大区');



select
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,  -- 地点编码
	count_cust,
	count_sku,
	
	count_cust_sddr,
	count_sku_sddr,	
	count_cust_sddr/count_cust as count_cust_sddr_zb,
	count_sku_sddr/ count_sku as count_sku_sddr_zb,

	count_cust_app,
	count_sku_app,
	count_cust_app/count_cust as count_cust_app_zb,
	count_sku_app/ count_sku as count_sku_app_zb,
	
	count_cust_apppc,
	count_sku_apppc,
	count_cust_apppc/count_cust as count_cust_apppc_zb,
	count_sku_apppc/ count_sku as count_sku_apppc_zb,
	
	count_cust_xswzsd,
	count_sku_xswzsd,
	count_cust_xswzsd/count_cust as count_cust_xswzsd_zb,
	count_sku_xswzsd/ count_sku as count_sku_xswzsd_zb,
	
	count_cust_tysdxtsc,
	count_sku_tysdxtsc,
	count_cust_tysdxtsc/count_cust as count_cust_tysdxtsc_zb,
	count_sku_tysdxtsc/ count_sku as count_sku_tysdxtsc_zb,
	
	count_cust_sdtpdr,
	count_sku_sdtpdr,
	count_cust_sdtpdr/count_cust as count_cust_sdtpdr_zb,
	count_sku_sdtpdr/ count_sku as count_sku_sdtpdr_zb,
	
	count_cust_xtzdpq,
	count_sku_xtzdpq,
	count_cust_xtzdpq/count_cust as count_cust_xtzdpq_zb,
	count_sku_xtzdpq/ count_sku as count_sku_xtzdpq_zb,
	
	count_cust_sdjgdr,
	count_sku_sdjgdr,	
	count_cust_sdjgdr/count_cust as count_cust_sdjgdr_zb,
	count_sku_sdjgdr/ count_sku as count_sku_sdjgdr_zb,
	
	count_cust_null,
	count_sku_null,
	count_cust_null/count_cust as count_cust_null_zb,
	count_sku_null/ count_sku as count_sku_null_zb	
from
(
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,  -- 地点编码
	count(distinct customer_code) as count_cust,
	count( product_code) as count_sku,
	
	count(distinct case when source='市调导入' then customer_code end) as count_cust_sddr,
	count( case when source='市调导入' then product_code end) as count_sku_sddr,	

	count(distinct case when source='小程序' then customer_code end) as count_cust_app,
	count( case when source='小程序' then product_code end) as count_sku_app,
	
	count(distinct case when source='小程序-pc端' then customer_code end) as count_cust_apppc,
	count( case when source='小程序-pc端' then product_code end) as count_sku_apppc,

	count(distinct case when source='线上网站市调' then customer_code end) as count_cust_xswzsd,
	count( case when source='线上网站市调' then product_code end) as count_sku_xswzsd,

	count(distinct case when source='通用市调系统生成' then customer_code end) as count_cust_tysdxtsc,
	count( case when source='通用市调系统生成' then product_code end) as count_sku_tysdxtsc,

	count(distinct case when source='市调图片导入' then customer_code end) as count_cust_sdtpdr,
	count( case when source='市调图片导入' then product_code end) as count_sku_sdtpdr,

	count(distinct case when source='系统自动爬取' then customer_code end) as count_cust_xtzdpq,
	count( case when source='系统自动爬取' then product_code end) as count_sku_xtzdpq,

	count(distinct case when source='市调价格录入' then customer_code end) as count_cust_sdjgdr,
	count( case when source='市调价格录入' then product_code end) as count_sku_sdjgdr,	

	count(distinct case when source is NULL then customer_code end) as count_cust_null,
	count( case when source is NULL then product_code end) as count_sku_null	
from csx_analyse_tmp.csx_analyse_tmp_1
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code
)a ;


select *
from csx_analyse_tmp.csx_analyse_tmp_1









select 
source,
count(1) aa
from csx_analyse_tmp.csx_analyse_tmp_1
group by source

















