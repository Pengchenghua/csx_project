--OEM绩效数据源
with tmp_goods_info as 
(select goods_code,
       goods_bar_code,
       goods_name,
       unit_name,
       brand_name,
       standard,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       division_code,
       division_name,
       purchase_group_code,
       purchase_group_name,
       category_small_code,
       csx_purchase_level_code,
       csx_purchase_level_name,  -- 1-一般商品、2-全国商品、3-OEM商品、4-空
       is_factory_goods_flag
from csx_dim.csx_dim_basic_goods
where sdt='current'
),

sale_mclass_goods as
(
select 
	
	-- a.performance_region_name,
	-- a.performance_province_name,	
	-- a.performance_city_name,
	business_type_name,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    classify_small_name,
	a.goods_code,
	b.goods_bar_code,
	b.goods_name,
	b.csx_purchase_level_code,	
    sum(a.sale_qty) sale_qty,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit	
from 
(
select  
    a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	business_type_name,
	a.goods_code,
	a.sale_qty,
	a.sale_amt,
	a.sale_cost,
	a.profit,
	direct_delivery_type
from csx_dws.csx_dws_sale_detail_di a 
where 1=1
and sdt>='20251001' and sdt<='20251031'
and business_type_code in ('1','2','6','9','10')
)a 
join tmp_goods_info b on a.goods_code=b.goods_code
-- 直送类型 详细履约模式的码表
left join 
(
select `code`,name,extra
from csx_dim.csx_dim_basic_topic_dict_df
where parent_code = 'direct_delivery_type'
)a2 on cast(a.direct_delivery_type as string)=a2.`code`
where 1=1
  and b.csx_purchase_level_code='03'
group by business_type_name,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
	a.goods_code,
	b.goods_bar_code,
	b.goods_name,
	b.csx_purchase_level_code,
	classify_small_name
)

select business_type_name,
  classify_large_name,
  classify_middle_name,
  classify_small_name,
  goods_code,
  goods_bar_code,
  goods_name,
  sum(sale_qty) sale_qty,  
  sum(sale_amt) sale_amt,
  sum(profit)  profit
from sale_mclass_goods
group by 
  classify_large_name,
  classify_middle_name,
  classify_small_name,
  goods_code,
  goods_bar_code,
  goods_name 
,business_type_name;



-- 周转天数

-- month_of_year `月份`,
-- d.classify_large_name   as `管理大类名称`,
-- d.classify_middle_code	as	`管理中类`,
-- d.classify_middle_name	as	`管理中类名称`,
-- a.goods_code as `商品编码`,
-- d.goods_name as `商品名称`, 


with tmp_goods_info as 
(select goods_code,
       goods_bar_code,
       goods_name,
       unit_name,
       brand_name,
       standard,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       division_code,
       division_name,
       purchase_group_code,
       purchase_group_name,
       category_small_code,
       csx_purchase_level_code,
       csx_purchase_level_name,  -- 1-一般商品、2-全国商品、3-OEM商品、4-空
       is_factory_goods_flag
from csx_dim.csx_dim_basic_goods
where sdt='current'
)
-- 周转
select 
month_of_year `月份`,
d.classify_large_name   as `管理大类名称`,
d.classify_middle_code	as	`管理中类`,
d.classify_middle_name	as	`管理中类名称`,
a.goods_code as `商品编码`,
d.goods_name as `商品名称`, 
sum(nearly30days_amt_no_tax)	as	`不含税近30天累计库存额`,
sum(nearly30days_sale_cost_no_tax)	as	`不含税近30天累计销售出库成本`,
sum(nearly30days_amt_no_tax)/ sum(nearly30days_sale_cost_no_tax) as `近30周转`
from 
 csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new a 
join  tmp_goods_info d on a.goods_code=d.goods_code
join 
(select distinct month_of_year, 
                month_end 
    from csx_dim.csx_dim_basic_date 
        where calday=regexp_replace('${edate}','-','') 
) b on a.sdt=b.month_end
join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current'
 ) c on a.dc_area_code=c.dc_code
 where csx_purchase_level_code='03'
 group by 
month_of_year  ,
d.classify_large_name   ,
d.classify_middle_code	,
d.classify_middle_name	,
a.goods_code ,
d.goods_name
; 