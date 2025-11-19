
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
)select * from  tmp_goods_info  where csx_purchase_level_code='03'
;



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
performance_region_name `大区名称`,
performance_province_name `省区`,
performance_city_name `城市`,
dc_area_code `DC编码`,
dc_area_name `DC名称`,
d.classify_large_name   as `管理大类名称`,
d.classify_middle_code	as	`管理中类`,
d.classify_middle_name	as	`管理中类名称`,
a.goods_code as `商品编码`,
d.goods_name as `商品名称`, 
sum(case when sdt='20251114' then  nearly30days_sale_cost  end ) `近30销售成本`,
sum(case when sdt='20251114' then  nearly30days_amt_no_tax end )	as	`不含税近30天累计库存额`,
sum(case when sdt='20251114' then  nearly30days_sale_cost_no_tax end )	as	`不含税近30天累计销售出库成本`,
sum(case when sdt='20251114' then  nearly30days_amt_no_tax end )/ sum(case when sdt='20251114' then  nearly30days_sale_cost_no_tax end ) as `近30周转`,
sum(case when sdt='20251114' then  stock_amt end ) `库存金额`,
sum(case when sdt='20251114' then  stock_qty end ) `库存数量`,
sum(case when sdt='20251107' then nearly30days_sale_cost  end ) `上期近30销售成本`,
sum(case when sdt='20251107' then nearly30days_amt_no_tax end )	as	`上期不含税近30天累计库存额`,
sum(case when sdt='20251107' then nearly30days_sale_cost_no_tax end )	as	`上期不含税近30天累计销售出库成本`,
sum(case when sdt='20251107' then nearly30days_amt_no_tax end )/ sum(case when sdt='20251114' then nearly30days_sale_cost_no_tax end ) as `上期近30周转`
from 
   csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new a 
join  tmp_goods_info d on a.goods_code=d.goods_code
join 
(select distinct month_of_year, 
                month_end ,
                calday
    from csx_dim.csx_dim_basic_date 
        where calday in ('20251114','20251107')
) b on a.sdt=b.calday
join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current'
 ) c on a.dc_area_code=c.dc_code
 where csx_purchase_level_code='03'
 group by 

d.classify_large_name   ,
d.classify_middle_code	,
d.classify_middle_name	,
a.goods_code ,
d.goods_name,
performance_region_name,
performance_province_name ,
performance_city_name ,
dc_area_code ,
dc_area_name 
; 