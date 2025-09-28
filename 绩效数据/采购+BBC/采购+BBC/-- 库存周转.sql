-- 库存周转
select 
month_of_year `月份`,
-- performance_region_code	    as	`业绩归属大区编码`,
-- performance_region_name	    as	`业绩归属大区名称`,
-- performance_province_code	as	`绩效归属省区编码`,
-- performance_province_name	as	`绩效归属省区名称`,
-- performance_city_code	    as	`绩效归属城市编码`,
-- performance_city_name	    as	`绩效归属城市名称`,
classify_large_name     as `管理大类名称`,
classify_middle_code	as	`管理中类`,
classify_middle_name	as	`管理中类名称`,
sum(nearly30days_stock_amt)	as	`近30天累计库存额`,
sum(nearly30days_cost)	as	`近30天成本`,
-- sum(nearly30days_transfer)	as	`30天周转天数`,
--sum(out_nearly30days_province_transfer_cost) `跨省区出库成本`,
sum(nearly30days_stock_amt)/ sum(nearly30days_cost) as `近30周转`
from  csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new a 
join 
(select distinct month_of_year, month_end from csx_dim.csx_dim_basic_date where calday>='20230101' ) b on a.sdt=b.month_end
 join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.dc_area_code=c.dc_code
 group by 
month_of_year  ,
-- performance_region_code	 ,
-- performance_region_name	 ,
-- performance_province_code,
-- performance_province_name,
-- performance_city_code	 ,
-- performance_city_name	 ,
classify_large_name     ,
classify_middle_code	,
classify_middle_name	