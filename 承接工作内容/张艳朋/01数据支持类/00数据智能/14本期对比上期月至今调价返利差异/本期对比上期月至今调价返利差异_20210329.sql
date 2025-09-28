-- 昨天
set current_day ='20210327';

-- 上个月的昨天日期
set last_current_day='20210227'; 


set current_start_day ='20210301';

-- 1个月前第一天
set last_start_day ='20210201';

select 
    region_code,
    region_name,
    province_code,
    province_name,
	business_type_name,
    sum(case when sdt >= ${hiveconf:current_start_day} then sales_value else 0 end) as current_sales_value,
    sum(case when sdt >= ${hiveconf:current_start_day} then profit else 0 end) as current_profit,
	sum(case when sdt >= ${hiveconf:current_start_day} and sales_type <>'fanli' then sales_value else 0 end) as current_sales_value2,
    sum(case when sdt >= ${hiveconf:current_start_day} and sales_type <>'fanli' then profit else 0 end) as current_profit2,
    sum(case when sdt <= ${hiveconf:last_current_day} then sales_value else 0 end) as last_sales_value,--上期
    sum(case when sdt <= ${hiveconf:last_current_day} then profit else 0 end) as last_profit,
    sum(case when sdt <= ${hiveconf:last_current_day} and sales_type <>'fanli' then sales_value else 0 end) as last_sales_value2,--上期
    sum(case when sdt <= ${hiveconf:last_current_day} and sales_type <>'fanli' then profit else 0 end) as last_profit2
  from csx_dw.dws_sale_r_d_detail
  where sdt >= ${hiveconf:last_start_day} and sdt <= ${hiveconf:current_day} --sales_type = 'fanli'
    AND channel_code in ('1','7','9')
  group by region_code,
    region_name,
    province_code,
    province_name,
	business_type_name;