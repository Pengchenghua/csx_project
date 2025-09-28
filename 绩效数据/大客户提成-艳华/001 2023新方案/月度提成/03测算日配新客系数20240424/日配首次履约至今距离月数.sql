
select
	d.performance_region_name,     --  销售大区名称(业绩划分)
	d.performance_province_name,     --  销售归属省区名称
	d.performance_city_name,     --  城市组名称(业绩划分)	
	a.*
from
(
select
coalesce(a.customer_code,b.customer_code) as customer_code,
coalesce(a.customer_name,b.customer_name) as customer_name,
coalesce(a.business_type_name,b.business_type_name) as business_type_name,
coalesce(a.first_business_sale_date,b.first_business_sale_date) as first_business_sale_date,
coalesce(a.last_business_sale_date,b.last_business_sale_date) as last_business_sale_date,
coalesce(a.month_diff_4,b.month_diff_4) as month_diff_4,
coalesce(a.month_diff_3,b.month_diff_3) as month_diff_3,
coalesce(a.month_diff_2,b.month_diff_2) as month_diff_2
from
(
select customer_code,customer_name,business_type_name,
	first_business_sale_date,last_business_sale_date,
	-- 日配首次履约至今距离月数
	-- datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd')))) date_diff,
	cast(months_between('2024-10-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_4,
	cast(months_between('2024-11-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_3,
	cast(months_between('2024-12-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_2
from csx_dws.csx_dws_crm_customer_business_active_di
where sdt = 'current'
-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.项目供应商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
and business_type_code in('1')
and first_business_sale_date<>''
)a
full join
(
select customer_code,customer_name,'项目供应商' as business_type_name,
	first_business_sale_date,last_business_sale_date,
	-- 日配首次履约至今距离月数
	-- datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd')))) date_diff,
	cast(months_between('2024-10-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_4,
	cast(months_between('2024-11-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_3,
	cast(months_between('2024-12-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_2
from csx_dws.csx_dws_crm_customer_business_active_di
where sdt = 'current'
-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.项目供应商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
and business_type_code in('4')
and first_business_sale_date<>''
)b on a.customer_code=b.customer_code
)a
left join  
   (
	 select
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name,     --  城市组名称(业绩划分)	 
		customer_code,
		customer_name,
			sales_user_number,
	sales_user_name	
	 from  csx_dim.csx_dim_crm_customer_info 
	 where sdt='current'	       
	)d on d.customer_code=a.customer_code
;






select
coalesce(a.customer_code,b.customer_code) as customer_code,
coalesce(a.customer_name,b.customer_name) as customer_name,
coalesce(a.business_type_name,b.business_type_name) as business_type_name,
coalesce(a.first_business_sale_date,b.first_business_sale_date) as first_business_sale_date,
coalesce(a.last_business_sale_date,b.last_business_sale_date) as last_business_sale_date,
coalesce(a.month_diff_3,b.month_diff_3) as month_diff_3,
coalesce(a.month_diff_2,b.month_diff_2) as month_diff_2
from
(
select customer_code,customer_name,business_type_name,
	first_business_sale_date,last_business_sale_date,
	-- 日配首次履约至今距离月数
	-- datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd')))) date_diff,
	cast(months_between('2024-04-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_3,
	cast(months_between('2024-03-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_2
from csx_dws.csx_dws_crm_customer_business_active_di
where sdt = 'current'
-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.项目供应商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
and business_type_code in('1')
and first_business_sale_date<>''
)a
full join
(
select customer_code,customer_name,'项目供应商' as business_type_name,
	first_business_sale_date,last_business_sale_date,
	-- 日配首次履约至今距离月数
	-- datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd')))) date_diff,
	cast(months_between('2024-04-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_3,
	cast(months_between('2024-03-01',trunc(to_date(from_unixtime(unix_timestamp(first_business_sale_date,'yyyyMMdd'))),'MM'))as int) as month_diff_2
from csx_dws.csx_dws_crm_customer_business_active_di
where sdt = 'current'
-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.项目供应商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
and business_type_code in('4')
and first_business_sale_date<>''
)b on a.customer_code=b.customer_code









