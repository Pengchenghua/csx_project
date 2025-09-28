--财务 crm  销售结算 销售趋势
-- 时间从 当年的1月1日起,每天刷新近2月的销售数据

-- 切换tez计算引擎
SET hive.execution.engine=tez;
-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
-- 启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;

----昨天
set last_1day = regexp_replace(date_sub(current_date,1),'-','');
--昨天所在年第一天
set last_1day_year_first_day=regexp_replace(trunc(to_date(from_unixtime(UNIX_TIMESTAMP(
         regexp_replace(date_sub(current_date,1),'-','') ,'yyyyMMdd'))), 'YEAR'), '-', '');
--昨天所在月第一天
set last_1day_mon_fisrt_day =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
--昨天的上个月的第一天
set last_month_first_day =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');

set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');
set created_by='zhangyanpeng';

drop table if exists  csx_tmp.report_sss_r_m_crm_customer_sale_detail_0;
create table csx_tmp.report_sss_r_m_crm_customer_sale_detail_0
as
select 
	sdt,substr(sdt,1,6) year_month,
	customer_no,
	business_type_code,
	business_type_name,
	sum(sales_value) as sales_value, 
	sum(profit) as profit, 
	sum(case when return_flag='X' then sales_value else 0 end) as refund_sales_value	
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt>=${hiveconf:last_month_first_day} and sdt<=${hiveconf:last_1day}
	and channel_code in('1','7','9')
	and business_type_code in('1','2','6')
	and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
group by 
	sdt,substr(sdt,1,6),customer_no,business_type_code,business_type_name
;

drop table if exists  csx_tmp.report_sss_r_m_crm_customer_sale_detail_1;
create table csx_tmp.report_sss_r_m_crm_customer_sale_detail_1
as
select
	'1' as business_type_code,'日配业务' as business_type_name,
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,
	month,customer_id,customer_no,customer_name,sales_id_new as sales_id,work_no_new as work_no,sales_name_new as sales_name,
	first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
	rp_service_user_id_new as service_user_id,
	rp_service_user_work_no_new as service_user_work_no,
	rp_service_user_name_new as service_user_name
from 
	csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
where 
	month>=substr(${hiveconf:last_month_first_day},1,6)
	and customer_no !=''
	and rp_service_user_work_no_new is not null
union all
select
	'2' as business_type_code,'福利业务' as business_type_name,
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,
	month,customer_id,customer_no,customer_name,sales_id_new as sales_id,work_no_new as work_no,sales_name_new as sales_name,
	first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
	fl_service_user_id_new as service_user_id,
	fl_service_user_work_no_new as service_user_work_no,
	fl_service_user_name_new as service_user_name
from 
	csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
where 
	month>=substr(${hiveconf:last_month_first_day},1,6)
	and customer_no !=''
	and fl_service_user_work_no_new is not null
union all
select
	'6' as business_type_code,'BBC' as business_type_name,
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,
	month,customer_id,customer_no,customer_name,sales_id_new as sales_id,work_no_new as work_no,sales_name_new as sales_name,
	first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
	bbc_service_user_id_new as service_user_id,
	bbc_service_user_work_no_new as service_user_work_no,
	bbc_service_user_name_new as service_user_name
from 
	csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
where 
	month>=substr(${hiveconf:last_month_first_day},1,6)
	and customer_no !=''
	and bbc_service_user_work_no_new is not null
;

drop table if exists  csx_tmp.report_sss_r_m_crm_customer_sale_detail_2;
create table csx_tmp.report_sss_r_m_crm_customer_sale_detail_2
as
select
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,
	month,customer_id,customer_no,customer_name,sales_id_new as sales_id,work_no_new as work_no,sales_name_new as sales_name,
	first_supervisor_code,first_supervisor_work_no,first_supervisor_name
from 
	csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
where 
	month>=substr(${hiveconf:last_month_first_day},1,6)
	and customer_no !=''
;

--需要销售员年至今销售额，所以从1月1号开始
drop table if exists  csx_tmp.report_sss_r_m_crm_customer_sale_detail_3;
create table csx_tmp.report_sss_r_m_crm_customer_sale_detail_3
as
select
	a.*,
	c.customer_id,
	c.customer_name,
	c.sales_id,
	c.work_no,
	c.sales_name,
	b.service_user_id,
	b.service_user_work_no,
	b.service_user_name,
	c.first_supervisor_code,
	c.first_supervisor_work_no,
	c.first_supervisor_name,
	c.region_code,
	c.region_name,
	c.province_code,
	c.province_name,
	c.city_group_code,
	c.city_group_name
from
	(
	select 
		sdt,year_month,customer_no,business_type_code,business_type_name,sales_value,profit,refund_sales_value
	from 
		csx_tmp.report_sss_r_m_crm_customer_sale_detail_0
	)a
	left join
		(
		select
			business_type_code,business_type_name,region_code,region_name,province_code,province_name,city_group_code,city_group_name,
			month,customer_id,customer_no,customer_name,sales_id,work_no,sales_name,
			first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
			service_user_id,
			service_user_work_no,
			service_user_name
		from
			csx_tmp.report_sss_r_m_crm_customer_sale_detail_1
		)b on a.customer_no=b.customer_no and a.year_month=b.month and a.business_type_code=b.business_type_code
	left join
		(
		select distinct
			region_code,region_name,province_code,province_name,city_group_code,city_group_name,
			month,customer_id,customer_no,customer_name,sales_id,work_no,sales_name,
			first_supervisor_code,first_supervisor_work_no,first_supervisor_name
		from
			csx_tmp.report_sss_r_m_crm_customer_sale_detail_2
		)c on a.customer_no=c.customer_no and a.year_month=c.month
;


--刷新数据
insert overwrite table csx_tmp.report_sss_r_m_crm_customer_sale_detail partition(month)
select
	concat_ws('&',customer_no,sdt,business_type_code) as biz_id,
	coalesce(region_code,'') as region_code,
	coalesce(region_name,'') as region_name,
	coalesce(province_code,'') as province_code,
	coalesce(province_name,'') as province_name,
	coalesce(city_group_code,'') as city_group_code,
	coalesce(city_group_name,'') as city_group_name,
	coalesce(customer_id,'') as customer_id,
	customer_no,
	coalesce(customer_name,'') as customer_name,
	coalesce(sales_id,'') as sales_id,
	coalesce(work_no,'') as work_no,
	coalesce(sales_name,'') as sales_name,
	business_type_code,
	business_type_name,
	coalesce(service_user_id,'') as service_user_id,
	coalesce(service_user_work_no,'') as service_user_work_no,
	coalesce(service_user_name,'') as service_user_name,
	coalesce(first_supervisor_code,'') as first_supervisor_code,
	coalesce(first_supervisor_work_no,'') as first_supervisor_work_no,
	coalesce(first_supervisor_name,'') as first_supervisor_name,
	sdt as sales_date,
	sales_value,
	profit,
	refund_sales_value,
	year_month,
	${hiveconf:created_by} as create_by,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as create_time,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time,
	year_month as month
from
	csx_tmp.report_sss_r_m_crm_customer_sale_detail_3
where 
    customer_no is not null
	and customer_id is not null
;