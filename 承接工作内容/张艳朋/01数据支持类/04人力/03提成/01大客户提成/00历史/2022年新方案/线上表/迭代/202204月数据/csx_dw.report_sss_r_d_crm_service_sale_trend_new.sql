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
set last_1day_mon_fisrt_day =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');


--需要销售员年至今销售额，所以从1月1号开始
drop table if exists  csx_tmp.tmp_report_sss_r_d_crm_service_sale_trend_1;
create table csx_tmp.tmp_report_sss_r_d_crm_service_sale_trend_1
as
select
	a.*,
	a2.customer_name,
	a2.service_user_work_no,
	a2.service_user_name,
	a2.service_user_id
from
	(
	select 
		sdt,substr(sdt,1,6) smonth,
		customer_no,
		-- 各类型销售额
		sum(sales_value ) as customer_sales_value, 
		sum(case when business_type_code in('1') then sales_value else 0 end) as customer_ripei_sales_value,
		sum(case when business_type_code in('6') then sales_value else 0 end) as customer_bbc_sales_value,
		sum(case when business_type_code in('1','6') then sales_value else 0 end) as customer_ripei_bbc_sales_value,
		sum(case when business_type_code in('2') then sales_value else 0 end) as customer_fuli_sales_value,
		-- 各类型定价毛利额
		sum(profit) as customer_profit, 
		sum(case when business_type_code in('1') then profit else 0 end) as customer_ripei_profit,
		sum(case when business_type_code in('6') then profit else 0 end) as customer_bbc_profit,
		sum(case when business_type_code in('1','6') then profit else 0 end) as customer_ripei_bbc_profit,
		sum(case when business_type_code in('2') then profit else 0 end) as customer_fuli_profit,
		-- 各类型退货金额
		sum(case when return_flag='X' then sales_value else 0 end) as customer_refund_sales_value, 
		sum(case when business_type_code in('1') and return_flag='X' then sales_value else 0 end) as customer_refund_ripei_sales_value,
		sum(case when business_type_code in('6') and return_flag='X' then sales_value else 0 end) as customer_refund_bbc_sales_value,
		sum(case when business_type_code in('1','6') and return_flag='X' then sales_value else 0 end) as customer_refund_ripei_bbc_sales_value,
		sum(case when business_type_code in('2') and return_flag='X' then sales_value else 0 end) as customer_refund_fuli_sales_value	
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20220401' and sdt<='20220430'
		and channel_code in('1','7','9')
		and business_type_code in('1','2','6')
		and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
	group by 
		sdt,substr(sdt,1,6),customer_no,customer_name
	)a
	left join
		(
		select 
			month as smonth,customer_no,customer_name,work_no_new as work_no,sales_name_new as sales_name,sales_id_new as sales_id,
			rp_service_user_id as service_user_id,rp_service_user_work_no as service_user_work_no,rp_service_user_name as service_user_name
		from 
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where 
			month='202204'
		)a2 on a.customer_no=a2.customer_no and a.smonth=a2.smonth;


--刷新数据
insert overwrite table csx_tmp.report_sss_r_d_crm_service_sale_trend_new partition(sdt)
select
	concat_ws('&',cast(service_user_id as string),sdt) as biz_id,
	service_user_id as service_id,
	service_user_work_no as service_work_no,
	service_user_name as service_name,
	sdt as sales_date,
	sum(customer_sales_value) as sales_value,
	sum(customer_ripei_sales_value) as ripei_sales_value,
	sum(customer_bbc_sales_value) as bbc_sales_value,
	sum(customer_ripei_bbc_sales_value) as ripei_bbc_sales_value,
	sum(customer_fuli_sales_value) as fuli_sales_value,
	sum(customer_profit) as profit,
	sum(customer_ripei_profit) as ripei_profit,
	sum(customer_bbc_profit) as bbc_profit,
	sum(customer_ripei_bbc_profit) as ripei_bbc_profit,
	sum(customer_fuli_profit) as fuli_profit,
	sum(customer_refund_sales_value) as refund_sales_value,
	sum(customer_refund_ripei_sales_value) as refund_ripei_sales_value,
	sum(customer_refund_bbc_sales_value) as refund_bbc_sales_value,
	sum(customer_refund_ripei_bbc_sales_value) as refund_ripei_bbc_sales_value,
	sum(customer_refund_fuli_sales_value) as refund_fuli_sales_value,
	substr(sdt,1,6) as smonth,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time,
	sdt
from
	csx_tmp.tmp_report_sss_r_d_crm_service_sale_trend_1
where
	service_user_id is not null
	and sdt>='20220401' and sdt<='20220430'
group by 
	service_user_id,service_user_work_no,service_user_name,sdt	
;