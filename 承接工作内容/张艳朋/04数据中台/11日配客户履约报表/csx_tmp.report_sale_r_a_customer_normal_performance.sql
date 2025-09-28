-- 切换tez计算引擎
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 昨日
set one_day_ago = regexp_replace(date_sub(current_date,1),'-','');

--本月初
set this_month_start_day =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--上月初
set last_month_start_day =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');	

-- 目标表
set target_table=csx_tmp.report_sale_r_a_customer_normal_performance;

with current_customer_normal_performance as 	
(	
select
	coalesce(b.sales_region_code,a.region_code) as region_code,
	coalesce(b.sales_region_name,a.region_name) as region_name,
	coalesce(b.province_code,a.province_code) as province_code,
	coalesce(b.province_name,a.province_name) as province_name,
	coalesce(b.city_group_code,a.city_group_code) as city_group_code,
	coalesce(b.city_group_name,a.city_group_name) as city_group_name,
	a.customer_id,
	coalesce(b.customer_no,'') as customer_no,
	a.customer_name,
	coalesce(b.work_no,a.work_no) as work_no,
	coalesce(b.sales_name,a.sales_name) as sales_name,
	regexp_replace(substr(a.sign_time,1,10),'-','') as sign_date,
	coalesce(c.normal_first_order_date,'') as normal_first_order_date,
	a.estimate_contract_amount,
	a.contract_cycle,
	coalesce(d.this_month_sales_value,'') as this_month_sales_value,
	coalesce(d.last_month_sales_value,'') as last_month_sales_value,
	if(d.this_month_sales_value is null and d.last_month_sales_value is null,'',coalesce(d.this_month_sales_value,0)+coalesce(d.last_month_sales_value,0)) as total_sales_value,
	d.csp_rate,
	coalesce(b.dev_source_code,0) as dev_source_code,
	coalesce(b.dev_source_name,'') as dev_source_name
from
	(
	select
		customer_id,business_number,customer_name,attribute,attribute_name,sales_id,work_no,sales_name,contract_cycle,
		business_stage,stage_desc,status,estimate_contract_amount,sign_time,city_group_code,city_group_name,province_code,province_name,region_code,region_name
	from
		csx_dw.ads_crm_r_m_business_customer
	where
		month=substr(${hiveconf:one_day_ago},1,6)
		and status=1
		and business_stage=5
		and attribute='1' -- 新客户属性（商机属性） 1：日配客户 2：福利客户 3：大宗贸易 4：M端 5：BBC 6：内购
		and regexp_replace(substr(sign_time,1,7),'-','') = substr(${hiveconf:last_month_start_day},1,6)
	) a 
	left join
		(
		select
			customer_id,customer_no,customer_name,sales_id,work_no,sales_name,province_code,province_name,sales_region_code,sales_region_name,city_group_code,city_group_name,
			dev_source_code,dev_source_name
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt=${hiveconf:one_day_ago}
		)b on b.customer_id=a.customer_id
	left join
		(
		select
			customer_no,customer_name,normal_first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = ${hiveconf:one_day_ago}
		)c on c.customer_no=b.customer_no
	left join
		(
		select 
			customer_no,
			sum(case when substr(sdt,1,6)=substr(${hiveconf:last_month_start_day},1,6) and business_type_code='1' then sales_value else null end) as last_month_sales_value,
			sum(case when substr(sdt,1,6)=substr(${hiveconf:this_month_start_day},1,6) and business_type_code='1' then sales_value else null end) as this_month_sales_value,
			sum(case when business_type_code='4' then sales_value else null end)/sum(sales_value) as csp_rate
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:last_month_start_day} and ${hiveconf:one_day_ago}
			and channel_code in ('1','7','9')
			and business_type_code in ('1','4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_no
		)d on d.customer_no=b.customer_no
)

insert overwrite table ${hiveconf:target_table} partition(sdt)	

select
	concat_ws('&',customer_id,${hiveconf:one_day_ago}) as biz_id,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	customer_id,
	customer_no,
	customer_name,
	work_no,
	sales_name,
	sign_date,
	normal_first_order_date,
	estimate_contract_amount,
	contract_cycle,
	this_month_sales_value,
	last_month_sales_value,
	total_sales_value,
	csp_rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	dev_source_code,
	dev_source_name,
	${hiveconf:one_day_ago} as sdt
from
	current_customer_normal_performance
;

--INVALIDATE METADATA csx_tmp.report_sale_r_a_customer_normal_performance;
	

/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sale_r_a_customer_normal_performance  上月签约日配业务客户表现情况

drop table if exists csx_tmp.report_sale_r_a_customer_normal_performance;
create table csx_tmp.report_sale_r_a_customer_normal_performance(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_group_code`                string              COMMENT    '城市编码',
`city_group_name`                string              COMMENT    '城市',
`customer_id`                    string              COMMENT    '客户ID',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`work_no`                        string              COMMENT    '业务员工号',
`sales_name`                     string              COMMENT    '业务员名称',
`sign_date`                      string              COMMENT    '签约日期',
`normal_first_order_date`        string              COMMENT    '日配首单日期',
`estimate_contract_amount`       decimal(15,6)       COMMENT    '签约金额',
`contract_cycle`                 string              COMMENT    '合同周期',
`this_month_sales_value`         decimal(15,6)       COMMENT    '本月履约额(日配)',
`last_month_sales_value`         decimal(15,6)       COMMENT    '上月履约额(日配)',
`total_sales_value`              decimal(15,6)       COMMENT    '总履约额(近俩月日配)',
`csp_rate`                       decimal(15,6)       COMMENT    '城市服务商履约额占比',
`update_time`                    string              COMMENT    '数据更新时间'

) COMMENT '上月签约日配业务客户表现情况'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	

--alter table csx_tmp.report_sale_r_a_customer_normal_performance add columns (dev_source_code int comment '开发来源编码',dev_source_name string comment '开发来源名称');

		
	