-- job名称
set mapred.job.name=report_crm_w_a_customer_service_manager_info_new;
-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- 来源表
set source_table_name = csx_dw.dws_crm_w_a_customer_sales_link;
-- 目标表
set target_table_name = csx_dw.report_crm_w_a_customer_service_manager_info_new;
-- 昨天日期
set one_day_ago = regexp_replace(date_sub(current_date, 1), '-', '');
-- 上月1日
set last_month_1_day = regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');


insert overwrite table ${hiveconf:target_table_name} partition (sdt)

select
	a.sales_province_code, 
	a.sales_province_name, 
	a.sales_city_code, 
	a.sales_city_name, 
	a.channel_code, 
	a.channel_name,
	a.customer_no, 
	a.customer_name, 
	a.work_no, 
	a.sales_name, 
	if(b.user_id is not null, '是', '否') as is_part_time_service_manager,
	a1.service_user_work_no, 
	a1.service_user_name, 
	case when c.customer_no is not null then '是' else '否' end is_sale,
	case when d.customer_no is not null then '是' else '否' end is_overdue,
	if(b.user_id is not null,0.3,if(a1.service_user_work_no<>'',0.7,1)) sales_sale_rate,
	if(b.user_id is not null,0.5,if(a1.service_user_work_no<>'',0.5,1)) sales_profit_rate,
	if(a1.service_user_work_no<>'',0.3,0) service_user_sale_rate,
	if(a1.service_user_work_no<>'',0.5,0) service_user_profit_rate,
	${hiveconf:one_day_ago} as sdt
from
	(
	select 
		channel_code,
		channel_name,
		province_code as sales_province_code,
		province_name as sales_province_name,
		city_group_code as sales_city_code,
		city_group_name as sales_city_name,
		customer_no,
		customer_name,
		first_category_name,
		second_category_name,
		third_category_name,
		sales_id,
		work_no,
		sales_name,
		first_sign_time,
		sign_time,
		contact_person,
		contact_phone
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='current' 
		--and channel_code in('1','7')
	)a
	--关联服务管家
	left join		
		(  
		select 
			customer_no,
			concat_ws(';', collect_list(service_user_work_no)) as service_user_work_no,
			concat_ws(';', collect_list(service_user_name)) as service_user_name
		from 
			(
			select 
				distinct customer_no,service_user_work_no,service_user_name
			from 
				${hiveconf:source_table_name} 
			where 
				sdt = 'current' 
				and is_additional_info = 1 
				and service_user_id <> 0
			)a
		group by 
			customer_no
		)a1 on a1.customer_no=a.customer_no
	--销售员是否兼职服务管家
	left join 
		(
		select 
			user_id
		from 
			csx_ods.source_uc_w_a_user_position
		where 
			sdt = ${hiveconf:one_day_ago} 
			and user_position = 'CUSTOMER_SERVICE_MANAGER'
		group by 
			user_id
		)b on a.sales_id = b.user_id
	--上月1日至今有销售
	left join
		(
		select 
			customer_no,first_order_date,last_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current'
			and last_order_date>=${hiveconf:last_month_1_day}
		)c on a.customer_no = c.customer_no
	--至今有逾期
	left join
		(
		select 
			customer_code as customer_no
		from 
			csx_dw.dws_sss_r_d_customer_settle_detail
		where 
			sdt = ${hiveconf:one_day_ago} 
			and overdue_amount >0
		group by 
			customer_code
		)d on a.customer_no = d.customer_no
;


--INVALIDATE METADATA csx_dw.report_crm_w_a_customer_service_manager_info_new;
	

/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.report_crm_w_a_customer_service_manager_info_new

drop table if exists csx_dw.report_crm_w_a_customer_service_manager_info_new;
create table csx_dw.report_crm_w_a_customer_service_manager_info_new(
`sales_province_code`            string              COMMENT    '销售省区编码',
`sales_province_name`            string              COMMENT    '销售省区名称',
`sales_city_code`                string              COMMENT    '销售城市编码',
`sales_city_name`                string              COMMENT    '销售城市名称',
`channel_code`                   string              COMMENT    '渠道编号',
`channel_name`                   string              COMMENT    '渠道名称',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`work_no`                        string              COMMENT    '业务员工号',
`sales_name`                     string              COMMENT    '业务员名称',
`is_part_time_service_manager`   string              COMMENT    '主业务员是否兼职服务管家',
`service_user_work_no`           string              COMMENT    '服务管家工号',
`service_user_name`              string              COMMENT    '服务管家名称',
`is_sale`                        string              COMMENT    '是否有销售',
`is_overdue`                     string              COMMENT    '是否有逾期',
`sales_sale_rate`                decimal(26,6)       COMMENT    '销售员_销售额提成比例',
`sales_profit_rate`              decimal(26,6)       COMMENT    '销售员_定价毛利额提成比例',
`service_user_sale_rate`         decimal(26,6)       COMMENT    '服务管家_销售额提成比例',
`service_user_profit_rate`       decimal(26,6)       COMMENT    '服务管家_定价毛利额提成比例'

) COMMENT '客户信息表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	
