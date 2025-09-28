-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;

with current_data_01 as
(
	select 
		a.sdt,substr(a.sdt,1,6) year_month,
		a.customer_code,
		a.business_type_code,
		a.business_type_name,
		sum(a.sale_amt) as sale_amt, 
		sum(a.profit) as profit, 
		sum(case when a.refund_order_flag='X' then a.sale_amt else 0 end) as refund_sale_amt,
		a.credit_code,
		b.business_attribute_code as credit_business_attribute_code,
		b.business_attribute_name as credit_business_attribute_name,
		a.sign_company_code
	from
		(
		select
			sdt,customer_code,business_type_code,business_type_name,sale_amt,profit,refund_order_flag,credit_code,sign_company_code
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and sdt<='${ytd}'
			and channel_code in('1','7','9')
			and business_type_code in('1','2','6')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
		) a 
		left join
			(
			select 
				customer_code,company_code,credit_code,business_attribute_code,business_attribute_name
			from 
				csx_dim.csx_dim_crm_customer_company_details 
			where 
				sdt='current'
			group by 
				customer_code,company_code,credit_code,business_attribute_code,business_attribute_name
			)b on b.customer_code=a.customer_code and b.company_code=a.sign_company_code and b.credit_code=a.credit_code
	group by 
		a.sdt,substr(a.sdt,1,6),a.customer_code,a.business_type_code,a.business_type_name,a.credit_code,b.business_attribute_code,b.business_attribute_name,a.sign_company_code
),

current_data_02 as
(
	select
		'1' as business_type_code,'日配业务' as business_type_name,substr(sdt,1,6)as month,customer_no,customer_name,
		rp_service_user_id_new as service_user_id,
		rp_service_user_work_no_new as service_user_number,
		rp_service_user_name_new as service_user_name
	from 
		csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where 
		sdt in(regexp_replace(last_day(add_months(trunc('${ytd_date}','MM'),-1)),'-',''),'${ytd}')
		and customer_no !=''
		and rp_service_user_work_no_new is not null
	union all
	select
		'2' as business_type_code,'福利业务' as business_type_name,substr(sdt,1,6)as month,customer_no,customer_name,
		fl_service_user_id_new as service_user_id,
		fl_service_user_work_no_new as service_user_number,
		fl_service_user_name_new as service_user_name
	from 
		csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where 
		sdt in(regexp_replace(last_day(add_months(trunc('${ytd_date}','MM'),-1)),'-',''),'${ytd}')
		and customer_no !=''
		and fl_service_user_work_no_new is not null
	union all
	select
		'6' as business_type_code,'BBC' as business_type_name,substr(sdt,1,6)as month,customer_no,customer_name,
		bbc_service_user_id_new as service_user_id,
		bbc_service_user_work_no_new as service_user_number,
		bbc_service_user_name_new as service_user_name
	from 
		csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where 
		sdt in(regexp_replace(last_day(add_months(trunc('${ytd_date}','MM'),-1)),'-',''),'${ytd}')
		and customer_no !=''
		and bbc_service_user_work_no_new is not null
),

current_data_03 as
(
	select
		region_code,region_name,province_code,province_name,city_group_code,city_group_name,
		substr(sdt,1,6) as month,customer_id,customer_no,customer_name,sales_id_new as sales_id,work_no_new as work_no,sales_name_new as sales_name,
		first_supervisor_code,first_supervisor_work_no,first_supervisor_name
	from 
		csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where 
		sdt in(regexp_replace(last_day(add_months(trunc('${ytd_date}','MM'),-1)),'-',''),'${ytd}')
		and customer_no !=''
)

--刷新数据
insert overwrite table csx_analyse.csx_analyse_report_sss_crm_customer_sale_detail_mi partition(month)
select
	concat_ws('&',a.customer_code,a.credit_code,a.sign_company_code,a.sdt,cast(a.business_type_code as string)) as biz_id,
	coalesce(c.region_code,'') as performance_region_code,
	coalesce(c.region_name,'') as performance_region_name,
	coalesce(c.province_code,'') as performance_province_code,
	coalesce(c.province_name,'') as performance_province_name,
	coalesce(c.city_group_code,'') as performance_city_code,
	coalesce(c.city_group_name,'') as performance_city_name,
	coalesce(c.customer_id,'') as customer_id,
	a.customer_code,
	coalesce(c.customer_name,'') as customer_name,
	coalesce(c.sales_id,'') as sales_user_id,
	coalesce(c.work_no,'') as sales_user_number,
	coalesce(c.sales_name,'') as sales_user_name,
	a.business_type_code,
	a.business_type_name,
	coalesce(b.service_user_id,'') as service_user_id,
	coalesce(b.service_user_number,'') as service_user_number,
	coalesce(b.service_user_name,'') as service_user_name,
	coalesce(c.first_supervisor_code,'') as supervisor_user_id,
	coalesce(c.first_supervisor_work_no,'') as supervisor_user_number,
	coalesce(c.first_supervisor_name,'') as supervisor_user_name,
	a.sdt as sales_date,
	a.sale_amt,
	a.profit,
	a.refund_sale_amt,
	a.year_month,
	'' as create_by,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as create_time,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as update_time,
	a.credit_code,
	a.credit_business_attribute_code,
	a.credit_business_attribute_name,
	a.sign_company_code,
	a.year_month as month
from
	current_data_01 a 
	left join current_data_02 b on a.customer_code=b.customer_no and a.year_month=b.month and a.business_type_code=b.business_type_code
	left join current_data_03 c on a.customer_code=c.customer_no and a.year_month=c.month
where 
    a.customer_code is not null
	and c.customer_id is not null
;


/*

create table csx_analyse.csx_analyse_report_sss_crm_customer_sale_detail_mi(
`biz_id`                         string              COMMENT    '业务主键',
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组编码',
`performance_city_name`          string              COMMENT    '城市组名称',
`customer_id`                    string              COMMENT    '客户id',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_user_id`                  string              COMMENT    '销售员id',
`sales_user_number`              string              COMMENT    '销售员工号',
`sales_user_name`                string              COMMENT    '销售员名称',
`business_type_code`             string              COMMENT    '业务类型编码',
`business_type_name`             string              COMMENT    '业务类型名称',
`service_user_id`                string              COMMENT    '服务管家ID',
`service_user_number`            string              COMMENT    '服务管家工号',
`service_user_name`              string              COMMENT    '服务管家名称',
`supervisor_user_id`             string              COMMENT    '销售主管id',
`supervisor_user_number`         string              COMMENT    '销售主管工号',
`supervisor_user_name`           string              COMMENT    '销售主管名称',
`sales_date`                     string              COMMENT    '销售日期',
`sale_amt`                       decimal(20,6)       COMMENT    '销售额',
`profit`                         decimal(20,6)       COMMENT    '定价毛利额',
`refund_sale_amt`                decimal(20,6)       COMMENT    '退货金额',
`year_month`                     string              COMMENT    '年月',
`create_by`                      string              COMMENT    '创建人',
`create_time`                    timestamp           COMMENT    '创建时间',
`update_time`                    timestamp           COMMENT    '更新时间'

) COMMENT '客户销售表'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	
