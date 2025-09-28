--动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

--启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr=true;

insert overwrite table csx_analyse.csx_analyse_report_sss_crm_customer_over_rate_detail_mi partition(sdt) 

select
	concat_ws('&',cast(d.customer_id as string),a.company_code,'${ytd}') as biz_id,
	coalesce(d.region_code,'') as performance_region_code,
	coalesce(d.region_name,'') as performance_region_name,
	coalesce(d.province_code,'') as performance_province_code,
	coalesce(d.province_name,'') as performance_province_name,
	coalesce(d.city_group_code,'') as performance_city_code,
	coalesce(d.city_group_name,'') as performance_city_name,
	substr('${ytd}',1,6) as yearmonth,
	coalesce(a.channel_code,'') as channel_code,
	coalesce(a.channel_name,'') as channel_name,	-- 渠道
	coalesce(d.customer_id,'') as customer_id,
	a.customer_code,	-- 客户编码
	coalesce(d.customer_name,'') as customer_name,	-- 客户名称
	coalesce(d.sales_id_new,'') as sales_user_id,
	coalesce(d.work_no_new,'') as sales_user_number,	-- 销售员工号
	coalesce(d.sales_name_new,'') as sales_user_name,	-- 销售员
	
	coalesce(d.rp_service_user_id_new,'') as rp_service_user_id,
	coalesce(d.rp_service_user_work_no_new,'') as rp_service_user_number,
	coalesce(d.rp_service_user_name_new,'') as rp_service_user_name,
	
	coalesce(d.fl_service_user_id_new,'') as fl_service_user_id,
	coalesce(d.fl_service_user_work_no_new,'') as fl_service_user_number,
	coalesce(d.fl_service_user_name_new,'') as fl_service_user_name,	
	
	coalesce(d.bbc_service_user_id_new,'') as bbc_service_user_id,
	coalesce(d.bbc_service_user_work_no_new,'') as bbc_service_user_number,
	coalesce(d.bbc_service_user_name_new,'') as bbc_service_user_name,
	
	a.account_period_code,	-- 账期编码
	a.account_period_value,	-- 帐期天数
	a.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称,
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
		else coalesce(case when overdue_coefficient_numerator>=0 and a.receivable_amount>0 then overdue_coefficient_numerator else 0 end, 0)
		/(case when overdue_coefficient_denominator>=0 and a.receivable_amount>0 then overdue_coefficient_denominator else 0 end) end, 6),0) as over_rate, -- 逾期系数
	if(receivable_amount>=1,'是','否') as is_greater_0,
	'' as create_by,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as created_time,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as update_time,
	substr('${ytd}',1,6) as sdt		
from
	(
	select
		sdt,customer_code,
		customer_name,company_code,company_name,channel_code,channel_name,account_period_code,account_period_value,account_period_name,receivable_amount,overdue_amount,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt='${ytd}'
		and customer_code is not null
	)a
	--剔除业务代理与内购客户
	join		
		(
		select 
			* 
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt='${ytd}' 
			and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_code=a.customer_code  
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 
		(
		select 
			distinct customer_code 
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),0),'-','') 
			and sdt<='${ytd}' 
			and business_type_code in('3','4')
		)e on e.customer_code=a.customer_code
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_id,customer_no as customer_code,customer_name,sales_id_new,work_no_new,sales_name_new,
			rp_service_user_id_new,rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_id_new,fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_id_new,bbc_service_user_work_no_new,bbc_service_user_name_new,
			region_code,region_name,province_code,province_name,city_group_code,city_group_name,
			channel_code,channel_name
		from 
			csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where
			sdt='${ytd}'
		)d on d.customer_code=a.customer_code		
where 
	e.customer_code is null
	and (a.receivable_amount>0 or a.receivable_amount is null)
;

/*

create table csx_analyse.csx_analyse_report_sss_crm_customer_over_rate_detail_mi(
`biz_id`                         string               NOT NULL  COMMENT    '业务主键',
`performance_region_code`        string               DEFAULT NULL COMMENT    '大区编码',
`performance_region_name`        string               DEFAULT NULL COMMENT    '大区名称',
`performance_province_code`      string               DEFAULT NULL COMMENT    '省区编码',
`performance_province_name`      string               DEFAULT NULL COMMENT    '省区名称',
`performance_city_code`          string               DEFAULT NULL COMMENT    '城市组编码',
`performance_city_name`          string               DEFAULT NULL COMMENT    '城市组名称',
`yearmonth`                      string               DEFAULT NULL COMMENT    '年月',
`channel_code`                   string               DEFAULT NULL COMMENT    '渠道编码',
`channel_name`                   string               DEFAULT NULL COMMENT    '渠道名称',
`customer_id`                    string               DEFAULT NULL COMMENT    '客户ID',
`customer_code`                  string               DEFAULT NULL COMMENT    '客户编号',
`customer_name`                  string               DEFAULT NULL COMMENT    '客户名称',
`sales_user_id`                  string               DEFAULT NULL COMMENT    '销售员ID',
`sales_user_number`              string               DEFAULT NULL COMMENT    '销售员工号',
`sales_user_name`                string               DEFAULT NULL COMMENT    '销售员名称',
`rp_service_user_id`             string               DEFAULT NULL COMMENT    '日配服务管家ID',
`rp_service_user_number`         string               DEFAULT NULL COMMENT    '日配服务管家工号',
`rp_service_user_name`           string               DEFAULT NULL COMMENT    '日配服务管家名称',
`fl_service_user_id`             string               DEFAULT NULL COMMENT    '福利服务管家ID',
`fl_service_user_number`         string               DEFAULT NULL COMMENT    '福利服务管家工号',
`fl_service_user_name`           string               DEFAULT NULL COMMENT    '福利服务管家名称',
`bbc_service_user_id`            string               DEFAULT NULL COMMENT    'bbc服务管家ID',
`bbc_service_user_number`        string               DEFAULT NULL COMMENT    'bbc服务管家工号',
`bbc_service_user_name`          string               DEFAULT NULL COMMENT    'bbc服务管家名称',
`account_period_code`            string               DEFAULT NULL COMMENT    '账期类型',
`account_period_value`           int                  DEFAULT NULL COMMENT    '账期天数',
`account_period_name`            string               DEFAULT NULL COMMENT    '账期名称',
`company_code`                   string               DEFAULT NULL COMMENT    '公司编码',
`company_name`                   string               DEFAULT NULL COMMENT    '公司名称',
`receivable_amount`              decimal(20,6)        DEFAULT NULL COMMENT    '应收金额',
`overdue_amount`                 decimal(20,6)        DEFAULT NULL COMMENT    '逾期金额',
`overdue_coefficient_numerator`  decimal(20,6)        DEFAULT NULL COMMENT    '逾期金额*逾期天数',
`overdue_coefficient_denominator`decimal(20,6)        DEFAULT NULL COMMENT    '应收金额*帐期天数',
`over_rate`                      decimal(20,6)        DEFAULT NULL COMMENT    '逾期系数',
`is_greater_0`                   string               DEFAULT NULL COMMENT    '应收大于0',
`create_by`                      string               DEFAULT NULL COMMENT    '创建人',
`create_time`                    string               DEFAULT NULL COMMENT    '创建时间',
`update_time`                    string               DEFAULT NULL COMMENT    '更新时间'

) COMMENT '客户逾期系数表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS parquet;

*/	
