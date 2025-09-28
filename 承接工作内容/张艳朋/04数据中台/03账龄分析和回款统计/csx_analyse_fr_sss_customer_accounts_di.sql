set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;

-- 启用引号识别
set hive.support.quoted.identifiers=none;


insert overwrite table csx_analyse.csx_analyse_fr_sss_customer_accounts_di partition(sdt)

select
	concat_ws('-','${ytd}',a.customer_code,a.company_code) as biz_id,
	a.customer_code,
	c.customer_name,
	c.channel_code,
	c.channel_name,
	c.business_attribute,
	c.business_attribute_desc,
	c.first_category_code,
	c.first_category_name,
	c.second_category_code,
	c.second_category_name,
	c.third_category_code,
	c.third_category_name,
	c.sales_user_id,
	c.sales_user_number,
	c.sales_user_name,
	c.supervisor_user_id,
	c.supervisor_user_number,
	c.supervisor_user_name,
	c.performance_province_code,
	c.performance_province_name,
	c.performance_city_code,
	c.performance_city_name,
	a.company_code,
	a.company_name,
	a.account_period_code,
	a.account_period_name,
	a.account_period_value,
	a.customer_level,
	a.credit_limit,
	a.temp_credit_limit,
	a.temp_begin_time,
	a.temp_end_time,
	a.overdue_amount,
	a.overdue_amount_15_day,
	a.overdue_amount_30_day,
	a.overdue_amount_60_day,
	a.overdue_amount_90_day,
	a.overdue_amount_120_day,
	a.overdue_amount_180_day,
	a.overdue_amount_1_year,
	a.overdue_amount_2_year,
	a.overdue_amount_3_year,
	a.overdue_amount_more_3_year,
	a.no_overdue_amount,
	a.receivable_amount,
	a.bad_debt_amount,
	a.max_overdue_day,
	b.last_sale_date,
	b.last_to_today_days,
	b.customer_sign_company_active_status_code,
	b.customer_sign_company_active_status_name,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	'${ytd}' as sdt -- 统计日期  	
from 
	(
	select 
		customer_code,customer_name,company_code,company_name,account_period_code,account_period_name,account_period_value,customer_level,credit_limit,temp_credit_limit,temp_begin_time,
		temp_end_time,overdue_amount,overdue_amount_15_day,overdue_amount_30_day,overdue_amount_60_day,overdue_amount_90_day,overdue_amount_120_day,overdue_amount_180_day,
		overdue_amount_1_year,overdue_amount_2_year,overdue_amount_3_year,overdue_amount_more_3_year,
		no_overdue_amount,receivable_amount,bad_debt_amount,max_overdue_day,sdt
	from 
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt='${ytd}'
	) a 
	left join
		(
		select 
			customer_code,sign_company_code,last_sale_date,last_to_today_days,customer_sign_company_active_status_code,customer_sign_company_active_status_name
		from 
			csx_dws.csx_dws_crm_customer_sign_company_active
		where 
			sdt ='current'
		) b on a.customer_code= b.customer_code and a.company_code = b.sign_company_code
	left join
		(
		select
			customer_code,customer_name,channel_code,channel_name,business_attribute,business_attribute_desc,first_category_code,first_category_name,second_category_code,second_category_name,
			third_category_code,third_category_name,sales_user_id,sales_user_number,sales_user_name,supervisor_user_id,supervisor_user_number,supervisor_user_name,performance_city_code,performance_city_name,
			performance_province_code,performance_province_name
		from
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt ='current'
		) c on c.customer_code=a.customer_code
;
	
/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sss_customer_accounts_di  账龄分析和回款统计

drop table if exists csx_analyse.csx_analyse_fr_sss_customer_accounts_di;
create table csx_analyse.csx_analyse_fr_sss_customer_accounts_di(
`biz_id`                         string              COMMENT    '业务主键',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`channel_code`                   string              COMMENT    '渠道编码',
`channel_name`                   string              COMMENT    '渠道名称',
`business_attribute`             string              COMMENT    '客户属性编码 ',
`business_attribute_desc`        string              COMMENT    '客户属性名称',
`first_category_code`            string              COMMENT    '企业一级分类名称',
`first_category_name`            string              COMMENT    '企业一级分类名称',
`second_category_code`           string              COMMENT    '企业二级分类编码',
`second_category_name`           string              COMMENT    '企业二级分类名称',
`third_category_code`            string              COMMENT    '企业三级分类编码',
`third_category_name`            string              COMMENT    '企业三级分类名称',
`sales_user_id`                  string              COMMENT    '销售员ID',
`sales_user_number`              string              COMMENT    '销售员OA工号',
`sales_user_name`                string              COMMENT    '销售员名称',
`supervisor_user_id`             string              COMMENT    '主管ID',
`supervisor_user_number`         string              COMMENT    '主管OA工号',
`supervisor_user_name`           string              COMMENT    '主管名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市编码',
`performance_city_name`          string              COMMENT    '城市名称',
`company_code`                   string              COMMENT    '公司代码',
`company_name`                   string              COMMENT    '公司代码名称',
`account_period_code`            string              COMMENT    '付款条件',
`account_period_name`            string              COMMENT    '付款条件名称',
`account_period_value`           int                 COMMENT    '账期值',
`customer_level`                 string              COMMENT    '客户等级',
`credit_limit`                   decimal(26,2)       COMMENT    '信控额度',
`temp_credit_limit`              decimal(26,2)       COMMENT    '临时额度',
`temp_begin_time`                timestamp           COMMENT    '临时额度起始时间',
`temp_end_time`                  timestamp           COMMENT    '临时额度截止时间',
`overdue_amount`                 decimal(15,2)       COMMENT    '逾期金额',
`overdue_amount_15_day`          decimal(15,2)       COMMENT    '逾期1-15天',
`overdue_amount_30_day`          decimal(15,2)       COMMENT    '逾期15-30天',
`overdue_amount_60_day`          decimal(15,2)       COMMENT    '逾期30-60天',
`overdue_amount_90_day`          decimal(15,2)       COMMENT    '逾期60-90天',
`overdue_amount_120_day`         decimal(15,2)       COMMENT    '逾期90-120天',
`overdue_amount_180_day`         decimal(15,2)       COMMENT    '逾期120-180天',
`overdue_amount_1_year`          decimal(15,2)       COMMENT    '逾期180-365天',
`overdue_amount_2_year`          decimal(15,2)       COMMENT    '逾期1-2年',
`overdue_amount_3_year`          decimal(15,2)       COMMENT    '逾期2-3年',
`overdue_amount_more_3_year`     decimal(15,2)       COMMENT    '逾期3年以上',
`no_overdue_amount`              decimal(15,2)       COMMENT    '未逾期金额',
`receivable_amount`              decimal(15,2)       COMMENT    '应收账款',
`bad_debt_amount`                decimal(15,2)       COMMENT    '坏账金额',
`max_overdue_day`                int                 COMMENT    '最大逾期天数',
`last_sale_date`                 string              COMMENT    '最后销售日期',
`last_to_today_days`             int                 COMMENT    '未销售天数',
`customer_sign_company_active_status_code`string     COMMENT    '客户标识名称',
`customer_sign_company_active_status_name`string     COMMENT    '客户标识',
`update_time`                    timestamp           COMMENT    '更新时间'

) COMMENT '账龄分析和回款统计'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/


