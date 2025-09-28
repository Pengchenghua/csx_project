-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 启用引号识别
set hive.support.quoted.identifiers=none;


with current_customer_codermal_performance as 	
(	
select
	coalesce(b.performance_region_code,a.performance_region_code) as performance_region_code,
	coalesce(b.performance_region_name,a.performance_region_name) as performance_region_name,
	coalesce(b.performance_province_code,a.performance_province_code) as performance_province_code,
	coalesce(b.performance_province_name,a.performance_province_name) as performance_province_name,
	coalesce(b.performance_city_code,a.performance_city_code) as performance_city_code,
	coalesce(b.performance_city_name,a.performance_city_name) as performance_city_name,
	a.customer_id,
	coalesce(b.customer_code,'') as customer_code,
	a.customer_name,
	coalesce(b.sales_user_number,'') as sales_user_number,
	coalesce(b.sales_user_name,'') as sales_user_name,
	regexp_replace(substr(a.business_sign_time,1,10),'-','') as sign_date,
	coalesce(c.first_business_sale_date,'') as first_business_sale_date,
	a.estimate_contract_amount,
	a.contract_cycle,
	coalesce(d.this_month_sale_amt,'') as this_month_sale_amt,
	coalesce(d.last_month_sale_amt,'') as last_month_sale_amt,
	if(d.this_month_sale_amt is null and d.last_month_sale_amt is null,'',coalesce(d.this_month_sale_amt,0)+coalesce(d.last_month_sale_amt,0)) as total_sale_amt,
	d.csp_rate,
	coalesce(b.dev_source_code,0) as dev_source_code,
	coalesce(b.dev_source_name,'') as dev_source_name
from
	(
	select
		customer_id,business_number,customer_name,business_attribute_code,business_attribute_name,contract_cycle,
		business_stage,estimate_contract_amount,business_sign_time,performance_region_code,performance_region_name,performance_province_code,
		performance_province_name,performance_city_code,performance_city_name
	from
		-- csx_dw.ads_crm_r_m_business_customer
		csx_dim.csx_dim_crm_business_info
	where
		sdt='${ytd}'
		and status=1 -- 是否有效
		and business_stage=5 -- 阶段5
		and business_attribute_code=1 -- 新客户属性（商机属性） 1：日配客户 2：福利客户 3：大宗贸易 4：M端 5：BBC 6：内购
		and regexp_replace(substr(business_sign_time,1,7),'-','') = substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6)
	) a 
	left join
		(
		select
			customer_id,customer_code,customer_name,sales_user_id,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,
			performance_province_name,performance_city_code,performance_city_name,dev_source_code,dev_source_name
		from
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='${ytd}'
		)b on b.customer_id=a.customer_id
	left join
		(
		select
			distinct customer_code,customer_name,first_business_sale_date
		from 
			csx_dws.csx_dws_crm_customer_business_active_di
		where 
			sdt = '${ytd}'
			and business_type_code=1
		)c on c.customer_code=b.customer_code
	left join
		(
		select 
			customer_code,
			sum(case when substr(sdt,1,6)=substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) and business_type_code='1' then sale_amt else null end) as last_month_sale_amt,
			sum(case when substr(sdt,1,6)=substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),0),'-',''),1,6) and business_type_code='1' then sale_amt else null end) as this_month_sale_amt,
			sum(case when business_type_code='4' then sale_amt else null end)/sum(sale_amt) as csp_rate
		from 
			-- csx_dw.dws_sale_r_d_detail
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and '${ytd}'
			and channel_code in ('1','7','9')
			and business_type_code in ('1','4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_code
		)d on d.customer_code=b.customer_code
)

insert overwrite table csx_analyse.csx_analyse_fr_sale_customer_normal_performance_di partition(sdt)	

select
	concat_ws('&',cast(customer_id as string),'${ytd}') as biz_id,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	customer_id,
	customer_code,
	customer_name,
	sales_user_number,
	sales_user_name,
	sign_date,
	first_business_sale_date,
	estimate_contract_amount,
	contract_cycle,
	this_month_sale_amt,
	last_month_sale_amt,
	total_sale_amt,
	csp_rate,
	dev_source_code,
	dev_source_name,
	'${ytd}' as sdt
from
	current_customer_codermal_performance
;
	

/*

create table csx_analyse.csx_analyse_fr_sale_customer_normal_performance_di(
`biz_id`                         string              COMMENT    '业务主键',
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市编码',
`performance_city_name`          string              COMMENT    '城市',
`customer_id`                    string              COMMENT    '客户ID',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_user_number`              string              COMMENT    '业务员工号',
`sales_user_name`                string              COMMENT    '业务员名称',
`sign_date`                      string              COMMENT    '签约日期',
`first_business_sale_date`       string              COMMENT    '日配首单日期',
`estimate_contract_amount`       decimal(15,6)       COMMENT    '签约金额',
`contract_cycle`                 string              COMMENT    '合同周期',
`this_month_sale_amt`            decimal(15,6)       COMMENT    '本月履约额(日配)',
`last_month_sale_amt`            decimal(15,6)       COMMENT    '上月履约额(日配)',
`total_sale_amt`                 decimal(15,6)       COMMENT    '总履约额(近俩月日配)',
`csp_rate`                       decimal(15,6)       COMMENT    '城市服务商履约额占比',
`dev_source_code`                string              COMMENT    '开发来源编码',
`dev_source_name`                string              COMMENT    '开发来源名称'

) COMMENT '上月签约日配业务客户履约情况'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	

		
	