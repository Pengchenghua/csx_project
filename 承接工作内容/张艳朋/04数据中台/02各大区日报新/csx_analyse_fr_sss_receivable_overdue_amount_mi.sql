-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;

-- 启用引号识别
set hive.support.quoted.identifiers=none;
	
with current_receivable_overdue_amount as 	
(
select 
	b.performance_region_code,
	b.performance_region_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_city_code,
	b.performance_city_name,
	coalesce(b.supervisor_user_number,'') as supervisor_user_number,
	coalesce(b.supervisor_user_name,'') as supervisor_user_name,
	coalesce(sum(last_month_receivable_amount),0) as last_month_receivable_amount,	-- 上月应收金额
	coalesce(sum(last_month_overdue_amount),0) as last_month_overdue_amount,	-- 上月逾期金额
	coalesce(sum(last_month_overdue_amount)/sum(last_month_receivable_amount),0) as last_month_overdue_amount_rate, -- 上月逾期率
	coalesce(sum(receivable_amount)-sum(last_month_receivable_amount),0) as add_receivable_amount, -- 新增应收
	coalesce(sum(overdue_amount)-sum(last_month_overdue_amount),0) as add_overdue_amount, -- 新增逾期
	coalesce(sum(receivable_amount),0) as receivable_amount,	-- 本月应收金额
	coalesce(sum(overdue_amount),0) as overdue_amount,	-- 本月逾期金额
	coalesce(sum(overdue_amount)/sum(receivable_amount),0) as overdue_amount_rate, -- 本月逾期率
	coalesce(sum(payment_amount_d),0) as payment_amount_d, -- 当日回款
	coalesce(sum(payment_amount_m),0) as payment_amount_m, -- 本月累计回款
	0 as payment_plan,
	0 as payment_rate
from
	( 
	select -- 应收逾期
		customer_code,
		sum(case when receivable_amount>=0 and sdt=regexp_replace(last_day(add_months('${ytd_date}',-1)),'-','') then receivable_amount else 0 end) last_month_receivable_amount,	-- 上月应收金额
		sum(case when overdue_amount>=0 and receivable_amount>0 and sdt=regexp_replace(last_day(add_months('${ytd_date}',-1)),'-','') then overdue_amount else 0 end) last_month_overdue_amount,	-- 上月逾期金额
		sum(case when receivable_amount>=0 and sdt='${ytd}' then receivable_amount else 0 end) receivable_amount,	-- 本月应收金额
		sum(case when overdue_amount>=0 and receivable_amount>0 and sdt='${ytd}' then overdue_amount else 0 end) overdue_amount,	-- 本月逾期金额
		0 as payment_amount_d,
		0 as payment_amount_m
	from
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt='${ytd}'
		or sdt=regexp_replace(last_day(add_months('${ytd_date}',-1)),'-','')
	group by 
		customer_code
	union all
	select -- 回款
		customer_code,0 as last_month_receivable_amount,0 as last_month_overdue_amount,0 as receivable_amount,0 as overdue_amount,
		sum(case when regexp_replace(substr(paid_time,1,10),'-','')='${ytd}' then pay_amt else null end) payment_amount_d,
		sum(pay_amt) payment_amount_m
	from
		csx_dwd.csx_dwd_sss_close_bill_account_record_di
	where 
		regexp_replace(substr(paid_time,1,10),'-','') >=regexp_replace(trunc('${ytd_date}', 'MM'), '-', '')
		and regexp_replace(substr(paid_time,1,10),'-','') <='${ytd}'
		and delete_flag ='0'
		and money_back_id<>'0' -- 回款关联ID为0是微信支付、-1是退货系统核销
	group by 
		customer_code
	)a
	left join -- 客户信息（每个城市都有虚拟主管，按省区汇总的时候把工号处理成相同的）
		(
		select 
			customer_code,customer_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,
			performance_city_code,performance_city_name,
			case when supervisor_user_name like '%虚拟主管%' then '00000000x' else supervisor_user_number end as supervisor_user_number,
			supervisor_user_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt='current'
		) as b on b.customer_code=a.customer_code
group by
	b.performance_region_code,
	b.performance_region_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_city_code,
	b.performance_city_name,
	coalesce(b.supervisor_user_number,''),
	coalesce(b.supervisor_user_name,'')
)

insert overwrite table csx_analyse.csx_analyse_fr_sss_receivable_overdue_amount_mi partition(month)			
			
select
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	supervisor_user_number,
	supervisor_user_name,
	last_month_receivable_amount,	-- 上月应收金额
	last_month_overdue_amount,	-- 上月逾期金额
	last_month_overdue_amount_rate, -- 上月逾期率
	add_receivable_amount, -- 新增应收
	add_overdue_amount, -- 新增逾期
	receivable_amount,	-- 本月应收金额
	overdue_amount,	-- 本月逾期金额
	overdue_amount_rate, -- 本月逾期率
	payment_amount_d, -- 当日回款
	payment_amount_m, -- 本月累计回款
	payment_plan,
	payment_rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr('${ytd}', 1, 6) as month
from
	current_receivable_overdue_amount
;
	

/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sss_receivable_overdue_amount_mi  主管维度应收逾期金额统计

drop table if exists csx_analyse.csx_analyse_fr_sss_receivable_overdue_amount_mi;
create table csx_analyse.csx_analyse_fr_sss_receivable_overdue_amount_mi(
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省份编码',
`performance_province_name`      string              COMMENT    '省份名称',
`performance_city_code`          string              COMMENT    '城市编码',
`performance_city_name`          string              COMMENT    '城市名称',
`supervisor_user_number`         string              COMMENT    '销售主管工号',
`supervisor_user_name`           string              COMMENT    '销售主管姓名',
`last_month_receivable_amount`   decimal(26,6)       COMMENT    '上月应收金额',
`last_month_overdue_amount`      decimal(26,6)       COMMENT    '上月逾期金额',
`last_month_overdue_amount_rate` decimal(26,6)       COMMENT    '上月逾期率',
`add_receivable_amount`          decimal(26,6)       COMMENT    '新增应收',
`add_overdue_amount`             decimal(26,6)       COMMENT    '新增逾期',
`receivable_amount`              decimal(26,6)       COMMENT    '本月应收金额',
`overdue_amount`                 decimal(26,6)       COMMENT    '本月逾期金额',
`overdue_amount_rate`            decimal(26,6)       COMMENT    '本月逾期率',
`payment_amount_d`               decimal(26,6)       COMMENT    '当日回款金额',
`payment_amount_m`               decimal(26,6)       COMMENT    '本月累计回款金额',
`payment_plan`                   decimal(26,6)       COMMENT    '还款计划',
`payment_rate`                   decimal(26,6)       COMMENT    '还款完成率',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT '主管维度应收逾期金额统计'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	