-- 华北日报 
-- 核心逻辑：按主管维度统计应收逾期金额情况

-- 切换tez计算引擎
set mapred.job.name=report_sss_r_m_receivable_overdue_amount;
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

-- 计算日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月第一天
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');

-- 上月最后一天
set last_month_end_day =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');

-- 目标表
set target_table=csx_tmp.report_sss_r_m_receivable_overdue_amount;
	
with current_receivable_overdue_amount as 	
(
select 
	b.sales_region_code as region_code,
	b.sales_region_name as region_name,
	b.sales_province_code as province_code,
	b.sales_province_name as province_name,
	coalesce(b.first_supervisor_work_no,'') as first_supervisor_work_no,
	coalesce(b.first_supervisor_name,'') as first_supervisor_name,
	coalesce(sum(last_month_receivable_amount),0) as last_month_receivable_amount,	-- 上月应收金额
	coalesce(sum(last_month_overdue_amount),0) as last_month_overdue_amount,	-- 上月逾期金额
	coalesce(sum(last_month_overdue_amount)/sum(last_month_receivable_amount),0) as last_month_overdue_amount_rate, -- 上月逾期率
	coalesce(sum(receivable_amount)-sum(last_month_receivable_amount),0) as add_receivable_amount, -- 新增应收
	coalesce(sum(overdue_amount)-sum(last_month_overdue_amount),0) as add_overdue_amount, -- 新增逾期
	coalesce(sum(receivable_amount),0) as receivable_amount,	-- 本月应收金额
	coalesce(sum(overdue_amount),0) as overdue_amount,	-- 本月逾期金额
	coalesce(sum(overdue_amount)/sum(receivable_amount),0) as overdue_amount_rate, --本月逾期率
	coalesce(sum(payment_amount_d),0) as payment_amount_d, -- 当日回款
	coalesce(sum(payment_amount_m),0) as payment_amount_m, -- 本月累计回款
	0 as payment_plan,
	0 as payment_rate
from
	( 
	select --应收逾期
		customer_no,
		sum(case when receivable_amount>=0 and sdt=${hiveconf:last_month_end_day} then receivable_amount else 0 end) last_month_receivable_amount,	-- 上月应收金额
		sum(case when overdue_amount>=0 and receivable_amount>0 and sdt=${hiveconf:last_month_end_day} then overdue_amount else 0 end) last_month_overdue_amount,	-- 上月逾期金额
		sum(case when receivable_amount>=0 and sdt=${hiveconf:current_day} then receivable_amount else 0 end) receivable_amount,	-- 本月应收金额
		sum(case when overdue_amount>=0 and receivable_amount>0 and sdt=${hiveconf:current_day} then overdue_amount else 0 end) overdue_amount,	-- 本月逾期金额
		0 as payment_amount_d,
		0 as payment_amount_m
	from
		csx_dw.dws_sss_r_a_customer_accounts
	where
		sdt=${hiveconf:last_month_end_day}
		or sdt=${hiveconf:current_day}
	group by 
		customer_no
	union all
	select -- 回款
		customer_code as customer_no,0 as last_month_receivable_amount,0 as last_month_overdue_amount,0 as receivable_amount,0 as overdue_amount,
		sum(case when regexp_replace(substr(paid_time,1,10),'-','')=${hiveconf:current_day} then payment_amount else null end) payment_amount_d,
		sum(payment_amount) payment_amount_m
	from
		csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
	where 
		regexp_replace(substr(paid_time,1,10),'-','') >=${hiveconf:current_start_day} 
		and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:current_day}
		and is_deleted ='0'
		and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
	group by 
		customer_code
	)a
	left join -- 客户信息（每个城市都有虚拟主管，按省区汇总的时候把工号处理成相同的）
		(
		select 
			customer_no,customer_name,sales_region_code,sales_region_name,sales_province_code,sales_province_name,
			case when first_supervisor_name like '%虚拟主管%' then '00000000x' else first_supervisor_work_no end as first_supervisor_work_no,
			first_supervisor_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		) as b on b.customer_no=a.customer_no
	join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_day}
			and emp_status='on'
		) tmp1 on tmp1.employee_code=b.first_supervisor_work_no
group by
	b.sales_region_code,
	b.sales_region_name,
	b.sales_province_code,
	b.sales_province_name,
	coalesce(b.first_supervisor_work_no,''),
	coalesce(b.first_supervisor_name,'')	
)

insert overwrite table ${hiveconf:target_table} partition(month)			
			
select
	region_code,
	region_name,
	province_code,
	province_name,
	first_supervisor_work_no,
	first_supervisor_name,
	last_month_receivable_amount,	-- 上月应收金额
	last_month_overdue_amount,	-- 上月逾期金额
	last_month_overdue_amount_rate, -- 上月逾期率
	add_receivable_amount, -- 新增应收
	add_overdue_amount, -- 新增逾期
	receivable_amount,	-- 本月应收金额
	overdue_amount,	-- 本月逾期金额
	overdue_amount_rate, --本月逾期率
	payment_amount_d, -- 当日回款
	payment_amount_m, -- 本月累计回款
	payment_plan,
	payment_rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr(${hiveconf:current_day}, 1, 6) as month
from
	current_receivable_overdue_amount
;



INVALIDATE METADATA csx_tmp.report_sss_r_m_receivable_overdue_amount;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sss_r_m_receivable_overdue_amount  华北日报 应收逾期金额统计

drop table if exists csx_tmp.report_sss_r_m_receivable_overdue_amount;
create table csx_tmp.report_sss_r_m_receivable_overdue_amount(
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`first_supervisor_work_no`       string              COMMENT    '一级主管工号',
`first_supervisor_name`          string              COMMENT    '一级主管姓名',
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
) COMMENT 'zhangyanpeng:华北日报-应收逾期金额统计'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	