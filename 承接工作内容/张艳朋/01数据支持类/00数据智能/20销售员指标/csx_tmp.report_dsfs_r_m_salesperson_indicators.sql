-- 销售员指标 

-- 切换tez计算引擎
set mapred.job.name=report_dsfs_r_m_salesperson_indicators;
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
set current_end_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月第一天
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');

-- 目标表
set target_table=csx_tmp.report_dsfs_r_m_salesperson_indicators;
	
with current_salesperson_indicators as 	
(
select
	g.region_code,g.region_name,g.province_code,g.province_name,g.city_code,g.city_name,a.work_no,a.sales_name,
	if(e.user_id is not null, '是', '否') as is_part_time_service_manager,
	coalesce(f.begin_date,'') as begin_date,
	case when emp_status='on' then floor(months_between(from_unixtime(unix_timestamp(${hiveconf:current_end_day},'yyyyMMdd')),from_unixtime(unix_timestamp(f.begin_date,'yyyyMMdd'))))
		when emp_status='leave' then floor(months_between(from_unixtime(unix_timestamp(f.end_date,'yyyyMMdd')),from_unixtime(unix_timestamp(f.begin_date,'yyyyMMdd'))))
		else 0 end as employment_months,
	coalesce(f.status,'') as status,
	a.under_name_total_customers_cnt,
	coalesce(b.deal_customers_cnt,0) as deal_customers_cnt,
	a.new_sign_customers_cnt,
	coalesce(b.sales_value,0) as sales_value,
	coalesce(b.profit,0) as profit,
	coalesce(b.front_profit,0) as front_profit,
	coalesce(c.percentage_amount,0) as percentage_amount,
	coalesce(d.receivable_amount,0) as receivable_amount,
	coalesce(d.overdue_amount,0) as overdue_amount,
	coalesce(c.overdue_rate,0) as overdue_rate,
	coalesce(d.overdue_customers_cnt,0) as overdue_customers_cnt
from	
	(
	select 
		sales_id,work_no,sales_name,
		count(distinct customer_no) as under_name_total_customers_cnt, -- 名下总客户数
		count(distinct case when regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day} then customer_no else null end) as new_sign_customers_cnt
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:current_end_day}
		and channel_code in('1','7','9')
		and customer_no !=''
		and work_no !=''
		and sales_name not rlike 'B'
		and sales_province_name not rlike '平台|BBC'
	group by 
		sales_id,work_no,sales_name
	) a
	left join
		(
		select 
			sales_id,work_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(front_profit)as front_profit,
			count(distinct customer_no) as deal_customers_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			sales_id,work_no
		) b on b.sales_id=a.sales_id
	left join	
		(
		select
			sales_id,sales_work_no,overdue_rate,
			sum(commion_total) as percentage_amount
		from
			csx_dw.ads_dsfs_r_m_crm_sales_customer_commission
		where
			smonth=substr(${hiveconf:current_end_day},1,6)
		group by 
			sales_id,sales_work_no,overdue_rate
		) c on c.sales_id=a.sales_id
	left join	
		(
		select --应收逾期
			sales_id,work_no,
			sum(case when receivable_amount>=0 then receivable_amount else null end) receivable_amount, -- 应收金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else null end) overdue_amount,	-- 逾期金额
			count(distinct case when overdue_amount>=0 and receivable_amount>0 then customer_no else null end) as overdue_customers_cnt
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_end_day}
		group by 
			sales_id,work_no
		) d on d.sales_id=a.sales_id
	left join --销售员是否兼职服务管家
		(
		select 
			user_id
		from 
			csx_ods.source_uc_w_a_user_position
		where 
			sdt = ${hiveconf:current_end_day}
			and user_position = 'CUSTOMER_SERVICE_MANAGER'
		) e on e.user_id = a.sales_id
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date,emp_status,end_date,
			case when emp_status='on' then '在职' when emp_status='leave' then '离职' else '' end as status
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_end_day}
		) f on f.employee_code=a.work_no
	left join 
		(
		select
			sales_id,region_code,region_name,province_code,province_name,city_code,city_name
		from
			(
			select 
				sales_id,
				sales_region_code as region_code,
				sales_region_name as region_name,
				sales_province_code as province_code,
				sales_province_name as province_name,
				sales_city_code as city_code,
				sales_city_name as city_name,
				row_number()over(partition by work_no order by create_time desc) as rn
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = ${hiveconf:current_end_day}
				and customer_no<>''	
				and sales_province_name not rlike '平台|BBC'
			) t1
		where
			rn=1
		) g on g.sales_id=a.sales_id
)

insert overwrite table ${hiveconf:target_table} partition(smonth)			
			
select
	'' as biz_id,region_code,region_name,province_code,province_name,city_code,city_name,work_no,sales_name,is_part_time_service_manager,begin_date,employment_months,status,under_name_total_customers_cnt,
	deal_customers_cnt,new_sign_customers_cnt,sales_value,profit,front_profit,percentage_amount,receivable_amount,overdue_amount,overdue_rate,overdue_customers_cnt,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr(${hiveconf:current_end_day}, 1, 6) as smonth
from
	current_salesperson_indicators
;



INVALIDATE METADATA csx_tmp.report_dsfs_r_m_salesperson_indicators;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_dsfs_r_m_salesperson_indicators  销售员指标

drop table if exists csx_tmp.report_dsfs_r_m_salesperson_indicators;
create table csx_tmp.report_dsfs_r_m_salesperson_indicators(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_code`                      string              COMMENT    '城市编码',
`city_name`                      string              COMMENT    '城市',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员',
`is_part_time_service_manager`   string              COMMENT    '销售员是否兼岗服务管家',
`begin_date`                     string              COMMENT    '入职日期',
`employment_months`              decimal(15,2)       COMMENT    '入职月数',
`status`                         string              COMMENT    '在职状态',
`under_name_total_customers_cnt` int                 COMMENT    '名下总客户数',
`deal_customers_cnt`             int                 COMMENT    '成交客户数',
`new_sign_customers_cnt`         int                 COMMENT    '新签客户数',
`sales_value`                    decimal(15,2)       COMMENT    '销售金额',
`profit`                         decimal(15,2)       COMMENT    '定价毛利额',
`front_profit`                   decimal(15,2)       COMMENT    '前端毛利额',
`percentage_amount`              decimal(15,2)       COMMENT    '提成金额',
`receivable_amount`              decimal(15,2)       COMMENT    '应收金额',
`overdue_amount`                 decimal(15,2)       COMMENT    '逾期金额',
`overdue_rate`                   decimal(15,2)       COMMENT    '逾期系数',
`overdue_customers_cnt`          int                 COMMENT    '逾期客户数',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT '销售员指标'
PARTITIONED BY (smonth string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	

