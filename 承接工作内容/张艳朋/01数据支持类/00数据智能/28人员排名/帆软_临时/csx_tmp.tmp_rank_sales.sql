-- 销售员指标 

-- 切换tez计算引擎
set mapred.job.name=tmp_r_m_rank_sales;
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
--set current_end_day = regexp_replace(date_sub(current_date, 1), '-', '');
set current_end_day = '20211031';
-- 当月第一天
--set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');
set current_start_day = '20211001';
-- 目标表
set target_table=csx_tmp.tmp_r_m_rank_sales;
	
with current_tmp_r_m_rank_sales as 	
(
select
	b.province_code,
	b.province_name,
	b.city_group_code,
	b.city_group_name,
	coalesce(f.sales_supervisor_name,'') as sales_supervisor_name,
	a.user_number,
	a.name,
	coalesce(g.begin_date,'') as begin_date,
	'' as target_1,
	c.sales_value,
	'' as target_2,
	'' as target_3,
	c.profit_rate,
	'' as target_4,
	'' as target_5,
	d.customer_cnt,
	d.estimate_contract_amount,
	'' as target_6,
	c.customer_cnt as customer_cnt_2,
	e.business_number_cnt,
	e.estimate_contract_amount as estimate_contract_amount_2,
	e.rate
from
	--销售员信息
	( 
	select
		user_number,name,city_name,prov_name
	from
		csx_dw.dws_basic_w_a_user
	where
		sdt=${hiveconf:current_end_day}
		and status = 0 
		and del_flag = '0'
		and user_position = 'SALES' 
		and prov_name not like '平台%'
		and name not rlike'A|B|C|M'
	) a 
	-- 地区信息
	left join
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) b on b.city_name=a.city_name and b.area_province_name=a.prov_name
	-- 销售额
	left join
		(	
		select 
			work_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate,
			count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
			and channel_code in('1','7','9')
		group by 
			work_no
		) c on c.work_no=a.user_number
	-- 新签客户
	left join
		( 
		select
			work_no,
			count(customer_no) as customer_cnt,
			sum(estimate_contract_amount) as estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		group by 
			work_no
		) d on d.work_no=a.user_number
	-- 商机数量及金额
	left join
		( 
		select 
			work_no,
			count(distinct business_number) as business_number_cnt,
			count(distinct case when business_stage=5 then business_number else null end)/count(distinct business_number) as rate,
			sum(estimate_contract_amount) as estimate_contract_amount		
		from 
			csx_dw.ads_crm_r_m_business_customer
		where 
			month = substr(${hiveconf:current_end_day},1,6)
			and status=1
			--and business_stage !=5 -- 不含100%
		group by 
			work_no
		) e on e.work_no=a.user_number
	-- 主管信息
	left join
		( 
		select
			sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		from
			(
			select
				name as sales_name,
				user_number as work_no,
				user_position as position,
				-- 主管
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by user_number order by distance) as sales_supervisor_id,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by user_number order by distance) as sales_supervisor_name,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by user_number order by distance) as sales_supervisor_work_no,
				row_number() over(partition by user_number order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt = ${hiveconf:current_end_day}
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		) f on f.work_no = a.user_number
	--入职信息
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt = ${hiveconf:current_end_day}
		) g on g.employee_code=a.user_number
)

insert overwrite table ${hiveconf:target_table} partition(smonth)			
			
select
	'' as biz_id,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	sales_supervisor_name,
	user_number,
	name,
	begin_date,
	target_1,
	sales_value,
	target_2,
	target_3,
	profit_rate,
	target_4,
	target_5,
	customer_cnt,
	estimate_contract_amount,
	target_6,
	customer_cnt_2,
	business_number_cnt,
	estimate_contract_amount_2,
	rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr(${hiveconf:current_end_day}, 1, 6) as smonth
from
	current_tmp_r_m_rank_sales
;



INVALIDATE METADATA csx_tmp.tmp_r_m_rank_sales;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.tmp_r_m_rank_sales  销售员指标

drop table if exists csx_tmp.tmp_r_m_rank_sales;
create table csx_tmp.tmp_r_m_rank_sales(
`biz_id`                         string              COMMENT    '业务主键',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_group_code`                string              COMMENT    '城市编码',
`city_group_name`                string              COMMENT    '城市',
`sales_supervisor_name`          string              COMMENT    '主管工号',
`user_number`                    string              COMMENT    '销售员工号',
`name`                           string              COMMENT    '销售员名称',
`begin_date`                     string              COMMENT    '入职日期',
`target_1`                       decimal(15,2)       COMMENT    '目标1',
`sales_value`                    decimal(15,2)       COMMENT    '销售额',
`target_2`                       decimal(15,2)       COMMENT    '目标2',
`target_3`                       decimal(15,2)       COMMENT    '目标3',
`profit_rate`                    decimal(15,2)       COMMENT    '定价毛利率',
`target_4`                       decimal(15,2)       COMMENT    '目标4',
`target_5`                       decimal(15,2)       COMMENT    '目标5',
`customer_cnt`                   decimal(15,2)       COMMENT    '客户数',
`estimate_contract_amount`       decimal(15,2)       COMMENT    '预计签约金额',
`target_6`                       decimal(15,2)       COMMENT    '目标6',
`customer_cnt_2`                 decimal(15,2)       COMMENT    '客户数2',
`business_number_cnt`            int                 COMMENT    '商机数',
`estimate_contract_amount_2`     decimal(15,2)       COMMENT    '签约金额',
`rate`                           decimal(15,2)       COMMENT    '转化率',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT '销售员排名'
PARTITIONED BY (smonth string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	

