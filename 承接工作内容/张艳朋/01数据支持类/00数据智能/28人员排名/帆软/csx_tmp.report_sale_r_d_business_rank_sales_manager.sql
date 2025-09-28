-- 销售员指标 

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_d_business_rank_sales_manager;
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
set current_end_day = '20211123';
-- 当月第一天
--set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');
set current_start_day = '20211101';
-- 目标表
set target_table=csx_tmp.report_sale_r_d_business_rank_sales_manager;
	
with current_tmp_rank as 	
(
select
	c.region_code,
	c.region_name,
	c.province_code,
	c.province_name,
	c.city_group_code,
	c.city_group_name,
	a.second_supervisor_work_no,
	a.second_supervisor_name,
	coalesce(e.begin_date,'') as begin_date,
	a.business_type,
	coalesce(b.status,'') as status,
	'' as sales_target,
	coalesce(f.sales_value,0) as sales_value,
	'' as sales_target_achv_rate,
	'' as profit_rate_target,
	coalesce(f.profit_rate,0) as profit_rate,
	'' as profit_rate_achv_rate,
	coalesce(f.performance_customers,0) as performance_customers,
	'' as new_sign_customers_target,
	coalesce(g.new_sign_customers,0) as new_sign_customers,
	coalesce(g.new_sign_amount,0) as new_sign_amount,
	coalesce(i.new_sign_business,0) as new_sign_business,
	coalesce(i.new_sign_business_amount,0) as new_sign_business_amount,
	'' as new_sign_customers_achv_rate,
	coalesce(h.business_number_cnt,0) as business_number_cnt,
	coalesce(h.estimate_contract_amount,0) as estimate_contract_amount,
	row_number()over(partition by c.province_name,c.city_group_name,a.second_supervisor_work_no order by f.sales_value desc) as rank_number
from
	(
	select --人员信息
		second_supervisor_code,second_supervisor_work_no,second_supervisor_name,
		if(work_no=second_supervisor_work_no,'自有','团队') as business_type
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:current_end_day}
		--and sales_position='SALES'
		and second_supervisor_work_no!=''
	group by 
		second_supervisor_code,second_supervisor_work_no,second_supervisor_name,
		if(work_no=second_supervisor_work_no,'自有','团队')
	) a 
	left join -- 用户表，获取城市信息
		(
		select
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用') as status
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt=${hiveconf:current_end_day}
			and del_flag = '0'
		group by 
			id,user_number,name,user_position,city_name,prov_name,if(status=0,'启用','禁用')
		) b on b.id=a.second_supervisor_code
	left join -- 区域表
		( 
		select
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) c on c.city_name=b.city_name and c.area_province_name=b.prov_name
	--left join --兼岗信息
	--	(
	--	select
	--		user_id,user_position
	--	from
	--		csx_ods.source_uc_w_a_user_position 
	--	where
	--		sdt=${hiveconf:current_end_day}
	--	group by 
	--		user_id,user_position
	--	) d on d.user_id=a.second_supervisor_code			
	left join -- 入职信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name,emp_status
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt =${hiveconf:current_end_day}
		) e on e.employee_code=a.second_supervisor_work_no
	left join --销售额 履约客户数
		(	
		select 
			t2.second_supervisor_code,
			t2.business_type,
			sum(t1.sales_value) as sales_value,
			sum(t1.profit) as profit,
			sum(t1.profit)/abs(sum(t1.sales_value)) as profit_rate,
			count(distinct t1.customer_no) as performance_customers
		from
			(
			select
				customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
			from
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_end_day}
				and channel_code in('1','7','9')
				and business_type_code !='4'
			) t1 
			left join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
					second_supervisor_code,second_supervisor_work_no,second_supervisor_name,if(work_no=second_supervisor_work_no,'自有','团队') as business_type
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and second_supervisor_code !=''
				) t2 on t2.customer_no=t1.customer_no					
		group by 
			t2.second_supervisor_code,t2.business_type
		) f on f.second_supervisor_code=a.second_supervisor_code and f.business_type=a.business_type		
	left join -- 新签客户
		( 
		select
			second_supervisor_code,
			if(work_no=second_supervisor_work_no,'自有','团队') as business_type,
			count(customer_no) as new_sign_customers,
			sum(estimate_contract_amount) as new_sign_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_end_day}
			and regexp_replace(substr(sign_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and second_supervisor_code !=''
		group by 
			second_supervisor_code,
			if(work_no=second_supervisor_work_no,'自有','团队')
		) g on g.second_supervisor_code=a.second_supervisor_code and g.business_type=a.business_type
	left join -- 商机数量及金额
		( 
		select 
			t2.second_supervisor_code,
			t2.business_type,
			count(distinct t1.business_number) as business_number_cnt,
			sum(case when t1.business_stage=3 then t1.estimate_contract_amount*0.5 when t1.business_stage=4 then t1.estimate_contract_amount*0.75 else null end) as estimate_contract_amount		
		from 
			(
			select
				customer_id,business_number,customer_name,business_stage,estimate_contract_amount
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status=1
				and business_stage in ('3','4')
			) t1 
			join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
					second_supervisor_code,second_supervisor_work_no,second_supervisor_name,if(work_no=second_supervisor_work_no,'自有','团队') as business_type
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and second_supervisor_code !=''
				) t2 on t2.customer_id=t1.customer_id
		group by 
			t2.second_supervisor_code,
			t2.business_type
		) h on h.second_supervisor_code=a.second_supervisor_code and h.business_type=a.business_type			
	left join --新签商机数及金额
		(
		select
			c.second_supervisor_code,
			c.business_type,
			count(distinct a.business_number) as new_sign_business,
			sum(estimate_contract_amount) as new_sign_business_amount
		from
			(
			select 
				customer_id,business_number,customer_name,sales_id,
				estimate_contract_amount,business_stage,status
			from
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = substr(${hiveconf:current_end_day},1,6)
				and status='1'
				and business_stage=5
			) a 
			join
				(
				select
					business_number,create_time
				from
					csx_ods.source_crm_r_d_operate_log
				where
					--sdt='20210927'
					after_data=5
					and regexp_replace(substr(create_time,1,10),'-','') between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				group by 
					business_number,create_time
				) b on b.business_number=a.business_number
			join
				(
				select
					customer_id,customer_no,customer_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name,
					second_supervisor_code,second_supervisor_work_no,second_supervisor_name,if(work_no=second_supervisor_work_no,'自有','团队') as business_type
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt = ${hiveconf:current_end_day}
					and second_supervisor_code !=''
				) c on c.customer_id=a.customer_id
		group by 
			c.second_supervisor_code,
			c.business_type
		) i on i.second_supervisor_code=a.second_supervisor_code and i.business_type=a.business_type	
where
	c.province_name is not null
	and b.user_position='SALES_CITY_MANAGER'
)

insert overwrite table ${hiveconf:target_table} partition(sdt)			
			
select
	concat_ws('&',city_group_code,second_supervisor_work_no,business_type,${hiveconf:current_end_day}) as biz_id,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	second_supervisor_work_no as sales_manager_work_no,
	second_supervisor_name as sales_manager_name,
	begin_date,
	business_type,
	status,
	sales_target,
	sales_value,
	sales_target_achv_rate,
	profit_rate_target,
	profit_rate,
	profit_rate_achv_rate,
	performance_customers,
	new_sign_customers_target,
	new_sign_customers,
	new_sign_amount,
	new_sign_business,
	new_sign_business_amount,
	new_sign_customers_achv_rate,
	business_number_cnt,
	estimate_contract_amount,
	rank_number,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	${hiveconf:current_end_day} as sdt
from
	current_tmp_rank
;



INVALIDATE METADATA csx_tmp.report_sale_r_d_business_rank_sales_manager;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sale_r_d_business_rank_sales_manager  销售员指标

drop table if exists csx_tmp.report_sale_r_d_business_rank_sales_manager;
create table csx_tmp.report_sale_r_d_business_rank_sales_manager(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_group_code`                string              COMMENT    '城市编码',
`city_group_name`                string              COMMENT    '城市',
`sales_manager_work_no`          string              COMMENT    '销售经理工号',
`sales_manager_name`             string              COMMENT    '销售经理名称',
`begin_date`                     string              COMMENT    '入职日期',
`business_type`                  string              COMMENT    '类型',
`status`                         string              COMMENT    '状态',
`sales_target`                   decimal(15,2)       COMMENT    '销售额目标',
`sales_value`                    decimal(15,2)       COMMENT    '销售额',
`sales_target_achv_rate`         decimal(15,2)       COMMENT    '销售金额目标达成率',
`profit_rate_target`             decimal(15,2)       COMMENT    '定价毛利率目标',
`profit_rate`                    decimal(15,2)       COMMENT    '定价毛利率',
`profit_rate_achv_rate`          decimal(15,2)       COMMENT    '定价毛利率目标达成',
`performance_customers`          decimal(15,2)       COMMENT    '履约客户数',
`new_sign_customers_target`      decimal(15,2)       COMMENT    '新签客户数目标',
`new_sign_customers`             decimal(15,2)       COMMENT    '新签客户数',
`new_sign_amount`                decimal(15,2)       COMMENT    '签约金额',
`new_sign_business`              decimal(15,2)       COMMENT    '新签商机数',
`new_sign_business_amount`       decimal(15,2)       COMMENT    '新签商机金额',
`new_sign_customers_achv_rate`   decimal(15,2)       COMMENT    '新签客户数达成率',
`business_number_cnt`            decimal(15,2)       COMMENT    '商机数',
`estimate_contract_amount`       decimal(15,2)       COMMENT    '预计签约金额',
`rank_number`                    int                 COMMENT    '排名',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT '经理排名'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	

