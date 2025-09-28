-- OKR数据-断约客户 
-- 核心逻辑： 

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_break_contract_customer;
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

-- 目标表
set target_table=csx_tmp.report_sale_r_m_break_contract_customer;
	
with current_break_contract_customer as 	
(
select
	a.region_code,
	c.region_name,
	a.province_code,
	c.province_name,
	a.city_group_code,
	c.city_group_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.cooperation_mode_name,
	b.sign_date,
	e.normal_last_order_date,
	a.days_cnt,
	a.sales_value,
	a.profit,
	a.profit_rate,
	f.receivable_amount,
	f.overdue_amount,
	f.overdue_amount_90,
	case when d.customer_no is null then 1 else 0 end as cur_qtr_break_contract_flag
from
	(
	select
		region_code,province_code,city_group_code,customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		count(distinct sdt) as days_cnt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' 
		and sdt <= '20210331'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and sales_type !='fanli'
	group by 
		region_code,province_code,city_group_code,customer_no
	) as a
	left join -- 客户信息
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split(sign_time,' ')[0],'-','') as sign_date,cooperation_mode_code,cooperation_mode_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split(sign_time,' ')[0],'-',''),cooperation_mode_code,cooperation_mode_name
		) b on a.customer_no=b.customer_no
	left join -- 区域信息
		(
		select 
			city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			city_group_code,city_group_name,province_code,province_name,region_code,region_name
		) c on c.city_group_code=a.city_group_code	
	left join -- 本季度是否下日配单 判断是否断约
		(
		select 
			customer_no
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210401'
			and sdt<='20210630'
			and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_type !='fanli'
		group by 
			customer_no
		) d on d.customer_no=a.customer_no
	left join -- 最后一次下日配单
		(
		select 
			customer_no,normal_last_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
		group by 
			customer_no,normal_last_order_date
		) e on e.customer_no=a.customer_no
	left join -- 应收逾期
		(
		select 
			customer_no,
			sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount, -- 应收金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount-overdue_amount1-overdue_amount15-overdue_amount30-overdue_amount60 else 0 end) overdue_amount_90	-- 逾期90天以上金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_day}
		group by 
			customer_no
		) f on f.customer_no=a.customer_no
where
	b.cooperation_mode_code='01' -- 合作模式编码(01长期客户,02一次性客户)
	and d.customer_no is null -- 本季度断约
)

insert overwrite table ${hiveconf:target_table} partition(quarter)			
			
select
	'' as biz_id,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	customer_no,
	customer_name,
	work_no,
	sales_name,
	first_category_name,
	second_category_name,
	third_category_name,
	cooperation_mode_name,
	sign_date,
	normal_last_order_date,
	coalesce(days_cnt,0) as last_qtr_days_cnt,
	coalesce(sales_value,0) as last_qtr_sales_value,
	coalesce(profit,0) as last_qtr_profit,
	coalesce(profit_rate,0) as last_qtr_profit_rate,
	coalesce(receivable_amount,0) as receivable_amount,
	coalesce(overdue_amount,0) as overdue_amount,
	coalesce(overdue_amount_90,0) as overdue_amount_90,
	cur_qtr_break_contract_flag,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	'202102' as quarter -- 月份
from
	current_break_contract_customer	
;



INVALIDATE METADATA csx_tmp.report_sale_r_m_break_contract_customer;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sale_r_m_break_contract_customer  OKR数据 断约客户

drop table if exists csx_tmp.report_sale_r_m_break_contract_customer;
create table csx_tmp.report_sale_r_m_break_contract_customer(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`first_category_name`            string              COMMENT    '一级分类名称',
`second_category_name`           string              COMMENT    '二级分类名称',
`third_category_name`            string              COMMENT    '三级分类名称',
`cooperation_mode_name`          string              COMMENT    '合作模式',
`sign_date`                      string              COMMENT    '签约日期',
`normal_last_order_date`         string              COMMENT    '最后下日配单日期',
`last_qtr_days_cnt`              int                 COMMENT    '上季度日配单下单次数',
`last_qtr_sales_value`           decimal(26,6)       COMMENT    '上季度日配业务销售额',
`last_qtr_profit`                decimal(26,6)       COMMENT    '上季度日配业务定价毛利额',
`last_qtr_profit_rate`           decimal(26,6)       COMMENT    '上季度日配业务定价毛利率',
`receivable_amount`              decimal(26,6)       COMMENT    '应收账款',
`overdue_amount`                 decimal(26,6)       COMMENT    '逾期账款',
`overdue_amount_90`              decimal(26,6)       COMMENT    '逾期90天以上账款',
`cur_qtr_break_contract_flag`    int                 COMMENT    '本季度断约标识',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:OKR数据-断约客户'
PARTITIONED BY (quarter string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	