-- 华北日报 
-- 核心逻辑： 按业务类型统计出库、回单、在途情况

-- 切换tez计算引擎
set mapred.job.name=report_sales_r_m_customer_new_regular;
SET hive.execution.engine=mr;
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
set target_table=csx_tmp.report_sales_r_m_customer_new_regular;
	
with current_customer_sales as 	
(
	select
		a.region_code, -- 大区编码
		a.region_name, -- 大区名称
		a.province_code, -- 省区编码
		a.province_name, -- 省区名称
		a.customer_no, --客户编码
		case when a.channel_code in ('1','7','9') then 'B端' when a.channel_code in('2') then 'M端' else '其他' end as channel_type, -- 业务类型
		case when a.business_type_code in ('3','5') then 'B端其他' when a.business_type_code in ('9') then 'M端' else a.business_type_name end as channel_type_detail, -- 业务类型详情
		a.sales_value, -- 销售额
		a.profit, -- 毛利额
		b.first_order_date -- 首单日期
	from
		(
		select
			customer_no,region_code,region_name,province_code,province_name,channel_code,business_type_code,business_type_name,sales_value,profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:current_start_day}
			and sdt<=${hiveconf:current_day}
			and channel_code in ('1','2','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		) a
		left join
			(
			select 
				customer_no,customer_name,first_order_date
			from 
				csx_dw.dws_crm_w_a_customer_active
			where 
				sdt = 'current'
			group by 
				customer_no,customer_name,first_order_date
			) b on b.customer_no = a.customer_no
)

insert overwrite table ${hiveconf:target_table} partition(month)			
			
select
	region_code, -- 大区编码
	region_name, -- 大区名称
	province_code, -- 省区编码
	province_name, -- 省区名称
	channel_type,
	channel_type_detail,
	coalesce(customer_regular_cnt,0) as customer_regular_cnt, -- 老客数量
	coalesce(customer_new_cnt,0) as customer_new_cnt, -- 新客数量
	coalesce(customer_regular_cnt+customer_new_cnt,0) as customer_total_cnt, -- 客户合计
	coalesce(customer_regular_sales_value,0) as customer_regular_sales_value, -- 老客销售额
	coalesce(customer_new_sales_value,0) as customer_new_sales_value, -- 新客销售额
	coalesce(customer_regular_sales_value+customer_new_sales_value) as customer_total_sales_value, -- 销售额合计
	coalesce(customer_regular_profit,0) as customer_regular_profit, -- 老客毛利额
	coalesce(customer_new_profit,0) as customer_new_profit, -- 新客毛利额
	coalesce(customer_regular_profit+customer_new_profit) as customer_total_profit, -- 毛利额合计	
	coalesce(customer_regular_profit/abs(customer_regular_sales_value),0) as customer_regular_profit_rate, -- 老客定价毛利率
	coalesce(customer_new_profit/abs(customer_new_sales_value),0) as customer_new_profit_rate, -- 新客定价毛利率
	coalesce(total_profit/abs(total_sales_value),0) as total_profit_rate, -- 总毛利率
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr(${hiveconf:current_day}, 1, 6) as month -- 月份
from
	(
	select
		region_code, -- 大区编码
		region_name, -- 大区名称
		province_code, -- 省区编码
		province_name, -- 省区名称
		channel_type,
		channel_type_detail,
		count(distinct case when first_order_date<${hiveconf:current_start_day} then customer_no else null end) as customer_regular_cnt,
		count(distinct case when first_order_date>=${hiveconf:current_start_day} and first_order_date<=${hiveconf:current_day} then customer_no else null end) as customer_new_cnt,
		sum(case when first_order_date<${hiveconf:current_start_day} then sales_value else 0 end) as customer_regular_sales_value,
		sum(case when first_order_date>=${hiveconf:current_start_day} and first_order_date<=${hiveconf:current_day} then sales_value else 0 end) as customer_new_sales_value,
		sum(case when first_order_date<${hiveconf:current_start_day} then profit else 0 end) as customer_regular_profit,
		sum(case when first_order_date>=${hiveconf:current_start_day} and first_order_date<=${hiveconf:current_day} then profit else 0 end) as customer_new_profit,
		sum(sales_value) as total_sales_value,
		sum(profit) as total_profit
	from
		current_customer_sales
	group by 
		region_code, -- 大区编码
		region_name, -- 大区名称
		province_code, -- 省区编码
		province_name, -- 省区名称		
		channel_type,
		channel_type_detail
	union all
	select
		region_code, -- 大区编码
		region_name, -- 大区名称
		province_code, -- 省区编码
		province_name, -- 省区名称		
		channel_type,
		'B端小计' as channel_type_detail,
		count(distinct case when first_order_date<${hiveconf:current_start_day} then customer_no else null end) as customer_regular_cnt,
		count(distinct case when first_order_date>=${hiveconf:current_start_day} and first_order_date<=${hiveconf:current_day} then customer_no else null end) as customer_new_cnt,
		sum(case when first_order_date<${hiveconf:current_start_day} then sales_value else 0 end) as customer_regular_sales_value,
		sum(case when first_order_date>=${hiveconf:current_start_day} and first_order_date<=${hiveconf:current_day} then sales_value else 0 end) as customer_new_sales_value,
		sum(case when first_order_date<${hiveconf:current_start_day} then profit else 0 end) as customer_regular_profit,
		sum(case when first_order_date>=${hiveconf:current_start_day} and first_order_date<=${hiveconf:current_day} then profit else 0 end) as customer_new_profit,
		sum(sales_value) as total_sales_value,
		sum(profit) as total_profit
	from
		current_customer_sales
	where
		channel_type='B端'
	group by 
		region_code, -- 大区编码
		region_name, -- 大区名称
		province_code, -- 省区编码
		province_name, -- 省区名称			
		channel_type
	) as t1
;



INVALIDATE METADATA csx_tmp.report_sales_r_m_customer_new_regular;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sales_r_m_customer_new_regular  华北日报 回单统计

drop table if exists csx_tmp.report_sales_r_m_customer_new_regular;
create table csx_tmp.report_sales_r_m_customer_new_regular(
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`channel_type`                   string              COMMENT    '业务类型',
`channel_type_detail`            string              COMMENT    '业务类型详情',
`customer_regular_cnt`           int                 COMMENT    '老客数量',
`customer_new_cnt`               int                 COMMENT    '新客数量',
`customer_total_cnt`             int                 COMMENT    '客户合计数量',
`customer_regular_sales_value`   decimal(26,6)       COMMENT    '老客销售额',
`customer_new_sales_value`       decimal(26,6)       COMMENT    '新客销售额',
`customer_total_sales_value`     decimal(26,6)       COMMENT    '客户总销售额',
`customer_regular_profit`        decimal(26,6)       COMMENT    '老客毛利额',
`customer_new_profit`            decimal(26,6)       COMMENT    '新客毛利额',
`customer_total_profit`          decimal(26,6)       COMMENT    '总毛利额',
`customer_regular_profit_rate`   decimal(26,6)       COMMENT    '老客定价毛利率',
`customer_new_profit_rate`       decimal(26,6)       COMMENT    '新客定价毛利率',
`total_profit_rate`              decimal(26,6)       COMMENT    '总定价毛利率',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:华北日报-新老客统计'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	