
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
	
with current_customer_sales as 	
(
	select
		a.performance_region_code, -- 大区编码
		a.performance_region_name, -- 大区名称
		a.performance_province_code, -- 省区编码
		a.performance_province_name, -- 省区名称
		a.performance_city_code,
		a.performance_city_name,
		a.customer_code, --客户编码
		case when a.channel_code in ('1','7','9') then 'B端' when a.channel_code in('2') then 'M端' else '其他' end as channel_type, -- 业务类型
		case when a.business_type_code in ('3','5') then 'B端其他' when a.business_type_code in ('9') then 'M端' else a.business_type_name end as channel_type_detail, -- 业务类型详情
		a.sale_amt, -- 销售额
		a.profit, -- 毛利额
		b.first_sale_date -- 首单日期
	from
		(
		select
			customer_code,channel_code,business_type_code,business_type_name,sale_amt,profit,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>=regexp_replace(trunc('${ytd_date}','MM'),'-','')
			and sdt<='${ytd}'
			and channel_code in ('1','2','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		) a
		left join
			(
			select 
				customer_code,customer_name,first_sale_date
			from 
				csx_dws.csx_dws_crm_customer_active_di
			where 
				sdt = 'current'
			group by 
				customer_code,customer_name,first_sale_date
			) b on b.customer_code = a.customer_code
)

insert overwrite table csx_analyse.csx_analyse_fr_sale_customer_new_regular_mi partition(month)			
			
select
	performance_region_code, -- 大区编码
	performance_region_name, -- 大区名称
	performance_province_code, -- 省区编码
	performance_province_name, -- 省区名称
	performance_city_code,
	performance_city_name,
	channel_type,
	channel_type_detail,
	coalesce(customer_regular_cnt,0) as customer_regular_cnt, -- 老客数量
	coalesce(customer_new_cnt,0) as customer_new_cnt, -- 新客数量
	coalesce(customer_regular_cnt+customer_new_cnt,0) as customer_total_cnt, -- 客户合计
	coalesce(customer_regular_sale_amt,0) as customer_regular_sale_amt, -- 老客销售额
	coalesce(customer_new_sale_amt,0) as customer_new_sale_amt, -- 新客销售额
	coalesce(customer_regular_sale_amt+customer_new_sale_amt) as customer_total_sale_amt, -- 销售额合计
	coalesce(customer_regular_profit,0) as customer_regular_profit, -- 老客毛利额
	coalesce(customer_new_profit,0) as customer_new_profit, -- 新客毛利额
	coalesce(customer_regular_profit+customer_new_profit) as customer_total_profit, -- 毛利额合计	
	coalesce(customer_regular_profit/abs(customer_regular_sale_amt),0) as customer_regular_profit_rate, -- 老客定价毛利率
	coalesce(customer_new_profit/abs(customer_new_sale_amt),0) as customer_new_profit_rate, -- 新客定价毛利率
	coalesce(total_profit/abs(total_sale_amt),0) as total_profit_rate, -- 总毛利率
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr('${ytd}', 1, 6) as month -- 月份
from
	(
	select
		performance_region_code, -- 大区编码
		performance_region_name, -- 大区名称
		performance_province_code, -- 省区编码
		performance_province_name, -- 省区名称
		performance_city_code,
		performance_city_name,
		channel_type,
		channel_type_detail,
		count(distinct case when first_sale_date<regexp_replace(trunc('${ytd_date}','MM'),'-','') then customer_code else null end) as customer_regular_cnt,
		count(distinct case when first_sale_date>=regexp_replace(trunc('${ytd_date}','MM'),'-','') and first_sale_date<='${ytd}' then customer_code else null end) as customer_new_cnt,
		sum(case when first_sale_date<regexp_replace(trunc('${ytd_date}','MM'),'-','') then sale_amt else 0 end) as customer_regular_sale_amt,
		sum(case when first_sale_date>=regexp_replace(trunc('${ytd_date}','MM'),'-','') and first_sale_date<='${ytd}' then sale_amt else 0 end) as customer_new_sale_amt,
		sum(case when first_sale_date<regexp_replace(trunc('${ytd_date}','MM'),'-','') then profit else 0 end) as customer_regular_profit,
		sum(case when first_sale_date>=regexp_replace(trunc('${ytd_date}','MM'),'-','') and first_sale_date<='${ytd}' then profit else 0 end) as customer_new_profit,
		sum(sale_amt) as total_sale_amt,
		sum(profit) as total_profit
	from
		current_customer_sales
	group by 
		performance_region_code, -- 大区编码
		performance_region_name, -- 大区名称
		performance_province_code, -- 省区编码
		performance_province_name, -- 省区名称
		performance_city_code,
		performance_city_name,		
		channel_type,
		channel_type_detail
	union all
	select
		performance_region_code, -- 大区编码
		performance_region_name, -- 大区名称
		performance_province_code, -- 省区编码
		performance_province_name, -- 省区名称	
		performance_city_code,
		performance_city_name,	
		channel_type,
		'B端小计' as channel_type_detail,
		count(distinct case when first_sale_date<regexp_replace(trunc('${ytd_date}','MM'),'-','') then customer_code else null end) as customer_regular_cnt,
		count(distinct case when first_sale_date>=regexp_replace(trunc('${ytd_date}','MM'),'-','') and first_sale_date<='${ytd}' then customer_code else null end) as customer_new_cnt,
		sum(case when first_sale_date<regexp_replace(trunc('${ytd_date}','MM'),'-','') then sale_amt else 0 end) as customer_regular_sale_amt,
		sum(case when first_sale_date>=regexp_replace(trunc('${ytd_date}','MM'),'-','') and first_sale_date<='${ytd}' then sale_amt else 0 end) as customer_new_sale_amt,
		sum(case when first_sale_date<regexp_replace(trunc('${ytd_date}','MM'),'-','') then profit else 0 end) as customer_regular_profit,
		sum(case when first_sale_date>=regexp_replace(trunc('${ytd_date}','MM'),'-','') and first_sale_date<='${ytd}' then profit else 0 end) as customer_new_profit,
		sum(sale_amt) as total_sale_amt,
		sum(profit) as total_profit
	from
		current_customer_sales
	where
		channel_type='B端'
	group by 
		performance_region_code, -- 大区编码
		performance_region_name, -- 大区名称
		performance_province_code, -- 省区编码
		performance_province_name, -- 省区名称	
		performance_city_code,
		performance_city_name,		
		channel_type
	) as t1
;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sale_customer_new_regular_mi

drop table if exists csx_analyse.csx_analyse_fr_sale_customer_new_regular_mi;
create table csx_analyse.csx_analyse_fr_sale_customer_new_regular_mi(
`performance_region_code`        string              COMMENT    '业绩大区编码',
`performance_region_name`        string              COMMENT    '业绩大区名称',
`performance_province_code`      string              COMMENT    '业绩省区编码',
`performance_province_name`      string              COMMENT    '业绩省区名称',
`performance_city_code`          string              COMMENT    '业绩城市编码',
`performance_city_name`          string              COMMENT    '业绩城市名称',
`channel_type`                   string              COMMENT    '业务类型',
`channel_type_detail`            string              COMMENT    '业务类型详情',
`customer_regular_cnt`           int                 COMMENT    '老客数量',
`customer_new_cnt`               int                 COMMENT    '新客数量',
`customer_total_cnt`             int                 COMMENT    '客户合计数量',
`customer_regular_sale_amt`      decimal(26,6)       COMMENT    '老客销售额',
`customer_new_sale_amt`          decimal(26,6)       COMMENT    '新客销售额',
`customer_total_sale_amt`        decimal(26,6)       COMMENT    '客户总销售额',
`customer_regular_profit`        decimal(26,6)       COMMENT    '老客毛利额',
`customer_new_profit`            decimal(26,6)       COMMENT    '新客毛利额',
`customer_total_profit`          decimal(26,6)       COMMENT    '总毛利额',
`customer_regular_profit_rate`   decimal(26,6)       COMMENT    '老客定价毛利率',
`customer_new_profit_rate`       decimal(26,6)       COMMENT    '新客定价毛利率',
`total_profit_rate`              decimal(26,6)       COMMENT    '总定价毛利率',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT '各大区日报新-新老客统计'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	
