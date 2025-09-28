-- 销售主题-业绩报表/财务主题-财务管报一体化表
-- 核心逻辑： 统计所有客户销售业绩
-- 更新范围： 更新近两个月数据

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;

with last_sale_order as 
(
	select 
		performance_region_code,
		performance_region_name,
		performance_province_code,
		performance_province_name,
		performance_city_code,
		performance_city_name,
		channel_code,     -- 客户渠道编码
		channel_name,     -- 客户渠道名称
		customer_code,
		sdt,
		sum(sale_amt) as sale_amt,  
		sum(sale_cost) as sale_cost,
		sum(profit) as profit,
		sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(sale_cost_no_tax) as sale_cost_no_tax,
		sum(profit_no_tax) as profit_no_tax    
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and sdt<='${ytd}'
		-- 排除特殊订单, 个性化需求
		and (order_code not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
          'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') 
		or order_code is null)
	group by 
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,channel_code,
		channel_name,customer_code,sdt
),
last_active_customer as 
(
	select 
		customer_code,
		customer_name,  
		sign_date,    -- 签约日期
		first_sale_date    -- 第一次销售日期
	from 
		csx_dws.csx_dws_crm_customer_active_di
	where 
		sdt='${ytd}'
)

insert overwrite table csx_analyse.csx_analyse_report_sale_achievement_mi partition(sdt)

select 
	concat_ws('&',t1.performance_city_code,t1.channel_code,t1.customer_code,t1.sdt) as biz_id,
	t1.performance_region_code,
	t1.performance_region_name,
	t1.performance_province_code,
	t1.performance_province_name,
	t1.performance_city_code,
	t1.performance_city_name,
	t1.channel_code,
	t1.channel_name,
	t1.customer_code,
	t2.customer_name,
	'' as attribute_code,
	if(substr(t2.sign_date,1,6)=substr(t1.sdt,1,6),'是', '否') is_new_sign,
	if(substr(t2.first_sale_date,1,6)=substr(t1.sdt,1,6),'是', '否') is_new_sale,  
	t1.sale_amt,
	t1.sale_cost,
	t1.profit,
	t1.sale_amt_no_tax,
	t1.sale_cost_no_tax,
	t1.profit_no_tax,
	t1.sdt
from 
	last_sale_order t1 
	left join last_active_customer t2 on t1.customer_code = t2.customer_code;


-- drop table csx_tmp.tmp_report_sale_r_d_achievement_etl;
-- create table csx_tmp.tmp_report_sale_r_d_achievement_etl 
-- as 
-- select 
--   *
-- from csx_analyse.csx_analyse_report_sale_achievement_mi
-- where sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and sdt<='${ytd}';

/*
create table csx_analyse.csx_analyse_report_sale_achievement_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`channel_code`                   string              COMMENT    '渠道编码',
`channel_name`                   string              COMMENT    '渠道名称',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`attribute_code`                 string              COMMENT    '客户属性',
`is_new_sign`                    string              COMMENT    '是否新签约客户',
`is_new_sale`                    string              COMMENT    '是否新下单客户',
`sale_amt`                       decimal(20,6)       COMMENT    '销售额',
`sale_cost`                      decimal(20,6)       COMMENT    '销售成本',
`profit`                         decimal(20,6)       COMMENT    '毛利',
`sale_amt_no_tax`                decimal(20,6)       COMMENT    '不含税销售额',
`sale_cost_no_tax`               decimal(20,6)       COMMENT    '不含税成本',
`profit_no_tax`                  decimal(20,6)       COMMENT    '不含税毛利'

) COMMENT '财报管报-业绩达成'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	