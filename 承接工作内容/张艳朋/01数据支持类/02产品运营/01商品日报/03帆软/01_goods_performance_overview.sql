--====================================================================================================================================================	
--商品简报 全国大客户日配商品模块业绩概览（MTD）

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--今日
set i_sdate_0 =regexp_replace(current_date,'-','');


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_goods_performance_overview_day partition(sdt)
	
select
	region_name,
	sales_city,
	--整体
	coalesce(sum(sales_value),0) as sales_value,
	coalesce(round(sum(profit)/abs(sum(sales_value)),8),0) as profit_rate,
	count(distinct customer_no) as customer_amount,
	coalesce(round(sum(sku_amount)/sum(sales_date_amount),1),0) as sku_date_customer_rate,
	--生鲜
	coalesce(sum(sx_sales_value),0) as sx_sales_value,
	coalesce(round(sum(sx_sales_value)/sum(sales_value),8),0) as sx_sales_value_rate,
	coalesce(round(sum(sx_profit)/abs(sum(sx_sales_value)),8),0) as sx_profit_rate,
	coalesce(round(count(distinct if(sx_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as sx_customer_rate,
	coalesce(round(sum(sx_sku_amount)/sum(sx_sales_date_amount),1),0) as sx_sku_date_customer_rate,
	--食百
	coalesce(sum(sb_sales_value),0) as sb_sales_value,
	coalesce(round(sum(sb_sales_value)/sum(sales_value),8),0) as sb_sales_value_rate,
	coalesce(round(sum(sb_profit)/abs(sum(sb_sales_value)),8),0) as sb_profit_rate,
	coalesce(round(count(distinct if(sb_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as sb_customer_rate,
	--coalesce(round(sum(sb_sku_amount)/sum(sb_sales_date_amount),1),0) as sb_sku_date_customer_rate,
	--非食品（用品类）
	coalesce(sum(n_food_sales_value),0) as n_food_sales_value,
	--coalesce(round(sum(n_food_sales_value)/sum(sales_value),8),0) as n_food_sales_value_rate,
	coalesce(round(sum(n_food_profit)/abs(sum(n_food_sales_value)),8),0) as n_food_profit_rate,
	coalesce(round(count(distinct if(n_food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as n_food_customer_rate,
	coalesce(round(sum(n_food_sku_amount)/sum(n_food_sales_date_amount),1),0) as n_food_sku_date_customer_rate,
	--食品（食品类）
	coalesce(sum(food_sales_value),0) as food_sales_value,
	--coalesce(round(sum(food_sales_value)/sum(sales_value),8),0) as food_sales_value_rate,
	coalesce(round(sum(food_profit)/abs(sum(food_sales_value)),8),0) as food_profit_rate,
	coalesce(round(count(distinct if(food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as food_customer_rate,
	coalesce(round(sum(food_sku_amount)/sum(food_sales_date_amount),1),0) as food_sku_date_customer_rate,
	${hiveconf:i_sdate_0} as update_time,
	substr(${hiveconf:i_sdate_11},1,6) as sdt
from
	(
	select
		region_name,
		sales_city,
		customer_no,
		--整体
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(sales_date_amount) as sales_date_amount,
		sum(sku_amount) as sku_amount,
		--生鲜
		sum(sx_sales_value) as sx_sales_value,
		sum(sx_profit) as sx_profit,
		sum(sx_customer_amount) as sx_customer_amount,
		sum(sx_sales_date_amount) as sx_sales_date_amount,
		sum(sx_sku_amount) as sx_sku_amount,
		--食百
		sum(sb_sales_value) as sb_sales_value,
		sum(sb_profit) as sb_profit,
		sum(sb_customer_amount) as sb_customer_amount,
		sum(sb_sales_date_amount) as sb_sales_date_amount,
		sum(sb_sku_amount) as sb_sku_amount,
		--非食品（用品类）
		sum(n_food_sales_value) as n_food_sales_value,
		sum(n_food_profit) as n_food_profit,
		sum(n_food_customer_amount) as n_food_customer_amount,
		sum(n_food_sales_date_amount) as n_food_sales_date_amount,
		sum(n_food_sku_amount) as n_food_sku_amount,
		--食品（食品类）
		sum(food_sales_value) as food_sales_value,
		sum(food_profit) as food_profit,
		sum(food_customer_amount) as food_customer_amount,
		sum(food_sales_date_amount) as food_sales_date_amount,
		sum(food_sku_amount) as food_sku_amount
	from
		(
		select
			region_name,
			sales_city,
			customer_no,
			sales_date,
			--整体
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			count(distinct sales_date) as sales_date_amount,
			count(distinct goods_code) as sku_amount,
			--生鲜
			sum(if(division_name in ('生鲜部','加工部'),sales_value,null)) as sx_sales_value,
			sum(if(division_name in ('生鲜部','加工部'),profit,null)) as sx_profit,
			count(distinct if(division_name in ('生鲜部','加工部'),customer_no,null)) as sx_customer_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),sales_date,null)) as sx_sales_date_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),goods_code,null)) as sx_sku_amount,
			--食百
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),sales_value,null)) as sb_sales_value,
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),profit,null)) as sb_profit,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),customer_no,null)) as sb_customer_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),sales_date,null)) as sb_sales_date_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),goods_code,null)) as sb_sku_amount,
			--非食品（用品类）
			sum(if(division_name in ('用品类','易耗品','服装'),sales_value,null)) as n_food_sales_value,
			sum(if(division_name in ('用品类','易耗品','服装'),profit,null)) as n_food_profit,
			count(distinct if(division_name in ('用品类','易耗品','服装'),customer_no,null)) as n_food_customer_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),sales_date,null)) as n_food_sales_date_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),goods_code,null)) as n_food_sku_amount,
			--食品（食品类）
			sum(if(division_name in ('食品类'),sales_value,null)) as food_sales_value,
			sum(if(division_name in ('食品类'),profit,null)) as food_profit,
			count(distinct if(division_name in ('食品类'),customer_no,null)) as food_customer_amount,
			count(distinct if(division_name in ('食品类'),sales_date,null)) as food_sales_date_amount,
			count(distinct if(division_name in ('食品类'),goods_code,null)) as food_sku_amount				
		from
			(
			select
				region.region_name,base.smonth,base.customer_no,base.province_code,base.province_name,base.sales_city,base.sales_date,base.goods_code,base.division_name,base.sales_value,base.profit
			from
				(
				select
					substr(sdt,1,6)smonth,customer_no,province_code,province_name,sales_city,sales_date,goods_code,division_name,sales_value,profit
				from
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
					and attribute_code != 5 --5为合伙人
					and channel in ('1') --大客户
					and province_name not like '平台%'
				) as base
				--大区
				left join 
					(
					select 
						province_code,province_name,region_code,region_name
					from 
						csx_dw.dim_area
					where
						area_rank='13'
					group by
						province_code,province_name,region_code,region_name
					) region on region.province_code= base.province_code
			) as base
		group by
			region_name,
			sales_city,
			customer_no,
			sales_date
		) as base
	group by
		region_name,
		sales_city,
		customer_no
	) as base
group by
	region_name,
	sales_city
	
union all

--商品简报 全国大客户日配商品模块业绩概览（MTD）	
select
	region_name,
	sales_city,
	--整体
	coalesce(sum(sales_value),0) as sales_value,
	coalesce(round(sum(profit)/abs(sum(sales_value)),8),0) as profit_rate,
	count(distinct customer_no) as customer_amount,
	coalesce(round(sum(sku_amount)/sum(sales_date_amount),1),0) as sku_date_customer_rate,
	--生鲜
	coalesce(sum(sx_sales_value),0) as sx_sales_value,
	coalesce(round(sum(sx_sales_value)/sum(sales_value),8),0) as sx_sales_value_rate,
	coalesce(round(sum(sx_profit)/abs(sum(sx_sales_value)),8),0) as sx_profit_rate,
	coalesce(round(count(distinct if(sx_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as sx_customer_rate,
	coalesce(round(sum(sx_sku_amount)/sum(sx_sales_date_amount),1),0) as sx_sku_date_customer_rate,
	--食百
	coalesce(sum(sb_sales_value),0) as sb_sales_value,
	coalesce(round(sum(sb_sales_value)/sum(sales_value),8),0) as sb_sales_value_rate,
	coalesce(round(sum(sb_profit)/abs(sum(sb_sales_value)),8),0) as sb_profit_rate,
	coalesce(round(count(distinct if(sb_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as sb_customer_rate,
	--coalesce(round(sum(sb_sku_amount)/sum(sb_sales_date_amount),1),0) as sb_sku_date_customer_rate,
	--非食品（用品类）
	coalesce(sum(n_food_sales_value),0) as n_food_sales_value,
	--coalesce(round(sum(n_food_sales_value)/sum(sales_value),8),0) as n_food_sales_value_rate,
	coalesce(round(sum(n_food_profit)/abs(sum(n_food_sales_value)),8),0) as n_food_profit_rate,
	coalesce(round(count(distinct if(n_food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as n_food_customer_rate,
	coalesce(round(sum(n_food_sku_amount)/sum(n_food_sales_date_amount),1),0) as n_food_sku_date_customer_rate,
	--食品（食品类）
	coalesce(sum(food_sales_value),0) as food_sales_value,
	--coalesce(round(sum(food_sales_value)/sum(sales_value),8),0) as food_sales_value_rate,
	coalesce(round(sum(food_profit)/abs(sum(food_sales_value)),8),0) as food_profit_rate,
	coalesce(round(count(distinct if(food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as food_customer_rate,
	coalesce(round(sum(food_sku_amount)/sum(food_sales_date_amount),1),0) as food_sku_date_customer_rate,
	${hiveconf:i_sdate_0} as update_time,
	substr(${hiveconf:i_sdate_11},1,6) as sdt
from
	(
	select
		region_name,
		sales_city,
		customer_no,
		--整体
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(sales_date_amount) as sales_date_amount,
		sum(sku_amount) as sku_amount,
		--生鲜
		sum(sx_sales_value) as sx_sales_value,
		sum(sx_profit) as sx_profit,
		sum(sx_customer_amount) as sx_customer_amount,
		sum(sx_sales_date_amount) as sx_sales_date_amount,
		sum(sx_sku_amount) as sx_sku_amount,
		--食百
		sum(sb_sales_value) as sb_sales_value,
		sum(sb_profit) as sb_profit,
		sum(sb_customer_amount) as sb_customer_amount,
		sum(sb_sales_date_amount) as sb_sales_date_amount,
		sum(sb_sku_amount) as sb_sku_amount,
		--非食品（用品类）
		sum(n_food_sales_value) as n_food_sales_value,
		sum(n_food_profit) as n_food_profit,
		sum(n_food_customer_amount) as n_food_customer_amount,
		sum(n_food_sales_date_amount) as n_food_sales_date_amount,
		sum(n_food_sku_amount) as n_food_sku_amount,
		--食品（食品类）
		sum(food_sales_value) as food_sales_value,
		sum(food_profit) as food_profit,
		sum(food_customer_amount) as food_customer_amount,
		sum(food_sales_date_amount) as food_sales_date_amount,
		sum(food_sku_amount) as food_sku_amount
	from
		(
		select
			'全国' as region_name,
			'全国' as sales_city,
			customer_no,
			sales_date,
			--整体
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			count(distinct sales_date) as sales_date_amount,
			count(distinct goods_code) as sku_amount,
			--生鲜
			sum(if(division_name in ('生鲜部','加工部'),sales_value,null)) as sx_sales_value,
			sum(if(division_name in ('生鲜部','加工部'),profit,null)) as sx_profit,
			count(distinct if(division_name in ('生鲜部','加工部'),customer_no,null)) as sx_customer_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),sales_date,null)) as sx_sales_date_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),goods_code,null)) as sx_sku_amount,
			--食百
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),sales_value,null)) as sb_sales_value,
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),profit,null)) as sb_profit,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),customer_no,null)) as sb_customer_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),sales_date,null)) as sb_sales_date_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),goods_code,null)) as sb_sku_amount,
			--非食品（用品类）
			sum(if(division_name in ('用品类','易耗品','服装'),sales_value,null)) as n_food_sales_value,
			sum(if(division_name in ('用品类','易耗品','服装'),profit,null)) as n_food_profit,
			count(distinct if(division_name in ('用品类','易耗品','服装'),customer_no,null)) as n_food_customer_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),sales_date,null)) as n_food_sales_date_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),goods_code,null)) as n_food_sku_amount,
			--食品（食品类）
			sum(if(division_name in ('食品类'),sales_value,null)) as food_sales_value,
			sum(if(division_name in ('食品类'),profit,null)) as food_profit,
			count(distinct if(division_name in ('食品类'),customer_no,null)) as food_customer_amount,
			count(distinct if(division_name in ('食品类'),sales_date,null)) as food_sales_date_amount,
			count(distinct if(division_name in ('食品类'),goods_code,null)) as food_sku_amount				
		from
			(
			select
				region.region_name,base.smonth,base.customer_no,base.province_code,base.province_name,base.sales_city,base.sales_date,base.goods_code,base.division_name,base.sales_value,base.profit
			from
				(
				select
					substr(sdt,1,6)smonth,customer_no,province_code,province_name,sales_city,sales_date,goods_code,division_name,sales_value,profit
				from
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
					and attribute_code != 5 --5为合伙人
					and channel in ('1') --大客户
					and province_name not like '平台%'
				) as base
				--大区
				left join 
					(
					select 
						province_code,province_name,region_code,region_name
					from 
						csx_dw.dim_area
					where
						area_rank='13'
					group by
						province_code,province_name,region_code,region_name
					) region on region.province_code= base.province_code		
			) as base
		group by
			region_name,
			sales_city,
			customer_no,
			sales_date
		) as base
	group by
		region_name,
		sales_city,
		customer_no
	) as base
	group by
		region_name,
		sales_city
;

INVALIDATE METADATA csx_tmp.ads_fr_goods_performance_overview_day;	


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_goods_performance_overview_day  盘点报损-计算

drop table if exists csx_tmp.ads_fr_goods_performance_overview_day;
create table csx_tmp.ads_fr_goods_performance_overview_day(
`region_name`                   string              COMMENT    '大区名称',
`sales_city`                    string              COMMENT    '城市',
`sales_value`                   decimal(26,6)       COMMENT    '销售额',
`profit_rate`                   decimal(26,6)       COMMENT    '定价毛利率',
`customer_amount`               decimal(26,6)       COMMENT    '客户数',
`sku_date_customer_rate`        decimal(26,6)       COMMENT    '销售sku数/日/客',
`sx_sales_value`                decimal(26,6)       COMMENT    '生鲜销售额',
`sx_sales_value_rate`           decimal(26,6)       COMMENT    '生鲜销售额占比',
`sx_profit_rate`                decimal(26,6)       COMMENT    '生鲜定价毛利率',
`sx_customer_rate`              decimal(26,6)       COMMENT    '生鲜客户渗透率',
`sx_sku_date_customer_rate`     decimal(26,6)       COMMENT    '生鲜销售sku数/日/客',
`sb_sales_value`                decimal(26,6)       COMMENT    '食百销售额',
`sb_sales_value_rate`           decimal(26,6)       COMMENT    '食百销售额占比',
`sb_profit_rate`                decimal(26,6)       COMMENT    '食百定价毛利率',
`sb_customer_rate`              decimal(26,6)       COMMENT    '食百客户渗透率',
`n_food_sales_value`            decimal(26,6)       COMMENT    '非食品销售额',
`n_food_profit_rate`            decimal(26,6)       COMMENT    '非食品定价毛利率',
`n_food_customer_rate`          decimal(26,6)       COMMENT    '非食品客户渗透率',
`n_food_sku_date_customer_rate` decimal(26,6)       COMMENT    '非食品销售sku数/日/客',
`food_sales_value`              decimal(26,6)       COMMENT    '食品销售额',
`food_profit_rate`              decimal(26,6)       COMMENT    '食品定价毛利率',
`food_customer_rate`            decimal(26,6)       COMMENT    '食品客户渗透率',
`food_sku_date_customer_rate`   decimal(26,6)       COMMENT    '食品销售sku数/日/客',
`update_time`                   string              COMMENT    '更新时间'
) COMMENT 'zhangyanpeng:商品日报-概览'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/		
		