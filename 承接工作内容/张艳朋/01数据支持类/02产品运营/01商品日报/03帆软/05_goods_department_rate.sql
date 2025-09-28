--商品负毛利

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--今日
set i_sdate_0 =regexp_replace(current_date,'-','');

--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_goods_department_rate_day partition(sdt)


select
	a.province_name,a.sales_city,a.division_type,a.department_name,
	coalesce(a.sales_value/b.sales_value,0) as sales_value_rate,
	a.profit_rate,
	${hiveconf:i_sdate_0} as update_time,
	substr(${hiveconf:i_sdate_11},1,6) as sdt
from
	(
	select
		province_name,sales_city,
		(case when division_name in ('生鲜部','加工部') then '生鲜'
			when division_name in ('食品类') then '食品'
			when division_name in ('用品类','易耗品','服装') then '非食品'
			else '其他'
		end) as division_type,
		department_name,
		sum(sales_value) as sales_value,
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
		and attribute_code != 5
		and channel in ('1')
		and province_name not like '平台%'
	group by 
		province_name,sales_city,
		(case when division_name in ('生鲜部','加工部') then '生鲜'
			when division_name in ('食品类') then '食品'
			when division_name in ('用品类','易耗品','服装') then '非食品' else '其他'
		end),
		department_name
	) a 
	left join
		(
		select
			province_name,sales_city,sum(sales_value) as sales_value
		from
			csx_dw.dws_sale_r_d_customer_sale
		where
			sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
			and attribute_code != 5
			and channel in ('1')
			and province_name not like '平台%'
		group by
			province_name,sales_city
		) as b on a.sales_city=b.sales_city

;

INVALIDATE METADATA csx_tmp.ads_fr_goods_department_rate_day;	


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_goods_department_rate_day  盘点报损-计算

drop table if exists csx_tmp.ads_fr_goods_department_rate_day;
create table csx_tmp.ads_fr_goods_department_rate_day(
`province_name`            string              COMMENT    '省份名称',
`sales_city`               string              COMMENT    '城市',
`division_type`            string              COMMENT    '部类',
`department_name`          string              COMMENT    '课组',
`sales_rate`               decimal(26,6)       COMMENT    '销售占比',
`profit_rate`              decimal(26,6)       COMMENT    '定价毛利率',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'zhangyanpeng:商品日报-课组销售占比'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	