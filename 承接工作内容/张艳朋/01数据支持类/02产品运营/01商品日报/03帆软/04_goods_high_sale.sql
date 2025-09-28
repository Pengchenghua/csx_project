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

insert overwrite table csx_tmp.ads_fr_goods_high_sale_day partition(sdt)


select 
	province_name,sales_city,division_type,department_name,goods_code,goods_name,
	sales_value,profit,profit_rate,sales_qty,customer_cnt,rn,
	${hiveconf:i_sdate_0} as update_time,
	substr(${hiveconf:i_sdate_11},1,6) as sdt
from 
	(
	select
		a.province_name,a.sales_city,a.division_type,a.department_name,a.goods_code,
		a.goods_name,a.sales_value,a.profit,a.profit_rate,a.sales_qty,a.customer_cnt,
		row_number()over(partition by a.sales_city order by a.sales_value desc) as rn
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
			goods_code,
			regexp_replace(goods_name, '\n|\t|\r', '') as goods_name,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/sum(sales_value) as profit_rate,
			sum(sales_qty) as sales_qty,
			count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt >= regexp_replace(date_sub(current_date,if(pmod(datediff(current_date, '1920-01-01') - 3, 7)=1,3,1)),'-','')
			and attribute_code != 5
			and channel in ('1')
			and province_name not like '平台%'
		group by 
			province_name,sales_city,
			(case when division_name in ('生鲜部','加工部') then '生鲜'
				when division_name in ('食品类') then '食品'
				when division_name in ('用品类','易耗品','服装') then '非食品'
				else '其他'
			end),
			department_name,
			goods_code,
			regexp_replace(goods_name, '\n|\t|\r', '')
		) a 
	)tmp1
where 
	rn<=10
;

INVALIDATE METADATA csx_tmp.ads_fr_goods_high_sale_day;	


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_goods_high_sale_day  盘点报损-计算

drop table if exists csx_tmp.ads_fr_goods_high_sale_day;
create table csx_tmp.ads_fr_goods_high_sale_day(
`province_name`            string              COMMENT    '省份名称',
`sales_city`               string              COMMENT    '城市',
`division_type`            string              COMMENT    '部类',
`department_name`          string              COMMENT    '课组',
`goods_code`               string              COMMENT    '商品编码',
`goods_name`               string              COMMENT    '商品名称',
`sales_value`              decimal(26,6)       COMMENT    '销售额',
`profit`                   decimal(26,6)       COMMENT    '定价毛利额',
`profit_rate`              decimal(26,6)       COMMENT    '定价毛利率',
`sales_qty`                decimal(26,6)       COMMENT    '销售数量',
`customer_cnt`             decimal(26,6)       COMMENT    '客户数',
`rn`                       decimal(26,6)       COMMENT    '排名',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'zhangyanpeng:商品日报-高销'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	