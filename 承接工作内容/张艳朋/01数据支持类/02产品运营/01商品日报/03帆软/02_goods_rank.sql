--商品排名top10

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--今日
set i_sdate_0 =regexp_replace(current_date,'-','');

--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_goods_rank_day partition(sdt)

select 
	province_name,sales_city,goods_code,goods_name,sales_value,profit,profit_rate,customer_cnt,customer_rate,rn,
	${hiveconf:i_sdate_0} as update_time,
	substr(${hiveconf:i_sdate_11},1,6) as sdt
from 
	(
	select
		a.province_name,a.sales_city,a.goods_code,a.goods_name,
		a.sales_value,a.profit,a.profit_rate,a.customer_cnt,
		a.customer_cnt/b.customer_cnt as customer_rate,
		row_number()over(partition by a.sales_city order by a.sales_value desc) as rn
	from
		(
		select 
			province_name,sales_city,goods_code,regexp_replace(goods_name, '\n|\t|\r', '') as goods_name,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/sum(sales_value) as profit_rate,
			count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
			and attribute_code != 5
			and channel in ('1')
			and province_name not like '平台%'
		group by
			province_name,sales_city,goods_code,regexp_replace(goods_name, '\n|\t|\r', '')
		) a 
		left join
			(
			select
				province_name,sales_city,
				count(distinct customer_no) as customer_cnt
			from
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11}
				and attribute_code != 5
				and channel in ('1')
				and province_name not like '平台%'
			group by
				province_name,sales_city
			) b on b.sales_city=a.sales_city
	) t1
where
	rn<=10
;

INVALIDATE METADATA csx_tmp.ads_fr_goods_rank_day;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_goods_rank_day  盘点报损-计算

drop table if exists csx_tmp.ads_fr_goods_rank_day;
create table csx_tmp.ads_fr_goods_rank_day(
`province_name`            string              COMMENT    '省份名称',
`sales_city`               string              COMMENT    '城市',
`goods_code`               string              COMMENT    '商品编码',
`goods_name`               string              COMMENT    '商品名称',
`sales_value`              decimal(26,6)       COMMENT    '销售额',
`profit`                   decimal(26,6)       COMMENT    '定价毛利额',
`profit_rate`              decimal(26,6)       COMMENT    '定价毛利率',
`customer_cnt`             decimal(26,6)       COMMENT    '客户数',
`customer_rate`            decimal(26,6)       COMMENT    '客户渗透率',
`rn`                       string              COMMENT    '排名',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'zhangyanpeng:商品日报-商品排名'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	
	