--品类毛利异常

-- 切换tez计算引擎
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

set one_day_ago = regexp_replace(date_sub(current_date,1),'-','');

set two_day_ago = regexp_replace(date_sub(current_date,2),'-','');

-- 目标表
set target_table=csx_tmp.report_sale_r_d_category_profit_anomaly;

set target_table2=csx_tmp.report_sale_r_d_category_profit_anomaly_goods;
	
with current_tmp_category_profit_anomaly as 	
(
select
	a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,
	coalesce(a.current_period_sales_value,0) as current_period_sales_value, --销售额
	coalesce(a.current_period_sales_value/sum(a.current_period_sales_value)over(partition by a.city_group_code),0) as sales_proportion, --销售额占比
	coalesce(a.current_period_sales_value/a.last_period_sales_value-1,0) as sales_value_chain_ratio, --销售额环比
	coalesce(a.current_period_profit_rate,0) as current_period_profit_rate,-- 毛利率
	coalesce(a.current_period_profit_rate,0)-coalesce(a.last_period_profit_rate,0) as profit_rate_chain_ratio,--毛利率环比
	coalesce(a.current_period_front_profit_rate,0) as current_period_front_profit_rate, --前端毛利率
	coalesce(a.current_period_avg_cost/a.last_period_avg_cost-1,0) as avg_cost_chain_ratio, -- 平均成本环比
	coalesce(a.current_period_avg_price/a.last_period_avg_price-1,0) as avg_price_chain_ratio, -- 平均售价环比
	row_number()over(partition by a.city_group_code order by current_period_sales_value desc) as rank_number
from
	(
	select
		b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,
		--前一天
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null)) current_period_sales_value,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.profit,null))/abs(sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null))) as current_period_profit_rate,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.front_profit,null))/abs(sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null))) as current_period_front_profit_rate,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_cost,null))/sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_qty,null)) as current_period_avg_cost,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null))/sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_qty,null)) as current_period_avg_price,
		--前两天
		sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null)) last_period_sales_value,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.profit,null))/abs(sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null))) as last_period_profit_rate,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.front_profit,null))/abs(sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null))) as last_period_front_profit_rate,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_cost,null))/sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_qty,null)) as last_period_avg_cost,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null))/sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_qty,null)) as last_period_avg_price
	from 
		(
		select 
			id,sdt,province_name,city_group_name,goods_code,customer_no,sales_value,sales_qty,profit,front_profit,sales_cost,dc_code
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:two_day_ago} and ${hiveconf:one_day_ago}
			and channel_code in ('1','7','9')
			and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and dc_code not in ('W0K4','W0Z7')
		)a
		left join
			(
			select 
				customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
			)b on b.customer_no=a.customer_no	
		left join   --商品表
			(
			select 
				goods_id,goods_name,unit_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
				category_small_code,category_small_name,brand_name,spu_goods_name,price_belt_type,standard
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt =${hiveconf:one_day_ago}
			) c on a.goods_code=c.goods_id		
	group by 
		b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name
	) a 
where
	current_period_profit_rate<0.05
	or coalesce(a.current_period_profit_rate,0)-coalesce(a.last_period_profit_rate,0)<0
) 

insert overwrite table ${hiveconf:target_table} partition(sdt)			
			
select
	concat_ws('&',city_group_code,classify_middle_code,${hiveconf:one_day_ago}) as biz_id,
	sales_region_code as region_code,
	sales_region_name as region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	current_period_sales_value,
	sales_proportion,
	sales_value_chain_ratio,
	current_period_profit_rate,
	profit_rate_chain_ratio,
	current_period_front_profit_rate,
	avg_cost_chain_ratio,
	avg_price_chain_ratio,
	rank_number,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	${hiveconf:one_day_ago} as sdt
from
	current_tmp_category_profit_anomaly
;


--品类毛利异常下的商品
	
drop table if exists csx_tmp.tmp_category_profit_anomaly_goods_middle;
create table csx_tmp.tmp_category_profit_anomaly_goods_middle
as
select
	a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,a.classify_small_code,classify_small_name,a.goods_code,a.goods_name,
	coalesce(current_period_sales_value,'') as current_period_sales_value,
	coalesce(current_period_sales_qty,'') as current_period_sales_qty,
	coalesce(current_period_profit_rate) as current_period_profit_rate,
	coalesce(current_period_profit_rate,0)-coalesce(last_period_profit_rate,0) as profit_rate_chain_ratio,
	coalesce(current_period_fact_price,'') as current_period_fact_price,
	coalesce(current_period_fact_price/last_period_fact_price-1,'') as fact_price_chain_ratio,
	coalesce(current_period_avg_cost,'') as current_period_avg_cost,
	coalesce(current_period_avg_cost/last_period_avg_cost-1,'') as avg_cost_chain_ratio,
	coalesce(current_period_avg_price,'') as current_period_avg_price,
	coalesce(current_period_avg_price/last_period_avg_price-1,'') as avg_price_chain_ratio,
	row_number()over(partition by a.city_group_code order by current_period_sales_value desc) as rank_number
from
	(
	select
		b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,classify_small_name,a.goods_code,c.goods_name,
		--前一天
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null)) current_period_sales_value,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_qty,null)) current_period_sales_qty,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.profit,null))/abs(sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null))) as current_period_profit_rate,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.front_profit,null))/abs(sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null))) as current_period_front_profit_rate,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_cost,null)) as current_period_sales_cost,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_cost,null))/sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_qty,null)) as current_period_avg_cost,
		sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null))/sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_qty,null)) as current_period_avg_price,
		sum(if(a.sdt=${hiveconf:one_day_ago},d.fact_price*a.sales_qty,null))/sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_qty,null)) as current_period_fact_price,
		--前两天
		sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null)) last_period_sales_value,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.profit,null))/abs(sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null))) as last_period_profit_rate,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.front_profit,null))/abs(sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null))) as last_period_front_profit_rate,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_cost,null))/sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_qty,null)) as last_period_avg_cost,
		sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null))/sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_qty,null)) as last_period_avg_price,
		sum(if(a.sdt=${hiveconf:two_day_ago},d.fact_price*a.sales_qty,null))/sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_qty,null)) as last_period_fact_price,
		row_number()over(partition by b.city_group_name,c.classify_middle_code order by sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null)) desc) as rank_number
	from 
		(
		select 
			id,sdt,province_name,city_group_name,goods_code,customer_no,sales_value,sales_qty,profit,front_profit,sales_cost,dc_code
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:two_day_ago} and ${hiveconf:one_day_ago}
			and channel_code in ('1','7','9')
			and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and dc_code not in ('W0K4','W0Z7')
		)a
		left join
			(
			select 
				customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
			)b on b.customer_no=a.customer_no	
		left join   --商品表
			(
			select 
				goods_id,goods_name,unit_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
				category_small_code,category_small_name,brand_name,spu_goods_name,price_belt_type,standard
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt =${hiveconf:one_day_ago}
			) c on a.goods_code=c.goods_id	
		left join 
			(
			select
				t2.goods_code,
				t2.credential_no,
				sum(t2.qty) as qty,
				round(sum(t2.amt)/sum(t2.qty),6) final_price,--出厂价
				round(sum(t3.fact_price*t2.qty)/sum(case when t3.fact_price is not null then t2.qty end),6) fact_price --原料入库价
			from 
				(
				select
					goods_code,
					credential_no,
					source_order_no,
					sum(amt) amt,
					sum(qty) as qty
				from 
					csx_dw.dws_wms_r_d_batch_detail
				where 
					sdt >= '20210801' 
					and sdt<=${hiveconf:one_day_ago}
					and move_type in ('107A', '108A')
				group by 
					goods_code, credential_no, source_order_no
				)t2 
				left join 
					(
					select 
						goods_code,
						order_code,
						sdt,
						round(sum(fact_values)/sum(goods_reality_receive_qty),6) as fact_price
					from 
						csx_dw.dws_mms_r_a_factory_order
					where 
						sdt >= '20210801' 
						and sdt<=${hiveconf:one_day_ago}
						and mrp_prop_key in('3061','3010')
					group by 
						goods_code, order_code,sdt
					)t3 on t2.source_order_no = t3.order_code and t2.goods_code = t3.goods_code
			group by 
				t2.goods_code,
				t2.credential_no
			) d on split(a.id,'&')[0]=d.credential_no and a.goods_code=d.goods_code	
		join
			(
			select
				classify_middle_code,classify_middle_name
			from
				csx_tmp.report_sale_r_d_category_profit_anomaly 
			group by 
				classify_middle_code,classify_middle_name
			) e on e.classify_middle_code=c.classify_middle_code
	group by 
		b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,classify_small_name,a.goods_code,c.goods_name
	) a 
where
	rank_number<=50
;

drop table if exists csx_tmp.tmp_category_profit_anomaly_goods_middle_2;
create table csx_tmp.tmp_category_profit_anomaly_goods_middle_2
as
select
	a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.goods_code,
	concat_ws(' ',a.customer_no,a.customer_name,concat(cast(a.current_period_sales_value as string),'元'),concat(cast(a.current_period_avg_price as string),'元'),concat(a.avg_price_chain_ratio*100,'%')) as avg_price_down_top_customers
from
	(
	select
		a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.goods_code,
		a.customer_no,a.customer_name,
		round(a.current_period_sales_value,0) as current_period_sales_value, --销售额
		round(a.current_period_avg_price,1) as current_period_avg_price,
		coalesce(round(a.current_period_avg_price/a.last_period_avg_price-1,3),0) as avg_price_chain_ratio, -- 平均售价环比
		row_number()over(partition by a.city_group_code,a.goods_code order by current_period_sales_value desc) as rank_number
	from
		(
		select
			b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,a.goods_code,
			a.customer_no,b.customer_name,
			--前一天
			sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null)) current_period_sales_value,
			sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_value,null))/sum(if(a.sdt=${hiveconf:one_day_ago},a.sales_qty,null)) as current_period_avg_price,
			--前两天
			sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null)) last_period_sales_value,
			sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_value,null))/sum(if(a.sdt=${hiveconf:two_day_ago},a.sales_qty,null)) as last_period_avg_price
		from 
			(
			select 
				id,sdt,province_name,city_group_name,goods_code,customer_no,sales_value,sales_qty,profit,front_profit,sales_cost,dc_code
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between ${hiveconf:two_day_ago} and ${hiveconf:one_day_ago}
				and channel_code in ('1','7','9')
				and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
				and dc_code not in ('W0K4','W0Z7')
			)a
			left join
				(
				select 
					customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
					sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					sdt = 'current'
				)b on b.customer_no=a.customer_no	
			left join   --商品表
				(
				select 
					goods_id,goods_name,unit_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
					category_small_code,category_small_name,brand_name,spu_goods_name,price_belt_type,standard
				from 
					csx_dw.dws_basic_w_a_csx_product_m
				where 
					sdt =${hiveconf:one_day_ago}
				) c on a.goods_code=c.goods_id		
		group by 
			b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,a.goods_code,
			a.customer_no,b.customer_name
		) a 
	where
		a.current_period_avg_price<a.last_period_avg_price
	) a 
where
	a.rank_number<=10
;

insert overwrite table ${hiveconf:target_table2} partition(sdt)			
			
select
	-- concat_ws('&',a.city_group_code,a.goods_code,${hiveconf:one_day_ago}) as biz_id,
	'' as biz_id,
	a.sales_region_code as region_code,
	a.sales_region_name as region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.classify_small_code,
	a.classify_small_name,
	a.goods_code,
	a.goods_name,
	a.current_period_sales_value,
	a.current_period_sales_qty,
	a.current_period_profit_rate,
	a.profit_rate_chain_ratio,
	a.current_period_fact_price,
	a.fact_price_chain_ratio,
	a.current_period_avg_cost,
	a.avg_cost_chain_ratio,
	a.current_period_avg_price,
	a.avg_price_chain_ratio,
	coalesce(b.avg_price_down_top_customers,'') as avg_price_down_top_customers,
	a.rank_number,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	${hiveconf:one_day_ago} as sdt
from
	csx_tmp.tmp_category_profit_anomaly_goods_middle a 
	left join csx_tmp.tmp_category_profit_anomaly_goods_middle_2 b on a.city_group_code=b.city_group_code and a.goods_code=b.goods_code
		
;

	
--INVALIDATE METADATA csx_tmp.report_sale_r_d_category_profit_anomaly;
--INVALIDATE METADATA csx_tmp.report_sale_r_d_category_profit_anomaly_goods;
	

/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sale_r_d_category_profit_anomaly  品类毛利异常

drop table if exists csx_tmp.report_sale_r_d_category_profit_anomaly;
create table csx_tmp.report_sale_r_d_category_profit_anomaly(
`biz_id`                             string              COMMENT    '业务主键',
`region_code`                        string              COMMENT    '大区编码',
`region_name`                        string              COMMENT    '大区名称',
`province_code`                      string              COMMENT    '省区编码',
`province_name`                      string              COMMENT    '省区名称',
`city_group_code`                    string              COMMENT    '城市编码',
`city_group_name`                    string              COMMENT    '城市',
`classify_large_code`                string              COMMENT    '管理大类编码',
`classify_large_name`                string              COMMENT    '管理大类名称',
`classify_middle_code`               string              COMMENT    '管理中类编码',
`classify_middle_name`               string              COMMENT    '管理中类名称',
`current_period_sales_value`         decimal(15,6)       COMMENT    '销售额',
`sales_proportion`                   decimal(15,6)       COMMENT    '销售额占比',
`sales_value_chain_ratio`            decimal(15,6)       COMMENT    '销售额环比',
`current_period_profit_rate`         decimal(15,6)       COMMENT    '定价毛利率',
`profit_rate_chain_ratio`            decimal(15,6)       COMMENT    '定价毛利率环比',
`current_period_front_profit_rate`   decimal(15,6)       COMMENT    '前端毛利率',
`avg_cost_chain_ratio`               decimal(15,6)       COMMENT    '平均成本环比',
`avg_price_chain_ratio`              decimal(15,6)       COMMENT    '平均售价环比',
`rank_number`                        int                 COMMENT    '排名',
`update_time`                        string              COMMENT    '数据更新时间'
) COMMENT '品类毛利异常'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sale_r_d_category_profit_anomaly_goods  品类毛利异常商品

drop table if exists csx_tmp.report_sale_r_d_category_profit_anomaly_goods;
create table csx_tmp.report_sale_r_d_category_profit_anomaly_goods(
`biz_id`                             string              COMMENT    '业务主键',
`region_code`                        string              COMMENT    '大区编码',
`region_name`                        string              COMMENT    '大区名称',
`province_code`                      string              COMMENT    '省区编码',
`province_name`                      string              COMMENT    '省区名称',
`city_group_code`                    string              COMMENT    '城市编码',
`city_group_name`                    string              COMMENT    '城市',
`classify_large_code`                string              COMMENT    '管理大类编码',
`classify_large_name`                string              COMMENT    '管理大类名称',
`classify_middle_code`               string              COMMENT    '管理中类编码',
`classify_middle_name`               string              COMMENT    '管理中类名称',
`classify_small_code`                string              COMMENT    '管理小类编码',
`classify_small_name`                string              COMMENT    '管理小类名称',
`goods_code`                         string              COMMENT    '商品编码',
`goods_name`                         string              COMMENT    '商品名称',
`current_period_sales_value`         decimal(15,6)       COMMENT    '销售额',
`current_period_sales_qty`           decimal(15,6)       COMMENT    '销售额数量',
`current_period_profit_rate`         decimal(15,6)       COMMENT    '定价毛利率',
`profit_rate_chain_ratio`            decimal(15,6)       COMMENT    '定价毛利率环比',
`current_period_fact_price`          decimal(15,6)       COMMENT    '原料价',
`fact_price_chain_ratio`             decimal(15,6)       COMMENT    '原料价环比',
`current_period_avg_cost`            decimal(15,6)       COMMENT    '成品价',
`avg_cost_chain_ratio`               decimal(15,6)       COMMENT    '成品价环比',
`current_period_avg_price`           decimal(15,6)       COMMENT    '售价',
`avg_price_chain_ratio`              decimal(15,6)       COMMENT    '售价环比',
`avg_price_down_top_customers`       string              COMMENT    '售价下滑主要客户',
`rank_number`                        int                 COMMENT    '排名',
`update_time`                        string              COMMENT    '数据更新时间'
) COMMENT '品类毛利异常商品'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	

