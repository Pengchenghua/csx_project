-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 启用引号识别
set hive.support.quoted.identifiers=none;
	
with current_tmp_category_profit_anomaly as 	
(
select
	a.performance_region_code,a.performance_region_name,a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,
	coalesce(a.current_period_sale_amt,0) as current_period_sale_amt, --销售额
	coalesce(a.current_period_sale_amt/sum(a.current_period_sale_amt)over(partition by a.performance_city_code),0) as sale_proportion, --销售额占比
	coalesce(a.current_period_sale_amt/a.last_period_sale_amt-1,0) as sale_amt_chain_ratio, --销售额环比
	coalesce(a.current_period_profit_rate,0) as current_period_profit_rate,-- 毛利率
	coalesce(a.current_period_profit_rate,0)-coalesce(a.last_period_profit_rate,0) as profit_rate_chain_ratio,--毛利率环比
	coalesce(a.current_period_avg_cost/a.last_period_avg_cost-1,0) as avg_cost_chain_ratio, -- 平均成本环比
	coalesce(a.current_period_avg_price/a.last_period_avg_price-1,0) as avg_price_chain_ratio, -- 平均售价环比
	row_number()over(partition by a.performance_city_code order by current_period_sale_amt desc) as rank_number
from
	(
	select
		b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,b.performance_city_code,b.performance_city_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,
		--前一天
		sum(if(a.sdt='${ytd}',a.sale_amt,null)) current_period_sale_amt,
		sum(if(a.sdt='${ytd}',a.profit,null))/abs(sum(if(a.sdt='${ytd}',a.sale_amt,null))) as current_period_profit_rate,
		sum(if(a.sdt='${ytd}',a.sale_cost,null))/sum(if(a.sdt='${ytd}',a.sale_qty,null)) as current_period_avg_cost,
		sum(if(a.sdt='${ytd}',a.sale_amt,null))/sum(if(a.sdt='${ytd}',a.sale_qty,null)) as current_period_avg_price,
		--前两天
		sum(if(a.sdt='${before_ytd}',a.sale_amt,null)) last_period_sale_amt,
		sum(if(a.sdt='${before_ytd}',a.profit,null))/abs(sum(if(a.sdt='${before_ytd}',a.sale_amt,null))) as last_period_profit_rate,
		sum(if(a.sdt='${before_ytd}',a.sale_cost,null))/sum(if(a.sdt='${before_ytd}',a.sale_qty,null)) as last_period_avg_cost,
		sum(if(a.sdt='${before_ytd}',a.sale_amt,null))/sum(if(a.sdt='${before_ytd}',a.sale_qty,null)) as last_period_avg_price
	from 
		(
		select 
			id,sdt,goods_code,customer_code,sale_amt,sale_qty,profit,sale_cost,inventory_dc_code
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between '${before_ytd}' and '${ytd}'
			and channel_code in ('1','7','9')
			and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and inventory_dc_code not in ('W0K4','W0Z7')
		)a
		left join
			(
			select 
				customer_code,customer_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from 
				csx_dim.csx_dim_crm_customer_info
			where 
				sdt = '${ytd}'
			)b on b.customer_code=a.customer_code	
		left join   --商品表
			(
			select 
				goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
			from 
				csx_dim.csx_dim_basic_goods
			where 
				sdt ='${ytd}'
			) c on a.goods_code=c.goods_code		
	group by 
		b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,b.performance_city_code,b.performance_city_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name
	) a 
where
	current_period_profit_rate<0.05
	or coalesce(a.current_period_profit_rate,0)-coalesce(a.last_period_profit_rate,0)<0
),

current_tmp_category_profit_anomaly_goods_middle as
(
select
	a.performance_region_code,a.performance_region_name,a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,a.classify_small_code,classify_small_name,a.goods_code,a.goods_name,
	coalesce(current_period_sale_amt,'') as current_period_sale_amt,
	coalesce(current_period_sale_qty,'') as current_period_sale_qty,
	coalesce(current_period_profit_rate) as current_period_profit_rate,
	coalesce(current_period_profit_rate,0)-coalesce(last_period_profit_rate,0) as profit_rate_chain_ratio,
	coalesce(current_period_fact_price,'') as current_period_fact_price,
	coalesce(current_period_fact_price/last_period_fact_price-1,'') as fact_price_chain_ratio,
	coalesce(current_period_avg_cost,'') as current_period_avg_cost,
	coalesce(current_period_avg_cost/last_period_avg_cost-1,'') as avg_cost_chain_ratio,
	coalesce(current_period_avg_price,'') as current_period_avg_price,
	coalesce(current_period_avg_price/last_period_avg_price-1,'') as avg_price_chain_ratio,
	row_number()over(partition by a.performance_city_code order by current_period_sale_amt desc) as rank_number
from
	(
	select
		b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,b.performance_city_code,b.performance_city_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,classify_small_name,a.goods_code,c.goods_name,
		--前一天
		sum(if(a.sdt='${ytd}',a.sale_amt,null)) current_period_sale_amt,
		sum(if(a.sdt='${ytd}',a.sale_qty,null)) current_period_sale_qty,
		sum(if(a.sdt='${ytd}',a.profit,null))/abs(sum(if(a.sdt='${ytd}',a.sale_amt,null))) as current_period_profit_rate,
		sum(if(a.sdt='${ytd}',a.sale_cost,null)) as current_period_sale_cost,
		sum(if(a.sdt='${ytd}',a.sale_cost,null))/sum(if(a.sdt='${ytd}',a.sale_qty,null)) as current_period_avg_cost,
		sum(if(a.sdt='${ytd}',a.sale_amt,null))/sum(if(a.sdt='${ytd}',a.sale_qty,null)) as current_period_avg_price,
		sum(if(a.sdt='${ytd}',d.fact_price*a.sale_qty,null))/sum(if(a.sdt='${ytd}',a.sale_qty,null)) as current_period_fact_price,
		--前两天
		sum(if(a.sdt='${before_ytd}',a.sale_amt,null)) last_period_sale_amt,
		sum(if(a.sdt='${before_ytd}',a.profit,null))/abs(sum(if(a.sdt='${before_ytd}',a.sale_amt,null))) as last_period_profit_rate,
		sum(if(a.sdt='${before_ytd}',a.sale_cost,null))/sum(if(a.sdt='${before_ytd}',a.sale_qty,null)) as last_period_avg_cost,
		sum(if(a.sdt='${before_ytd}',a.sale_amt,null))/sum(if(a.sdt='${before_ytd}',a.sale_qty,null)) as last_period_avg_price,
		sum(if(a.sdt='${before_ytd}',d.fact_price*a.sale_qty,null))/sum(if(a.sdt='${before_ytd}',a.sale_qty,null)) as last_period_fact_price,
		row_number()over(partition by b.performance_city_code,c.classify_middle_code order by sum(if(a.sdt='${ytd}',a.sale_amt,null)) desc) as rank_number
	from 
		(
		select 
			id,sdt,goods_code,customer_code,sale_amt,sale_qty,profit,sale_cost,inventory_dc_code
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between '${before_ytd}' and '${ytd}'
			and channel_code in ('1','7','9')
			and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and inventory_dc_code not in ('W0K4','W0Z7')
		)a
		left join
			(
			select 
				customer_code,customer_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from 
				csx_dim.csx_dim_crm_customer_info
			where 
				sdt = '${ytd}'
			)b on b.customer_code=a.customer_code	
		left join   --商品表
			(
			select 
				goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
			from 
				csx_dim.csx_dim_basic_goods
			where 
				sdt ='${ytd}'
			) c on a.goods_code=c.goods_code	
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
					-- csx_dw.dws_wms_r_d_batch_detail
					csx_dws.csx_dws_wms_batch_detail_di
				where 
					sdt >= '20210801' 
					and sdt<='${ytd}'
					and move_type_code in ('107A', '108A')
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
						-- csx_dw.dws_mms_r_a_factory_order
						csx_dws.csx_dws_mms_factory_order_df
					where 
						sdt >= '20210801' 
						and sdt<='${ytd}'
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
				current_tmp_category_profit_anomaly
			group by 
				classify_middle_code,classify_middle_name
			) e on e.classify_middle_code=c.classify_middle_code
	group by 
		b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,b.performance_city_code,b.performance_city_name,
		c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,classify_small_name,a.goods_code,c.goods_name
	) a 
where
	rank_number<=50
),

current_tmp_category_profit_anomaly_goods_middle_2 as
(
select
	a.performance_region_code,a.performance_region_name,a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,a.goods_code,
	concat_ws(' ',a.customer_code,a.customer_name,concat(cast(a.current_period_sale_amt as string),'元'),concat(cast(a.current_period_avg_price as string),'元'),concat(a.avg_price_chain_ratio*100,'%')) as avg_price_down_top_customers
from
	(
	select
		a.performance_region_code,a.performance_region_name,a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,a.goods_code,
		a.customer_code,a.customer_name,
		round(a.current_period_sale_amt,0) as current_period_sale_amt, --销售额
		round(a.current_period_avg_price,1) as current_period_avg_price,
		coalesce(round(a.current_period_avg_price/a.last_period_avg_price-1,3),0) as avg_price_chain_ratio, -- 平均售价环比
		row_number()over(partition by a.performance_city_code,a.goods_code order by current_period_sale_amt desc) as rank_number
	from
		(
		select
			b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,b.performance_city_code,b.performance_city_name,a.goods_code,
			a.customer_code,b.customer_name,
			--前一天
			sum(if(a.sdt='${ytd}',a.sale_amt,null)) current_period_sale_amt,
			sum(if(a.sdt='${ytd}',a.sale_amt,null))/sum(if(a.sdt='${ytd}',a.sale_qty,null)) as current_period_avg_price,
			--前两天
			sum(if(a.sdt='${before_ytd}',a.sale_amt,null)) last_period_sale_amt,
			sum(if(a.sdt='${before_ytd}',a.sale_amt,null))/sum(if(a.sdt='${before_ytd}',a.sale_qty,null)) as last_period_avg_price
		from 
			(
			select 
				id,sdt,goods_code,customer_code,sale_amt,sale_qty,profit,sale_cost,inventory_dc_code
			from 
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt between '${before_ytd}' and '${ytd}'
				and channel_code in ('1','7','9')
				and business_type_code ='1' -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
				and inventory_dc_code not in ('W0K4','W0Z7')
			)a
			left join
				(
				select 
					customer_code,customer_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
				from 
					csx_dim.csx_dim_crm_customer_info
				where 
					sdt = '${ytd}'
				)b on b.customer_code=a.customer_code	
			left join   --商品表
				(
				select 
					goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
				from 
					csx_dim.csx_dim_basic_goods
				where 
					sdt ='${ytd}'
				) c on a.goods_code=c.goods_code		
		group by 
			b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,b.performance_city_code,b.performance_city_name,a.goods_code,
			a.customer_code,b.customer_name
		) a 
	where
		a.current_period_avg_price<a.last_period_avg_price
	) a 
where
	a.rank_number<=10
)

insert overwrite table csx_analyse.csx_analyse_fr_sale_category_anomaly_goods_di partition(sdt)			
			
select
	'' as biz_id,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.classify_small_code,
	a.classify_small_name,
	a.goods_code,
	a.goods_name,
	a.current_period_sale_amt,
	a.current_period_sale_qty,
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
	'${ytd}' as sdt
from
	current_tmp_category_profit_anomaly_goods_middle a 
	left join current_tmp_category_profit_anomaly_goods_middle_2 b on a.performance_city_code=b.performance_city_code and a.goods_code=b.goods_code	
;
	

/*

create table csx_analyse.csx_analyse_fr_sale_category_anomaly_goods_di(
`biz_id`                             string              COMMENT    '业务主键',
`performance_region_code`            string              COMMENT    '大区编码',
`performance_region_name`            string              COMMENT    '大区名称',
`performance_province_code`          string              COMMENT    '省区编码',
`performance_province_name`          string              COMMENT    '省区名称',
`performance_city_code`              string              COMMENT    '城市编码',
`performance_city_name`              string              COMMENT    '城市',
`classify_large_code`                string              COMMENT    '管理大类编码',
`classify_large_name`                string              COMMENT    '管理大类名称',
`classify_middle_code`               string              COMMENT    '管理中类编码',
`classify_middle_name`               string              COMMENT    '管理中类名称',
`classify_small_code`                string              COMMENT    '管理小类编码',
`classify_small_name`                string              COMMENT    '管理小类名称',
`goods_code`                         string              COMMENT    '商品编码',
`goods_name`                         string              COMMENT    '商品名称',
`current_period_sale_amt`            decimal(15,6)       COMMENT    '销售额',
`current_period_sale_qty`            decimal(15,6)       COMMENT    '销售额数量',
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
) COMMENT '品类毛利异常-商品'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	

