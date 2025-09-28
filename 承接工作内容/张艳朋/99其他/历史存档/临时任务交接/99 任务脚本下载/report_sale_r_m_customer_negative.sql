set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;
-- 增加reduce过程
set hive.optimize.sort.dynamic.partition=true;


--数据范围：当月月至今销售明细订单数据（截至昨日），大客户
--退货冲销，如某客户1月1日销售额10000元，1月3日退货3000元（1月1日销售中产生的退货），则该客户1月1日的销售额与毛利额均需扣除退货部分，处理方式将退货单返回原始正向单的出库日期

-- 昨日月1日，昨日、上月1日，上月昨日;  本周六，昨日、上周六，上周昨日
--select ${hiveconf:i_sdate_m11},${hiveconf:i_sdate_m12},${hiveconf:i_sdate_m21},${hiveconf:i_sdate_m22},
--		 ${hiveconf:i_sdate_w11},${hiveconf:i_sdate_w12},${hiveconf:i_sdate_w21},${hiveconf:i_sdate_w22},${hiveconf:i_sdate_dd};

set i_sdate_m11 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
set i_sdate_m12 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_m5 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-5),'-','');

set i_sdate_m21 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');		
set i_sdate_m22 =concat(substr(regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-',''),1,6),
					if(date_sub(current_date,1)=last_day(date_sub(current_date,1))
					,substr(regexp_replace(last_day(add_months(trunc(date_sub(current_date,1),'MM'),-1)),'-',''),7,2)
					,substr(regexp_replace(date_sub(current_date,1),'-',''),7,2)));					

set i_sdate_dd =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间

set target_table = csx_dw.report_sale_r_m_customer_negative;


drop table csx_tmp.test_cust_profit_f_day_01;
create temporary table csx_tmp.test_cust_profit_f_day_01
as
select 
	a.id,
	a.business_type_code,
	a.business_type_name as sale_group,	
	a.channel_code,     --战报渠道编码
	a.channel_name,     --战报渠道名称
	a.region_code,
	a.region_name,	
	a.province_code,     --战报省区编码
	a.province_name,     --战报省区名称
	a.city_group_code,     --战报城市组编码
	a.city_group_name,     --战报城市组名称
	a.dc_code,     --库存地点编码
	a.dc_name,     --库存地点名称
	a.customer_no,     --客户编号
	e.customer_name,     --客户名称
	e.first_category,     --客户一级分类
	e.second_category,     --客户二级分类
	e.attribute_code,
	e.attribute,     --客户属性
	a.origin_order_no,     --原订单号
	a.order_no,     --订单号
	a.credential_no,     --成本核算凭证号
	a.goods_code,     --商品编号
	f.goods_name,     --商品名称
	f.department_id,     --课组编号
	f.department_name,     --课组名称
	f.category_large_code,     --大类编号 
	f.category_large_name,     --大类名称	
	f.classify_middle_code,     --管理中类编号
	f.classify_middle_name,     --管理中类名称	
	a.sales_value,     --含税销售额
	--a.sales_cost,     --含税销售成本
	a.profit,     --含税毛利
	a.front_profit,--`前端毛利`
	a.sdt sales_date,
	a.sales_qty     --销售数量		
from 
	(
	select 
		id,
		business_type_code,
		business_type_name,
		region_code,
		region_name,
		dc_code,     --库存地点编码
		dc_name,     --库存地点名称
		customer_no,     --客户编号
		origin_order_no,
		order_no,     --订单号
		split(id,'&')[0] as credential_no,     --成本核算凭证号
		goods_code,     --商品编号
		sales_qty,     --销售数量
		sales_value,     --含税销售额
		sales_cost,     --含税销售成本
		profit,     --含税毛利
		front_profit,--`前端毛利`
		channel_code,     --战报渠道编码
		channel_name,     --战报渠道名称
		province_code,     --战报省区编码
		province_name,     --战报省区名称
		city_group_code,     --战报城市组编码
		city_group_name,     --战报城市组名称
		sdt	
	from csx_dw.dws_sale_r_d_detail
	where sdt>=${hiveconf:i_sdate_m11}
	and sdt<=${hiveconf:i_sdate_m12}
	and channel_code in('1','7','9')
	)a	
join 
	(select distinct customer_no,customer_name,first_category_name as first_category,second_category_name as second_category,attribute as attribute_code,attribute_desc as attribute
	from csx_dw.dws_crm_w_a_customer 
	--where sdt=${hiveconf:i_sdate_m12} 
	where sdt='current'	
	--and (attribute_code <>'5' or attribute_code is null)
	) e on e.customer_no=a.customer_no		
left join 
	(select goods_id,regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,department_id,department_name,
		category_large_code,category_large_name,classify_middle_code,classify_middle_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )f on a.goods_code=f.goods_id	
;


--负毛利每日数据
--负毛利每日数据_明细
insert overwrite table csx_dw.report_sale_r_m_customer_negative  partition(smonth)
select
	concat_ws('-',substr(${hiveconf:i_sdate_m12},1,6),a.province_code,a.city_group_name,a.sale_group,a.customer_no,a.sales_date) as id,
	a.sales_date,a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_name as city_name,sale_group,
	a.first_category,a.second_category,a.customer_no,a.customer_name,a.attribute_code,a.attribute,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(if(a.profit<0,a.profit,0)) profit_f,
	'饶艳华' create_by,
	from_utc_timestamp(current_timestamp(),'GMT') create_time,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(${hiveconf:i_sdate_m12},1,6) smonth
from csx_tmp.test_cust_profit_f_day_01 a
join 
  (
	select customer_no,sales_date,
		sum(sales_value) D_cust_sales_value,
		sum(profit) D_cust_profit
    from csx_tmp.test_cust_profit_f_day_01 
	group by customer_no,sales_date
	having sum(sales_value)>0 and sum(profit)<0
  )c on c.customer_no=a.customer_no and c.sales_date=a.sales_date
group by 
	concat_ws('-',substr(${hiveconf:i_sdate_m12},1,6),a.province_code,a.city_group_name,a.sale_group,a.customer_no,a.sales_date),
	a.sales_date,a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_name,a.sale_group,
	a.first_category,a.second_category,a.customer_no,a.customer_name,a.attribute_code,a.attribute
;
