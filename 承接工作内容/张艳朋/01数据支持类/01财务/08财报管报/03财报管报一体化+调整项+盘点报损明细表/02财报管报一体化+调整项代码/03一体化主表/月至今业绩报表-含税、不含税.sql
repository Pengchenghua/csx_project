

----------------------------------------------------------------------开始--------------------------------------------------------------------------------------------

-- 昨日、上月1日，取近两月数据和战报一致
set i_sdate_1 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_2 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');

--select ${hiveconf:i_sdate_1},${hiveconf:i_sdate_2};


--客户最小最大成交日期
drop table csx_tmp.tmp_sale_cust_min_max;
create temporary table csx_tmp.tmp_sale_cust_min_max
as 
select
customer_no,min(sales_date) as min_sales_date,max(sales_date) as max_sales_date,count(distinct sales_date) as count_day
from 
(select customer_no,sales_date,sales_value from csx_dw.sale_item_m where sdt>='20180101' and sdt<'20190101' and sales_type in('qyg','sapqyg','sapgc','sc','bbc','gc','anhui') 
union all 
select customer_no,sales_date,sales_value from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20190101' and sdt<=${hiveconf:i_sdate_1} and sales_type in('qyg','sapqyg','sapgc','sc','bbc') 
and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
) a
group by customer_no;


drop table csx_tmp.tmp_cust_sale_day;
create temporary table csx_tmp.tmp_cust_sale_day
as 
select 
	c.region_code,c.region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.channel,a.channel_name,a.customer_no,b.customer_name,
	case when a.channel in ('1','7','9') and b.attribute is null then '日配客户' else b.attribute end attribute,
	if(substr(b.sign_time,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sign,
	if(substr(e.min_sales_date,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sale,
	sales_value,sales_value-profit sales_cost,profit,
	no_tax_sales,no_tax_sales-no_tax_profit no_tax_cost,no_tax_profit,
	a.sdt
from
	(select	province_code,province_name,city_group_code,city_group_name,channel,channel_name,customer_no,sdt,
		sum(sales_value)sales_value,
		sum(sales_cost)sales_cost,
		sum(profit)profit,
		sum(excluding_tax_sales)no_tax_sales,
		sum(excluding_tax_cost)no_tax_cost,
		sum(excluding_tax_profit)no_tax_profit		
	from csx_dw.dws_sale_r_d_customer_sale
	where sdt>=${hiveconf:i_sdate_2} 
	and sdt<=${hiveconf:i_sdate_1}
	and sales_type in ('sapqyg','sapgc','qyg','sc','bbc') 
	and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)

	group by province_code,province_name,city_group_code,city_group_name,channel,channel_name,customer_no,sdt )a
left join 
	(select customer_no,customer_name,regexp_replace(split(sign_time, ' ')[0], '-', '') sign_time,
		 case when attribute is null then '日配客户'
			  when attribute not in('日配客户','福利客户','贸易客户') then '商贸批发和其他' 
			  else attribute end attribute
	from csx_dw.dws_crm_w_a_customer_m_v1
	where sdt = ${hiveconf:i_sdate_1}
	and customer_no<>''
	and channel_code in('1','7','9')
	)b on b.customer_no=a.customer_no
left join (select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank='13')c on c.province_code=a.province_code
left join csx_tmp.tmp_sale_cust_min_max e on e.customer_no=a.customer_no;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.ads_wms_r_d_fixation_report_cust_sale_day_v1  partition(sdt)
select 
region_code,region_name,province_code,province_name,city_group_code,city_group_name,channel,channel_name,customer_no,customer_name,
attribute,is_new_sign,is_new_sale,
sales_value,sales_cost,profit,
no_tax_sales,no_tax_cost,no_tax_profit,
sdt
from csx_tmp.tmp_cust_sale_day;

--INVALIDATE METADATA csx_dw.ads_wms_r_d_fixation_report_cust_sale_day_v1;


/*
----------------------------hive 建表语句-------------------------------------

drop table if exists csx_dw.ads_wms_r_d_fixation_report_cust_sale_day_v1;
create table csx_dw.ads_wms_r_d_fixation_report_cust_sale_day_v1(
  `region_code` string COMMENT '大区编码',
  `region_name` string COMMENT '大区',  
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区',
  `city_group_code` string COMMENT '城市组编码',
  `city_group_name` string COMMENT '城市组',  
  `channel` string COMMENT '渠道编码',
  `channel_name` string COMMENT '渠道',
  `customer_no` string COMMENT '客户编码',
  `customer_name` string COMMENT '客户名称',
  `attribute` string COMMENT '客户属性',
  `is_new_sign` string COMMENT '是否新签约客户',
  `is_new_sale` string COMMENT '是否新成交客户',  
  `sales_value` decimal(26,6)  COMMENT '含税销售额',
  `sales_cost` decimal(26,6)  COMMENT '定价成本',
  `profit` decimal(26,6)  COMMENT '含税毛利额',
  `no_tax_sales` decimal(26,6)  COMMENT '不含税销售额',
  `no_tax_cost` decimal(26,6)  COMMENT '不含税定价成本',
  `no_tax_profit` decimal(26,6)  COMMENT '不含税毛利额'  
) COMMENT '客户每日销售额 '
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;


----------------------------mysql 建表语句-------------------------------------

drop table if exists fixation_report_cust_sale_day_v1;
create table fixation_report_cust_sale_day_v1(
  `region_code` varchar(64) DEFAULT NULL COMMENT '大区编码',
  `region_name` varchar(64) DEFAULT NULL COMMENT '大区',  
  `province_code` varchar(64) DEFAULT NULL COMMENT '省区编码',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省区',
  `city_group_code` varchar(64) DEFAULT NULL COMMENT '城市组编码',
  `city_group_name` varchar(64) DEFAULT NULL COMMENT '城市组',  
  `channel` varchar(64) DEFAULT NULL COMMENT '渠道编码',
  `channel_name` varchar(64) DEFAULT NULL COMMENT '渠道',
  `customer_no` varchar(64) DEFAULT NULL COMMENT '客户编码',
  `customer_name` varchar(64) DEFAULT NULL COMMENT '客户名称',
  `attribute` varchar(64) DEFAULT NULL COMMENT '客户属性',
  `is_new_sign` varchar(64) DEFAULT NULL COMMENT '是否新签约客户',
  `is_new_sale` varchar(64) DEFAULT NULL COMMENT '是否新成交客户',  
  `sales_value` decimal(26,6) DEFAULT NULL COMMENT '含税销售额',
  `sales_cost` decimal(26,6) DEFAULT NULL COMMENT '定价成本',
  `profit` decimal(26,6) DEFAULT NULL COMMENT '含税毛利额',
  `no_tax_sales` decimal(26,6) DEFAULT NULL COMMENT '不含税销售额',
  `no_tax_cost` decimal(26,6) DEFAULT NULL COMMENT '不含税定价成本',
  `no_tax_profit` decimal(26,6) DEFAULT NULL COMMENT '不含税毛利额',  
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户每日销售额 ';

*/






