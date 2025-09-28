--  动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

--  中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
--  启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;

insert overwrite table csx_analyse.csx_analyse_fr_bbc_newyobo_di partition (sdt)

select
	  concat_ws('&',a.order_no,a.goods_code) as biz_id,
	  coalesce(e.region_code,a.region_code) as region_code,
	  coalesce(e.region_name,a.region_name) as region_name,
	  coalesce(area_province_code,a.province_code) as province_code,
	  coalesce(c.province_name,a.province_name) as province_name,
	  if(coalesce(c.city_name,a.city_name)='仙游县','350322',coalesce(e.city_code,a.city_code)) as city_group_code,
	  coalesce(c.city_name,a.city_name) as city_group_name,
	  coalesce(c.dc_name,'') as dc_name,
	  coalesce(c.store_id,'') as store_id,
	  coalesce(c.business_district,'') as business_district,
	  receiver_area_info,
	  user_telephone,
	  sdt out_date,
	  a.order_no,
	  bbc_first_category_code,
	  bbc_first_category_name,
	  bbc_second_category_code,
	  bbc_second_category_name,
	  a.goods_code,
	  a.goods_name,
	  division_name,
	  spec,
	  unit,
	  a.sales_price,
	  order_qty,
	  sales_qty,
	  excluding_tax_sales,
	  sales_value,
	  sales_qty*coalesce(f.rebate_price, 0) as fanli_value,
	  coalesce(profit,0) as profit,
	  sign_company_code,
	  sign_company_name,
	  real_sales_value,
	  is_use_voucher,
	  sales_cost,
	  excluding_tax_cost,
	  excluding_tax_profit,
	  profit_rate,
	  (profit-sales_qty*coalesce(f.rebate_price, 0))/sales_value as excluding_fanli_profit_rate,
	  kc_code,
	  kc_name,
	  customer_no,
	  customer_name,
	  sdt
from
	(
	select
		city_code,city_name,province_code,province_name,region_code,region_name,bbc_first_category_code,bbc_first_category_name,bbc_second_category_code,
		bbc_second_category_name,sdt,order_no,customer_no,customer_name,goods_code,goods_name,division_name,spec,unit,sales_price,order_qty,sales_qty,
		excluding_tax_sales,sales_value,sign_company_code,sign_company_name,sales_value-consumer_voucher_amount as real_sales_value,
		if(consumer_voucher_amount=0,'否','是') as is_use_voucher,sales_cost,excluding_tax_cost,excluding_tax_profit,profit,coalesce(profit,0)/sales_value as profit_rate,
		user_id,dc_code as kc_code,dc_name as kc_name
	from 
		csx_dw.dws_bbc_r_d_sale_detail
	where 
		sdt >= regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -1), '-', '') and customer_name like '%牛约堡%'
	) a 
	left join
		(
		select
			order_no,user_telephone,receiver_telephone,receiver_area_info
		from 
			csx_dw.dwd_bbc_r_d_wshop_order
		where 
			sdt>='20211001'
		) b on if(substr(a.order_no,1,1)='R',substr(a.order_no,2,16),a.order_no) = b.order_no
	left join csx_dw.dws_bbc_w_a_store_phone c on b.receiver_area_info = c.store_address and b.user_telephone = c.phone_number
	left join
		( -- 获取省区与城市组信息
		select
			city_code,city_name,area_province_name,area_province_code,city_group_code,city_group_name,province_code, province_name,region_code, region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		) e on c.province_name = e.area_province_name and c.city_name = e.city_name
	left join csx_dw.dws_bbc_w_a_rebate_order_goods f on a.order_no = f.order_no and a.goods_code = f.goods_code;




/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di  慧共享保证金明细

drop table if exists csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di;
create table csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di(
`biz_id`                               string         COMMENT    '业务主键',


) COMMENT '慧共享保证金明细'
PARTITIONED BY (sdt string COMMENT '单据审批通过日期分区')
STORED AS TEXTFILE;

*/