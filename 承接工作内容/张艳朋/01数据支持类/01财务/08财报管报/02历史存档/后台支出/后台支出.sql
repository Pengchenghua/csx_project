-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');
set i_sdate_24 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-3),'-','');


--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

drop table csx_tmp.tmp_cbgb_tz_htzh;
create temporary table csx_tmp.tmp_cbgb_tz_htzh 
as 
select 
	a.adjust_reason,a.dc_code,a.dc_name,
	case when a.dc_code='W0H4' then '-' else a.province_code end province_code,
	case when a.dc_code='W0H4' then '供应链' else a.province_name end province_name,
	case when a.dc_code='W0H4' then '-' else a.city_code end city_code,
	case when a.dc_code='W0H4' then '供应链' else a.city_name end city_name,
	c.channel_name,a.goods_code as product_code,a.goods_name as product_name,e.dept_id,e.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(sales_value/(1+tax_rate/100)) amt_no_tax,
	sum(sales_value) amt
--from
--	(select a.*
--	from csx_dw.dwd_csms_r_d_rebate_order a
--	where a.order_type_code in ('0','1')
--	and a.commit_time>=${hiveconf:i_sdate_12}
--	and a.commit_time<${hiveconf:i_sdate_11}
--	and a.order_status='1')a	

from
	(
	select 
		a.adjust_reason,a.dc_code,a.dc_name,a.customer_no,a.customer_name,
		a.goods_code,a.goods_name,a.sales_value,a.tax_rate,
		b.province_code,b.province_name,b.city_code,b.city_name
	from
		(
		select
			a.adjust_reason,a.dc_code,a.dc_name,a.customer_no,a.customer_name,
			a.goods_code,a.goods_name,a.sales_value,a.tax_rate
		from
			(
			select
				'Z68' as adjust_reason,rebate_no as no_type,dc_code,dc_name,customer_no,customer_name,goods_code,goods_name,sales_value,tax_rate
			from
				csx_dw.dwd_sss_r_d_customer_rebate_detail --客户返利单明细表
			where
				sdt>='20220317' and sdt<=regexp_replace(date_sub(current_date, 1), '-', '') 
			union all
			select
				'Z69' as adjust_reason,adjust_price_no as no_type,dc_code,dc_name,customer_no,customer_name,goods_code,goods_name,sales_value,tax_rate
			from
				csx_dw.dwd_sss_r_d_customer_adjust_price_detail --客户调价单明细表
			where
				sdt>='20220317' and sdt<=regexp_replace(date_sub(current_date, 1), '-', '') 
			union all
			select
				adjust_reason_code as adjust_reason,rebate_no as no_type,dc_code,dc_name,customer_no,customer_name,goods_code,goods_name,sales_value,tax_rate
			from
				csx_dw.dwd_csms_r_d_rebate_order_detail	
			where
				sdt>='20220301' and sdt<'20220317'					
			) a 
			join(select distinct order_no from csx_dw.dws_sale_r_d_detail where sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_21}) b on b.order_no=a.no_type
		) a 
		--csx_dw.dwd_csms_r_d_rebate_order a
		left join --省区
			(
			select 
				shop_id,shop_name,sales_province_code province_code,sales_province_name province_name,city_group_code as city_code,city_group_name as city_name
			from 
				csx_dw.dws_basic_w_a_csx_shop_m 
			where sdt = 'current'
			) b on b.shop_id=a.dc_code
	)a	
	left join --渠道
	(
	  select * from csx_dw.dws_crm_w_a_customer
	  where sdt = regexp_replace(date_sub(current_date, 1), '-', '') 
	) c on a.customer_no = c.customer_no
	left join --是否工厂商品
		(select
			workshop_code, province_code, goods_code
		  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
		  where sdt='current' and new_or_old=1
		)d on a.province_code=d.province_code and a.goods_code=d.goods_code
	left join --课组
		(select goods_id,goods_name,department_id dept_id,department_name dept_name
			from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )e on a.goods_code=e.goods_id
group by 
	a.adjust_reason,a.dc_code,a.dc_name,
	case when a.dc_code='W0H4' then '-' else a.province_code end,
	case when a.dc_code='W0H4' then '供应链' else a.province_name end,
	case when a.dc_code='W0H4' then '-' else a.city_code end,
	case when a.dc_code='W0H4' then '供应链' else a.city_name end,
	c.channel_name,a.goods_code,a.goods_name,e.dept_id,e.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') ;


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_htzh partition(sdt)
select adjust_reason,dc_code as inventory_dc_code,dc_name as inventory_dc_name,
	province_code,province_name,city_code,city_name,
	channel_name,product_code,product_name,dept_id,dept_name,is_factory_goods_name,
	amt_no_tax,amt,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_htzh;
