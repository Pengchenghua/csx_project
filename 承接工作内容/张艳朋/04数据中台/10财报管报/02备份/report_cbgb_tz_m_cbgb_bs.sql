-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');


drop table csx_tmp.tmp_cbgb_tz_bs;
create table csx_tmp.tmp_cbgb_tz_bs
as 
select 
	a.province_code,a.province_name,a.city_code,a.city_name,a.location_code,a.location_name,a.posting_time,a.wms_order_no,a.wms_biz_type,a.wms_biz_type_name,a.credential_no,
	a.purchase_group_code,a.purchase_group_name,b.dept_id,b.dept_name,a.product_code,a.product_name,a.unit,a.qty,a.price_no_tax,a.amt_no_tax,a.amt,
	'' fac_adjust_amt_no_tax,
	'' negative_adjust_amt_no_tax,
	'' remedy_adjust_amt_no_tax,
	'' manual_adjust_amt_no_tax,
	'' cost_amt_no_tax,
	a.company_code,a.company_name,a.cost_center_code,a.cost_center_name,b.small_category_code,b.small_category_name,a.reservoir_area_code,a.reservoir_area_name,a.reservoir_area_prop        
from
	(
	select 
		a.*,
		case when a.location_code='W0H4' then '-' else b.province_code end province_code,
		case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
		case when a.location_code='W0H4' then '-' else b.city_code end city_code,
		case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,
		b.shop_id,b.shop_name
	from 
		(
		select
			a.*,b.reservoir_area_name,b.reservoir_area_attribute,b.reservoir_area_attribute reservoir_area_prop
		from
			(
			select
				location_code,location_name,company_code,company_name,goods_code product_code,goods_name product_name,unit,price_no_tax,
				credential_no,posting_time,purchase_group_code,purchase_group_name,move_type,reservoir_area_code,wms_biz_type_code,
				wms_order_no,wms_biz_type_code wms_biz_type,wms_biz_type_name,cost_center_code,cost_center_name,
				if(move_type in ('117B','118B'),-1*qty,qty) qty,
				if(move_type in ('117B','118B'),-1*amt_no_tax,amt_no_tax) amt_no_tax,
				if(move_type in ('117B','118B'),-1*amt,amt) amt
			from 
				csx_dw.dws_cas_r_d_account_credential_detail
			where 
				sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_21}
				and wms_biz_type_code in (35, 36, 37, 38, 39, 40, 41, 64, 66, 76, 77, 78)
			) a
			left join
				(
				select * 
				from csx_ods.source_wms_w_a_wms_reservoir_area
				)b on a.location_code=b.warehouse_code and a.reservoir_area_code=b.reservoir_area_code
		where 
			(reservoir_area_attribute='C' or reservoir_area_attribute='Y')
			and (( a.wms_biz_type_code <>'64' and b.reservoir_area_attribute = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
			or a.wms_biz_type_code = '64' )
		) a 
		left join 
			(
			select 
				shop_id,shop_name,sales_province_code province_code,sales_province_name province_name,city_group_code as city_code,city_group_name as city_name
			from 
				csx_dw.dws_basic_w_a_csx_shop_m 
			where sdt = 'current'
			) b on b.shop_id=a.location_code
	) a
	left join 
		(
		select 
			goods_id,goods_name,department_id dept_id,department_name dept_name,
			category_small_code small_category_code,category_small_name small_category_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m 
		where 
			sdt = 'current' 
		)b on a.product_code=b.goods_id;




--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_bs partition(sdt) 
select 
	province_code,province_name,city_code,city_name,
	location_code,location_name,
	posting_time,
	wms_order_no,
	wms_biz_type,
	wms_biz_type_name,
	credential_no,
	purchase_group_code,
	purchase_group_name,	
	dept_id,dept_name,	
	product_code,
	product_name,
	unit,
	qty,
	price_no_tax,
	amt_no_tax,
	amt,
	fac_adjust_amt_no_tax,
	negative_adjust_amt_no_tax,
	remedy_adjust_amt_no_tax,
	manual_adjust_amt_no_tax,
	cost_amt_no_tax,
	company_code,
	company_name,
	cost_center_code,
	cost_center_name,
	small_category_code,
	small_category_name,
	reservoir_area_code,
	reservoir_area_name,
	reservoir_area_prop,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_bs;
