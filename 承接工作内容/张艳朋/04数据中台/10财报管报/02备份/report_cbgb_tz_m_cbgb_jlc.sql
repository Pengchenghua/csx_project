
-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

drop table csx_tmp.tmp_cbgb_tz_jlc;
create temporary table csx_tmp.tmp_cbgb_tz_jlc 
as 
select 
	a.province_code,a.province_name,a.city_code,a.city_name,
	a.location_code,a.cost_center_code,a.product_code,b.goods_name product_name,b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(amount)amount
from
	(select 
		a.*,
		case when a.location_code='W0H4' then '-' else b.province_code end province_code,
		case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
		case when a.location_code='W0H4' then '-' else b.city_code end city_code,
		case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,
		b.shop_id,b.shop_name
	from (select * from csx_ods.source_mms_r_a_factory_report_no_share_product
	--where sdt='20200606'
	where sdt=regexp_replace(date_sub(current_date,0),'-','') 
	and period in(substr(${hiveconf:i_sdate_12},1,7)))a  --'2020-05'
	left join 
		(select 
			shop_id,shop_name,sales_province_code province_code,sales_province_name province_name,city_group_code as city_code,city_group_name as city_name
		from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
	) a
left join 
	(select regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') goods_name,
			goods_id,department_id dept_id,department_name dept_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
	(select
		workshop_code, province_code, goods_code
	from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
	where sdt='current' and new_or_old=1
	)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by a.province_code,a.province_name,a.city_code,a.city_name,
a.location_code,a.cost_center_code,a.product_code,b.goods_name,b.dept_id,b.dept_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');



--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_jlc partition(sdt)
select 
	province_code,province_name,city_code,city_name,
	location_code,cost_center_code,
	product_code,product_name,dept_id,dept_name,is_factory_goods_name,
	amount,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_jlc;
