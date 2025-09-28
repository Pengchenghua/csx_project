
-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

--后台收入明细
drop table csx_tmp.cbgb_tz_m_cbgb_htsr;
create table csx_tmp.cbgb_tz_m_cbgb_htsr
as 
select 
	case when a.settle_place_code='W0H4' then '-' else b.province_code end province_code,
	case when a.settle_place_code='W0H4' then '供应链' else b.province_name end province_name,
	case when a.settle_place_code='W0H4' then '-' else b.city_code end city_code,
	case when a.settle_place_code='W0H4' then '供应链' else b.city_name end city_name,
	a.settle_no,a.agreement_no,a.settle_date,a.purchase_org_code,a.purchase_org_name,a.department_code dept_id,a.department_name dept_name,a.cost_code,a.cost_name,
	a.sdt attribution_date,a.supplier_code,a.supplier_name,a.settle_place_code,a.settle_place_name,a.company_code,a.company_name,
	a.net_value,a.tax_amount,a.value_tax_total,a.bill_total_amount,a.invoice_code,a.invoice_name,substr(${hiveconf:i_sdate_22},1,6) as sdt
from 
( select * from csx_dw.dwd_gss_r_d_settle_bill 
where sdt >= ${hiveconf:i_sdate_22} 
and sdt < ${hiveconf:i_sdate_21} 
)a
left join (select shop_id,shop_name,sales_province_code province_code,sales_province_name province_name,city_group_code as city_code,city_group_name as city_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code;

drop table csx_tmp.cbgb_tz_m_cbgb_htsr_2;
create temporary table csx_tmp.cbgb_tz_m_cbgb_htsr_2
as 
select 
	case when cost_name like '目标返利%' then '目标返利'
		when cost_name like '仓储服务费%' then '仓储服务费'  
		else cost_name end cost_name ,
	province_code,province_name,city_code,city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settle_place_code,settle_place_name,sdt,
	sum( net_value) net_value,sum( value_tax_total) value_tax_total
from csx_tmp.cbgb_tz_m_cbgb_htsr
group by case when cost_name like '目标返利%' then '目标返利'
			when cost_name like '仓储服务费%' then '仓储服务费'  
			else cost_name end,
	province_code,province_name,city_code,city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settle_place_code,settle_place_name,sdt;
	
	

--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_htsr partition(sdt) 
select 
	cost_name,province_code,province_name,city_code,city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settle_place_code,settle_place_name,net_value,value_tax_total,sdt
from csx_tmp.cbgb_tz_m_cbgb_htsr_2;