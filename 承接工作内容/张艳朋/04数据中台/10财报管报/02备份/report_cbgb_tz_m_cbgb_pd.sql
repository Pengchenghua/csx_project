-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

drop table csx_tmp.tmp_cbgb_tz_pd;
create temporary table csx_tmp.tmp_cbgb_tz_pd 
as 
select a.province_code,a.province_name,a.city_code,a.city_name,
	--c.channel_name,a.cost_center_code,a.cost_center_name,
	a.location_code,a.location_name,a.company_code,a.company_name,a.product_code,
	regexp_replace(regexp_replace(a.product_name,'\n',''),'\r','') product_name,
	b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(case when amt_no_tax>=0 then -amt_no_tax end )  inventory_p_no, --盘盈  
	sum(case when amt_no_tax<0 then -amt_no_tax end )  inventory_l_no, --盘亏

	sum(case when amt>=0 then -amt end )  inventory_p, --盘盈  
	sum(case when amt<0 then -amt end ) inventory_l --盘亏
from
	(select a.*,
		case when a.location_code='W0H4' then '-' else b.city_code end province_code,
		case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
		case when a.location_code='W0H4' then '-' else b.city_code end city_code,
		case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,
		b.shop_id,b.shop_name
	from 
	  (
        select location_code,location_name,company_code,company_name,goods_code product_code,goods_name product_name,
         credential_no,posting_time,purchase_group_code,move_type,reservoir_area_code,
         if(move_type in ('115B','116A'),-1*qty,qty) qty,
         if(move_type in ('115B','116A'),-1*amt_no_tax,amt_no_tax) amt_no_tax,   --不含税金额
         if(move_type in ('115B','116A'),-1*amt,amt) amt   --含税金额
        from csx_dw.dws_cas_r_d_account_credential_detail
        where sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_21}
        and wms_biz_type_code = 34
        and reservoir_area_code = 'PD01' 
        and (purchase_group_code like 'H%' or purchase_group_code like 'U%')		
      )a 
	  left join 
		(select shop_id,shop_name,sales_province_code province_code,sales_province_name province_name,city_group_code as city_code,city_group_name as city_name
		from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
	)a
left join 
	(select goods_id,goods_name,department_id dept_id,department_name dept_name
		from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
	(select distinct
		workshop_code,province_code,goods_code
	  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
	  where sdt='current' and new_or_old=1
	)d on a.province_code=d.province_code and a.product_code=d.goods_code
--left join csx_tmp.tmp_sale_order_flag c on a.wms_order_no=c.order_no and a.product_code=c.goods_code
group by a.province_code,a.province_name,a.city_code,a.city_name,
	--c.channel_name,a.cost_center_code,a.cost_center_name,
	a.location_code,a.location_name,a.company_code,a.company_name,a.product_code,
	regexp_replace(regexp_replace(a.product_name,'\n',''),'\r',''),
	b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品');


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_pd partition(sdt)
select province_code,province_name,city_code,city_name,
	location_code,location_name,company_code,company_name,product_code,product_name,
	dept_id,dept_name,is_factory_goods_name,
	inventory_p_no,inventory_l_no,
	inventory_p,inventory_l,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_pd;

