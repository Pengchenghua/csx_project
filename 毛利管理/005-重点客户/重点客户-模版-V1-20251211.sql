
-- 导入关联客户
-- csx_analyse_tmp_link_customer_info 手工关联客户
-- csx_analyse_tmp_link_customer_info 关联客户目标
-- drop table csx_analyse_tmp.csx_analyse_tmp_sale_detial_01; 
create table csx_analyse_tmp.csx_analyse_tmp_sale_detial_01 as 
with tmp_sale_detail as (
select
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	d.bloc_code,
	j.link_customer_code,
    j.link_customer_name,
	f.work_no,
	f.sales_name,
	f.second_supervisor_work_no,
	f.second_supervisor_name,
	f.rp_service_user_work_no_new,
	f.rp_service_user_name_new,	
	i.price_name,
	sum(case when a.s_month ='202512' then sale_amt/10000 end) sale_amt_12,
	sum(case when a.s_month ='202512' then profit/10000 end) profit_12,
	sum(case when a.s_month ='202512' and a.order_channel_code=6  then profit/10000 end) as tj_amt,
	sum(case when a.s_month ='202512' and  a.delivery_type_code=2 and a.order_channel_code not in (6, 4) and refund_order_flag='0' then sale_amt/10000 end) zs_sale_amt_12,
	sum(case when a.s_month ='202512' and  a.delivery_type_code=2 and a.order_channel_code not in (6, 4) and refund_order_flag='0' then profit/10000 end) as zs_profit_12,
	sum(case when a.s_month ='202511' then sale_amt/10000 end) sale_amt_11,
	sum(case when a.s_month ='202511' then profit/10000 end) profit_11,	
	sum(case when a.s_month ='202510' then sale_amt/10000 end) sale_amt_10,
	sum(case when a.s_month ='202510' then profit/10000 end) profit_10,
	sum(case when a.s_month ='202509' then sale_amt/10000 end) sale_amt_09,
	sum(case when a.s_month ='202509' then profit/10000 end) profit_09,
	sum(case when a.s_month ='202508' then sale_amt/10000 end) sale_amt_08,
	sum(case when a.s_month ='202508' then profit/10000 end) profit_08,
	sum(case when a.sdt='20251210' then sale_amt/10000 end) yester_sale_amt,
	sum(case when a.sdt='20251210' then profit/10000 end) yester_profit,
	sum(case when a.sdt='20251209' then sale_amt/10000 end) yester_sale_amt_09,
	sum(case when a.sdt='20251209' then profit/10000 end) yester_profit_09,
	max(i.target_rate) target_rate,
	i.target_profit,
	i.sale_base,
	p.operater
from 
	(select 
	    *,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		substr(sdt,1,6) s_month
	from    csx_dws.csx_dws_sale_detail_di
	where 
		sdt>= regexp_replace(add_months(trunc('${yester_day}','MM'),-4),'-','') 
		and sdt<='${yester}'
		and business_type_code=1  
		and shipper_code='YHCSX'
	) a	
	-- 客户数据
	left join 
	(select * 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
	) d on a.customer_code=d.customer_code 
	-- 关联客户
	left join 
	(select a.* ,b.customer_name as link_customername
	 from 
		(select
			customer_code as link_customercode,  
			get_json_object(item_json, '$.customerCode') as customer_code -- 这个字段是主客户信息
		FROM csx_ods.csx_ods_csx_price_prod_customer_config_df
		LATERAL VIEW explode(split(regexp_replace(substr(customer_link, 2, length(customer_link) - 2), '\\}\\,', '\\}\\|\\|'), '\\|\\|')) r1 AS item_json
		where sdt='${yester}'
		)a
		left join -- 取关联客户名称
		(select customer_code,customer_name from csx_dim.csx_dim_crm_customer_info where sdt='current') b on a.link_customercode=b.customer_code	
	) e on a.customer_code=e.customer_code		
	-- 客户管家、销售、经理
	left join  	
	(select
		customer_no,
		coalesce(work_no,'') work_no,
		coalesce(sales_name,'') sales_name, 
		coalesce(second_supervisor_work_no,'') second_supervisor_work_no,
		coalesce(second_supervisor_name,'') second_supervisor_name,
		coalesce(rp_service_user_work_no_new, '') as rp_service_user_work_no_new, -- 管家
		coalesce(rp_service_user_name_new, '') as rp_service_user_name_new		
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt = '${yester}'
	) f on a.customer_code = f.customer_no
	-- 采购参与/销售管理
	left join 
    (select
        code as type,
        max(name) as name,
        max(extra) as extra 
    from csx_dim.csx_dim_basic_topic_dict_df
    where parent_code = 'direct_delivery_type' 
    group by code 
    ) h on a.direct_delivery_type=h.type 
	-- 客户目标
	left join 
	(SELECT customer_code, target_rate,price_name,sale_base,target_profit 
     FROM csx_analyse_tmp.linkcustomer_target_rate
	) i on a.customer_code=i.customer_code		
	inner join csx_analyse_tmp.csx_analyse_tmp_link_customer_info j on a.customer_code=j.customer_code
	left join csx_analyse_tmp.city_operater_tmp p on a.performance_city_name=p.city_name 
    where h.extra='采购参与'		
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	j.link_customer_code,
    j.link_customer_name,	
	f.work_no,
	f.sales_name,
	f.second_supervisor_work_no,
	f.second_supervisor_name,
	f.rp_service_user_work_no_new,
	f.rp_service_user_name_new,	
	i.price_name,
	i.target_profit,
	i.sale_base,
	p.operater,
	d.bloc_code
	)
	select * from tmp_sale_detail a 
;

select a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
-- 	a.customer_code,
-- 	a.customer_name,
	a.link_customer_code,
    a.link_customer_name,
	a.work_no,
	a.sales_name,
	a.second_supervisor_work_no,
	a.second_supervisor_name,
	a.rp_service_user_work_no_new,
	a.rp_service_user_name_new,
	a.operater,
	a.bloc_count,
	a.customer_count,
	a.sale_amt_11,
	a.profit_11,
	a.profit_rate_11,
	a.sale_amt_12,
	a.profit_12,
	a.profit_rate_12,
	a.diff_profit_rate_12_11,
	a.tj_amt,
	 -- 调价影响
    CASE WHEN sale_amt_12 > 0 AND (sale_amt_12 - tj_amt) > 0 
         THEN profit_rate_12 - (profit_12 - tj_amt)/(sale_amt_12 - tj_amt) 
         ELSE 0 END AS tj_profit_effect,
	a.zs_sale_amt_12,
	a.zs_profit_12,
	-- 直送影响
	CASE WHEN sale_amt_12 > 0 AND (sale_amt_12 - zs_sale_amt_12) > 0 
         THEN profit_rate_12 - (profit_12 - zs_profit_12)/(sale_amt_12 - zs_sale_amt_12) 
         ELSE 0 END AS zs_price_effect,	
	a.yester_sale_amt,
	a.yester_profit,
	a.yester_profit/yester_sale_amt as profit_rate_yester,
	yester_sale_amt_09,
	yester_profit_09,
	yester_profit_09/yester_sale_amt_09 yester_profit_rate_09,
	a.sale_amt_10,
	a.profit_10,
	a.profit_10/a.sale_amt_10 as profit_rate_10,
	a.sale_amt_09,
	a.profit_09,
	a.profit_09/a.sale_amt_09 as profit_rate_09,
	a.sale_amt_08,
	a.profit_08,
	a.profit_08/a.sale_amt_08 as profit_rate_08
	-- a.target_rate,
	-- a.target_profit,
	-- a.sale_base

from (
	select a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	-- a.customer_code,
	-- a.customer_name,
	a.link_customer_code,
    a.link_customer_name,
	concat_ws(',',collect_set(work_no))  work_no,
	concat_ws(',',collect_set(sales_name)) sales_name,
	concat_ws(',',collect_set(second_supervisor_work_no)) second_supervisor_work_no,
	concat_ws(',',collect_set(second_supervisor_name)) second_supervisor_name,
	concat_ws(',',collect_set(rp_service_user_work_no_new)) rp_service_user_work_no_new,
	concat_ws(',',collect_set(rp_service_user_name_new)) rp_service_user_name_new,	
	a.operater,
	count(distinct bloc_code) bloc_count,
	count(a.customer_code) as customer_count,
	sum(sale_amt_11) sale_amt_11,
	sum(profit_11) profit_11,
	sum(profit_11)/sum(sale_amt_11) as profit_rate_11,
	sum(sale_amt_12) sale_amt_12,
	sum(profit_12) profit_12,
	sum(profit_12)/sum(sale_amt_12) as profit_rate_12,
	sum(profit_12)/sum(sale_amt_12)-sum(profit_11)/sum(sale_amt_11) as diff_profit_rate_12_11,

	sum(tj_amt) as tj_amt,
	
	sum(zs_sale_amt_12) zs_sale_amt_12,
	sum(zs_profit_12) as zs_profit_12,
	
	sum(sale_amt_10) sale_amt_10,
	sum(profit_10) profit_10,
	sum(profit_10)/sum(sale_amt_10) as profit_rate_10,
	sum( sale_amt_09) sale_amt_09,
	sum( profit_09) profit_09,
	sum( profit_09)/sum(sale_amt_09) as profit_rate_09,
	sum( sale_amt_08) sale_amt_08,
	sum( profit_08) profit_08,
	sum( profit_08)/sum(sale_amt_08) as profit_rate_08,
	sum( yester_sale_amt) yester_sale_amt,
	sum(yester_profit) yester_profit,
	sum(yester_profit)/sum(yester_sale_amt) as profit_rate_yester,
	sum( yester_sale_amt_09) yester_sale_amt_09,
	sum(yester_profit_09) yester_profit_09,
	sum(yester_profit_09)/sum(yester_sale_amt_09) as profit_rate_yester_09
	from csx_analyse_tmp.csx_analyse_tmp_sale_detial_01 a 
	group by a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.link_customer_code,
    a.link_customer_name,
	a.operater

) a 
	;


-- 明细

select a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.customer_name,
	a.link_customer_code,
    a.link_customer_name,
    d.bloc_name,
	a.work_no,
	a.sales_name,
	a.second_supervisor_work_no,
	a.second_supervisor_name,
	a.rp_service_user_work_no_new,
	a.rp_service_user_name_new,
	a.operater,
	a.sale_amt_11,
	a.profit_11,
	profit_11/sale_amt_11 as  profit_rate_11,
	a.sale_amt_12,
	a.profit_12,
	profit_12/sale_amt_12 profit_rate_12,
	profit_12/sale_amt_12 - profit_11/sale_amt_11  diff_profit_rate_12_11,
	a.tj_amt,
	 -- 调价影响
    CASE WHEN sale_amt_12 > 0 AND (sale_amt_12 - tj_amt) > 0 
         THEN profit_12/sale_amt_12 - (profit_12 - tj_amt)/(sale_amt_12 - tj_amt) 
         ELSE 0 END AS tj_profit_effect,
	a.zs_sale_amt_12,
	a.zs_profit_12,
	a.zs_profit_12/zs_sale_amt_12 as zs_profit_rate,
	-- 直送影响
	CASE WHEN sale_amt_12 > 0 AND (sale_amt_12 - zs_sale_amt_12) > 0 
         THEN profit_12/sale_amt_12 - (profit_12 - zs_profit_12)/(sale_amt_12 - zs_sale_amt_12) 
         ELSE 0 END AS zs_price_effect,	
	a.yester_sale_amt,
	a.yester_profit,
	a.yester_profit/yester_sale_amt as profit_rate_yester,
	yester_sale_amt_09,
	yester_profit_09,
	yester_profit_09/yester_sale_amt_09 as yester_profit_rate_09,
	a.yester_profit/yester_sale_amt- profit_11/sale_amt_11 as diff_profit_rate_yester,
	a.sale_amt_10,
	a.profit_10,
	a.profit_10/a.sale_amt_10 as profit_rate_10,
	a.sale_amt_09,
	a.profit_09,
	a.profit_09/a.sale_amt_09 as profit_rate_09,
	a.sale_amt_08,
	a.profit_08,
	a.profit_08/a.sale_amt_08 as profit_rate_08
	-- a.target_rate,
	-- a.target_profit,
	-- a.sale_base

from  csx_analyse_tmp.csx_analyse_tmp_sale_detial_01 a 
	left join 
	(select * 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
	) d on a.customer_code=d.customer_code 