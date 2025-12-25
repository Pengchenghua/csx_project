-- ******************************************************************** 
-- @功能描述：
-- @创建者： 公会敏 
-- @创建者日期：2025-10-17 18:14:48 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 



 WITH csx_analyse_tmp_link_customer_info as 
 -- 重点客户及集团客户目标
(select a.*,b.target_rate,price_name,sale_base,target_profit
 from csx_analyse_tmp.csx_analyse_tmp_link_customer_info a 
left join csx_analyse_tmp.csx_analyse_tmp_link_customer_profit_target b 
	on a.bloc_code=b.bloc_code
)
insert overwrite table csx_analyse.csx_analyse_fr_linkcustomer_top_sales 
select
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
	coalesce(j.price_name,p.operater) price_name ,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(last_day(add_months('${yester_day}',-1)),'-','') then sale_amt/10000 end) sy_whole_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(last_day(add_months('${yester_day}',-1)),'-','') then profit/10000 end) sy_whole_profit,	
	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yester_day}',-1),'-','') then sale_amt/10000 end) sy_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yester_day}',-1),'-','')  then profit/10000 end) as sy_profit,
	
	sum(case when a.sdt >= regexp_replace(trunc('${yester_day}','MM'),'-','') and a.sdt <= '${yester}' then sale_amt/10000 end) as by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${yester_day}','MM'),'-','') and a.sdt <= '${yester}' then profit/10000 end) as by_profit,

	sum(case when a.sdt = '${yester}' then sale_amt/10000 end) as r_sale_amt,
	sum(case when a.sdt = '${yester}' then profit/10000 end) as r_profit,
	
	max(cast(target_rate as decimal(16,4) )) target_rate,       -- 目标毛利率
	
    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-6-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-0-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then sale_amt/10000 end) sz_sale_amt,

    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-6-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-0-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then profit/10000 end) sz_profit,

    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and  a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),7-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then sale_amt/10000 else 0 end) bz_sale_amt,

    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and  a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),7-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then profit/10000 else 0 end) bz_profit,
	
	max(cast(target_profit as decimal(16,4) ) ) target_profit,
    max(cast( sale_base as decimal(16,4) )) sale_base,
    
    --  月调价与直送
    sum(case when a.sdt >= regexp_replace(trunc('${yester_day}','MM'),'-','') and a.sdt <= '${yester}' and a.order_channel_code=6  then profit/10000 end) as by_tj_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${yester_day}','MM'),'-','') and a.sdt <= '${yester}' and  a.delivery_type_code=2 and a.order_channel_code not in (6, 4) and refund_order_flag='0' then sale_amt/10000 end) by_zs_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${yester_day}','MM'),'-','') and a.sdt <= '${yester}' and  a.delivery_type_code=2 and a.order_channel_code not in (6, 4) and refund_order_flag='0' then profit/10000 end) as by_zs_profit,
	
	-- 周调价与直送
	sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
        and  a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),7-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
        and a.order_channel_code=6  then sale_amt/10000 else 0 end) bz_tj_amt,
    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
        and  a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),7-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
        and  a.delivery_type_code=2 and a.order_channel_code not in (6, 4) and refund_order_flag='0'   then sale_amt/10000 else 0 end) bz_zs_sale_amt,
	sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
        and  a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),7-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','')
        and  a.delivery_type_code=2 and a.order_channel_code not in (6, 4) and refund_order_flag='0'  then profit/10000 else 0 end) bz_zs_profit,
	
	j.bloc_code,
	j.bloc_name,
	if(j.customer_code is not null ,1, 0) key_customer_flag	,-- 重点客户标识
		current_timestamp() update_time

from 
	(select 
	    *,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week
	from csx_dws.csx_dws_sale_detail_di
	where 
		sdt>=regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') 
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
	left join csx_analyse_tmp_link_customer_info j on a.customer_code=j.customer_code
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
	j.bloc_code,
	j.bloc_name,
	coalesce(j.price_name,p.operater) ,
	if(j.customer_code is not null ,1, 0)
	;


-- 每日推送帆软SQL

	WITH base_data AS (
    SELECT 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        bloc_code,
        bloc_name,
        link_customercode as link_customer_code,
        link_customername as link_customer_name,
        sales_name,
        second_supervisor_name,
        rp_service_user_name_new,
        price_name,
        target_rate,
        customer_code,
        if(bloc_code='297509',210,sy_whole_sale_amt) sy_whole_sale_amt,
        if(bloc_code='297509',-13.2,sy_whole_profit) sy_whole_profit,
        sy_sale_amt,
        sy_profit,
        by_sale_amt,
        by_profit,
        r_sale_amt,
        r_profit,
        sz_sale_amt,
        sz_profit,
        bz_sale_amt,
        bz_profit,
        target_profit,
        if(bloc_code='297509',210,sale_base) as sale_base,
	   by_zs_sale_amt,
        by_zs_profit,
        by_tj_amt
    FROM csx_analyse.csx_analyse_fr_linkcustomer_top_sales
   where key_customer_flag='1'
   AND  performance_region_name in ('${dq}')
),
-- 城市销售求占比
city_sale_ratio AS (
    SELECT 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        sum(by_sale_amt) city_sale_amt,
        sum(sum(by_sale_amt))over(partition by performance_region_name,performance_province_name) province_sale_amt,
        sum(sum(by_sale_amt))over(partition by performance_region_name) region_sale_amt
	from csx_analyse.csx_analyse_fr_linkcustomer_top_sales
	group by performance_region_name,
        performance_province_name,
        performance_city_name
),
main_aggregates AS (
    SELECT 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        bloc_code,
        bloc_name,
        link_customer_code,
        link_customer_name,
        price_name as price_names,  
        MAX(target_rate) as target_rate,
        COUNT(customer_code) as customer_num,
        SUM(sy_whole_sale_amt) as sy_whole_sale_amt,
        SUM(sy_whole_profit) as sy_whole_profit,
        SUM(sy_sale_amt) as sy_sale_amt,
        SUM(sy_profit) as sy_profit,
        SUM(by_sale_amt) as by_sale_amt,
        SUM(by_profit) as by_profit,
        SUM(r_sale_amt) as r_sale_amt,
        SUM(r_profit) as r_profit,
        SUM(sz_sale_amt) as sz_sale_amt,
        SUM(sz_profit) as sz_profit,
        SUM(bz_sale_amt) as bz_sale_amt,
        SUM(bz_profit) as bz_profit,
        MAX(target_profit) as target_profit,
        MAX(sale_base) as sale_base,
		sum(by_zs_sale_amt) as by_zs_sale_amt,
        SUM(by_zs_profit) as by_zs_profit,
        SUM(by_tj_amt) as by_tj_amt
    FROM base_data
    GROUP BY 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        bloc_code,
        bloc_name,
        link_customer_code,
        link_customer_name,
        price_name
),
bloc_counts AS (
    SELECT DISTINCT
        performance_region_name,
        performance_province_name,
        performance_city_name,
        bloc_code,
        bloc_name 
    FROM base_data
     
)
SELECT 
    m.*,
    b.bloc_name_count,
	c.city_sale_amt,
	c.province_sale_amt,
	c.region_sale_amt
FROM main_aggregates m
LEFT JOIN 
(
    SELECT  
        performance_region_name,
        performance_province_name,
        performance_city_name,
        bloc_code,
        bloc_name,
        COUNT(bloc_name)over(partition by performance_region_name,
        performance_province_name,
        performance_city_name,
        bloc_code) as bloc_name_count
    FROM bloc_counts
     
)b
    ON m.performance_region_name = b.performance_region_name
   AND m.performance_province_name = b.performance_province_name
   AND m.performance_city_name = b.performance_city_name
   AND m.bloc_code = b.bloc_code
left join city_sale_ratio c
    ON m.performance_region_name = c.performance_region_name
   AND m.performance_province_name = c.performance_province_name
   AND m.performance_city_name = c.performance_city_name
 ORDER BY by_sale_amt DESC;