-- ******************************************************************** 
-- @功能描述：
-- @创建者： 公会敏 
-- @创建者日期：2025-10-17 18:14:48 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
-- csx_analyse_tmp.linkcustomer_target_rate 重点客户清单 如果需要删除客户，需要导出来后，再删除，再导入

insert overwrite table csx_analyse.csx_analyse_fr_linkcustomer_top_sales 

select
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	case 
        when d.customer_name like '%总医院%'  or e.link_customername like '%总医院%' then '301'
        when e.link_customercode is not null then e.link_customercode
        else a.customer_code
    end as link_customercode,
    case 
        when d.customer_name like '%总医院%'  or e.link_customername like '%总医院%' then '301医院'
        when e.link_customername is not null then e.link_customername
        else a.customer_name
    end as link_customername,
	f.work_no,
	f.sales_name,
	f.second_supervisor_work_no,
	f.second_supervisor_name,
	f.rp_service_user_work_no_new,
	f.rp_service_user_name_new,	
	i.price_name,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(last_day(add_months('${yester_day}',-1)),'-','') then sale_amt/10000 end) sy_whole_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(last_day(add_months('${yester_day}',-1)),'-','') then profit/10000 end) sy_whole_profit,	
	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yester_day}',-1),'-','') then sale_amt/10000 end) sy_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${yester_day}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yester_day}',-1),'-','')  then profit/10000 end) as sy_profit,
	
	sum(case when a.sdt >= regexp_replace(trunc('${yester_day}','MM'),'-','') and a.sdt <= '${yester}' then sale_amt/10000 end) as by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${yester_day}','MM'),'-','') and a.sdt <= '${yester}' then profit/10000 end) as by_profit,

	sum(case when a.sdt = '${yester}' then sale_amt/10000 end) as r_sale_amt,
	sum(case when a.sdt = '${yester}' then profit/10000 end) as r_profit,
	
	max(i.target_rate) target_rate,
	
    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-6-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-0-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then sale_amt/10000 end) sz_sale_amt,

    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-6-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),-0-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then profit/10000 end) sz_profit,

    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and  a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),7-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then sale_amt/10000 else 0 end) bz_sale_amt,

    sum(case when a.sdt >=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') 
    and  a.sdt <=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),7-dayofweek(date_add(from_unixtime(unix_timestamp('${sdt3}','yyyyMMdd'),'yyyy-MM-dd'),1))),'-','') then profit/10000 else 0 end) bz_profit,
	
	i.target_profit,
	i.sale_base
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
	join 
	(SELECT customer_code, target_rate,price_name,sale_base,target_profit 
     FROM csx_analyse_tmp.linkcustomer_target_rate
	) i on a.customer_code=i.customer_code		
    where h.extra='采购参与'		
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	case 
        when d.customer_name like '%总医院%'  or e.link_customername like '%总医院%' then '301'
        when e.link_customercode is not null then e.link_customercode
        else a.customer_code
    end,
    case 
        when d.customer_name like '%总医院%'  or e.link_customername like '%总医院%' then '301医院'
        when e.link_customername is not null then e.link_customername
        else a.customer_name
    end,	
	f.work_no,
	f.sales_name,
	f.second_supervisor_work_no,
	f.second_supervisor_name,
	f.rp_service_user_work_no_new,
	f.rp_service_user_name_new,	
	i.price_name,
	i.target_profit,
	i.sale_base;