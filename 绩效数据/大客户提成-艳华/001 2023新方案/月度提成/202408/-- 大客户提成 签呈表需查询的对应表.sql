-- 大客户提成 签呈表需查询的对应表

-- 确认需对哪些客服经理补充等级比例 计划1--3号提供
select distinct
	a.performance_province_name,b.flag,b.user_work_no,b.user_name,
	d.s_level,d.level_sale_rate,d.level_profit_rate,
	sum(sale_amt) sale_amt,sum(profit) profit
from
	(
		select 
			performance_province_code,performance_province_name,customer_code,
			substr(sdt,1,6) smonth,sum(sale_amt) sale_amt,sum(profit) profit
		from csx_dws.csx_dws_sale_detail_di
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
			and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and channel_code in('1','7')
		group by performance_province_code,performance_province_name,customer_code,substr(sdt,1,6)
	)a	
left join 
(
	select 
		'客服经理' flag,
		customer_no,
		rp_service_user_work_no_new as user_work_no,
		rp_service_user_name_new as user_name,
		rp_service_user_id_new as user_id
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and rp_service_user_id_new is not null
	union all
	select 
		'客服经理' flag,
		customer_no,
		fl_service_user_work_no_new as user_work_no,
		fl_service_user_name_new as user_name,
		fl_service_user_id_new as user_id
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and fl_service_user_id_new is not null
	union all	
	select 
		'客服经理' flag,
		customer_no,
		bbc_service_user_work_no_new as user_work_no,
		bbc_service_user_name_new as user_name,
		bbc_service_user_id_new as user_id
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and bbc_service_user_id_new is not null
) b on b.customer_no=a.customer_code
left join 
(
	select *
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-2),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
	-- where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	-- and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- 不算提成 此类人员需再次确认
	and salary_level<>'不算提成'
)d on b.user_work_no=d.service_user_work_no	
where d.s_level is null
group by a.performance_province_name,b.flag,b.user_work_no,b.user_name,
d.s_level,d.level_sale_rate,d.level_profit_rate
;	


-- 客户销售员管家对应关系 ★★★★先维护好管家等级表 X月客户销售员对照表  
select 
a.customer_no,
a.region_name,
a.province_name,
a.city_group_name,
a.channel_name,
a.customer_no,
a.customer_name,
a.sales_id_new,
a.work_no_new,
a.sales_name_new,
a.rp_service_user_id_new,
a.rp_service_user_work_no_new,
a.rp_service_user_name_new,
a.fl_service_user_id_new,
a.fl_service_user_work_no_new,
a.fl_service_user_name_new,
a.bbc_service_user_id_new,
a.bbc_service_user_work_no_new,
a.bbc_service_user_name_new,
-- 202405签呈 每月 浙江全部福利BBC业务调整提成比例，销售员按100%，管家0%
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when length(a.rp_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as rp_sales_sale_rate,
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.fl_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as fl_sales_sale_rate,
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.bbc_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as bbc_sales_sale_rate,
b1.level_sale_rate as rp_service_user_fp_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b2.level_sale_rate end as fl_service_user_fp_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b3.level_sale_rate end as bbc_service_user_fp_rate    
from 
(
select *
from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
where sdt='20240731'
)a 
-- 关联管家等级	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b1 on a.rp_service_user_work_no_new=b1.service_user_work_no	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b2 on a.fl_service_user_work_no_new=b2.service_user_work_no
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b3 on a.bbc_service_user_work_no_new=b3.service_user_work_no
;

--用户信息
select user_id,user_number,user_name,province_name,city_name from csx_dim.csx_dim_uc_user where sdt='current' and user_name='杨权'

-- 调整对应人员比例查询
select 
concat_ws('-',substr(a.sdt,1,6),a.customer_no) as biz_id,
a.customer_id,
a.customer_no,
a.customer_name,
a.channel_code,
a.channel_name,
a.region_code,
a.region_name,
a.province_code,
a.province_name,
a.city_group_code,
a.city_group_name,
a.sales_id_new,
a.work_no_new,
a.sales_name_new,
a.rp_service_user_id_new,
a.rp_service_user_work_no_new,
a.rp_service_user_name_new,
a.fl_service_user_id_new,
a.fl_service_user_work_no_new,
a.fl_service_user_name_new,
a.bbc_service_user_id_new,
a.bbc_service_user_work_no_new,
a.bbc_service_user_name_new,

case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when length(a.rp_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as rp_sales_sale_rate,
null rp_sales_profit_rate,
	
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.fl_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as fl_sales_sale_rate,
null fl_sales_profit_rate,	

case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.bbc_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as bbc_sales_sale_rate,
null bbc_sales_profit_rate,	

b1.level_sale_rate as rp_service_user_sale_rate,
null rp_service_user_profit_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b2.level_sale_rate end as fl_service_user_sale_rate,
null fl_service_user_profit_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b3.level_sale_rate end as bbc_service_user_sale_rate,
null bbc_service_user_profit_rate,
substr(a.sdt,1,6) smt_date
from 
(
select *
from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df 
where sdt='20240731'

)a 
-- 关联管家等级	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b1 on a.rp_service_user_work_no_new=b1.service_user_work_no	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b2 on a.fl_service_user_work_no_new=b2.service_user_work_no
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b3 on a.bbc_service_user_work_no_new=b3.service_user_work_no
order by a.province_name
;
