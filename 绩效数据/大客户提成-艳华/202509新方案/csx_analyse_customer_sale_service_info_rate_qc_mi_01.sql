-- ******************************************************************** 
-- @功能描述：
-- @创建者： 饶艳华 
-- @创建者日期：2025-09-30 17:56:40 
-- @修改者日期：
-- @修改人：
-- @修改内容：更新系数,新方案测试
-- ******************************************************************** 

-- 计算管家提成分配系数
create table  csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi as 
with tmp_sale_detail as 
(
SELECT 
  substr(sdt,1,6) mon,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  case 
  when abs(sum(a.sale_amt)) = 0 then 0 
  else sum(a.profit)/abs(sum(a.sale_amt)) 
end as profit_rate
from    csx_dws.csx_dws_sale_detail_di a
 
  where sdt>=regexp_replace(trunc(last_day(add_months('2025-09-22',-1)),'MM'),'-','')
  and sdt<=regexp_replace(last_day(add_months('2025-09-22',-1)),'-','')
  and business_type_code='1'
  and partner_type_code not  in (1, 3) 
  group by 
  substr(sdt,1,6) ,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name
  ) ,
tmp_customer_sale_detail as  
(SELECT 
  substr(sdt,1,6) mon,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  business_type_code,
  a.business_type_name,  
  sales_user_number,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  case 
  when abs(sum(a.sale_amt)) = 0 then 0 
  else sum(a.profit)/abs(sum(a.sale_amt)) 
end as profit_rate
from     csx_dws.csx_dws_sale_detail_di a
  where sdt>=regexp_replace(trunc(last_day(add_months('2025-09-22',-1)),'MM'),'-','')
  and sdt<=regexp_replace(last_day(add_months('2025-09-22',-1)),'-','')
--   and business_type_code='1'
  group by 
  substr(sdt,1,6) ,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.business_type_name,
  business_type_code,
  customer_code,
  sales_user_number
  ) ,

  tmp_customer_info as 
(
select 
	distinct 
	concat_ws('-',substr(regexp_replace(add_months('2025-09-22',-1),'-',''), 1, 6),a.customer_no) as biz_id,
	a.customer_id,a.customer_no,a.customer_name,
	a.channel_code,a.channel_name,
	a.region_code,a.region_name,
	a.province_code,a.province_name,
	a.city_group_code,a.city_group_name,
	sales_id_new as sales_id,
	work_no_new as work_no,
	sales_name_new as sales_name,
	rp_service_user_id_new as rp_service_user_id,
	rp_service_user_work_no_new as rp_service_user_work_no,		
	rp_service_user_name_new as rp_service_user_name,

	fl_service_user_id_new as fl_service_user_id,
	fl_service_user_work_no_new as fl_service_user_work_no,
	fl_service_user_name_new as fl_service_user_name,

	bbc_service_user_id_new as bbc_service_user_id,	
	bbc_service_user_work_no_new as bbc_service_user_work_no,
	bbc_service_user_name_new as bbc_service_user_name,	
  -- 销售系数
	 0.6 as rp_sales_fp_rate,
	case 
		 when length(fl_service_user_id_new)<>0 and length(work_no_new)>0 then 0.6
		 when length(work_no_new)>0 then 1
		 end as fl_sales_fp_rate,	
	case 
		 when length(bbc_service_user_id_new)<>0 and length(work_no_new)>0 then 0.6
		 when length(work_no_new)>0 then 1
		 end as bbc_sales_fp_rate,
  -- 管家系数
	case when length(rp_service_user_id_new)<>0 then allocation_coefficient  end as rp_service_user_fp_rate,
	case when length(fl_service_user_id_new)<>0 then allocation_coefficient end as fl_service_user_fp_rate,
	case when length(bbc_service_user_id_new)<>0  then allocation_coefficient end as bbc_service_user_fp_rate,    
	customer_profit_rate,
	city_profit_rate,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(add_months('2025-09-22',-1),'-',''), 1, 6) as smt -- 统计日期
from 
(
	select a.*,
	    c.profit_rate as customer_profit_rate,
	    b.profit_rate as city_profit_rate,
   case when a.work_no_new='' or a.work_no_new is null then 0.4
        when b.profit_rate>=c.profit_rate then 0.2
        when b.profit_rate< c.profit_rate then 0.1
        else 0.4 end as allocation_coefficient 
  from     csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df  a 
    
  -- 客户维度毛利率      
  left join tmp_customer_sale_detail  c on a.customer_no=c.customer_code
  -- 城市维度毛利率
  left join tmp_sale_detail b on a.city_group_name=b.performance_city_name
	
	where sdt=regexp_replace(last_day(add_months('2025-09-22',-1)),'-','')
)a
left join 
(
	select * 
	-- from csx_analyse.csx_analyse_tc_customer_special_rules_mf 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf
	where smt=substr(regexp_replace(last_day(add_months('2025-09-22',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('2025-09-22',-1)),'-',''),1,6)
	and category_first like '%大客户提成-调整对应人员比例%'
) d on d.customer_code=a.customer_no
where d.category_first is null
union all 
select 
	biz_id,
	customer_id,
	customer_no,
	customer_name,
	channel_code,
	channel_name,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	sales_id,
	work_no,
	sales_name,
	rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,
	bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	cast(rp_sales_sale_fp_rate as decimal(20,6)) rp_sales_sale_fp_rate,
	cast(fl_sales_sale_fp_rate as decimal(20,6)) fl_sales_sale_fp_rate,
	cast(bbc_sales_sale_fp_rate as decimal(20,6)) bbc_sales_sale_fp_rate,
	cast(rp_service_user_sale_fp_rate as decimal(20,6)) rp_service_user_fp_rate,
	cast(fl_service_user_sale_fp_rate as decimal(20,6)) fl_service_user_fp_rate,
	cast(bbc_service_user_sale_fp_rate as decimal(20,6)) bbc_service_user_fp_rate,
	0 as customer_profit_rate,
	0 as city_profit_rate,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	smt
from csx_analyse.csx_analyse_tc_customer_person_rate_special_rules_mf
where smt=substr(regexp_replace(last_day(add_months('2025-09-22',-1)),'-',''),1,6)
and smt_date=substr(regexp_replace(last_day(add_months('2025-09-22',-1)),'-',''),1,6)
) 
-- insert overwrite table   csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi partition(smt)
select 	biz_id,
	a.customer_id,
	a.customer_no,
	b.customer_name,
	a.channel_code,
	a.channel_name,
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	sales_id,
	work_no,
	sales_name,
	rp_service_user_id,
	rp_service_user_work_no,		
	rp_service_user_name,

	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,

	bbc_service_user_id,	
	bbc_service_user_work_no,
	bbc_service_user_name,	
    rp_sales_fp_rate,
    fl_sales_fp_rate,	
    bbc_sales_fp_rate,
	-- 按照新方案浙江、华西无销售员的管家按照旧方案系数	 
	-- 安徽(11)、浙江未挂销售的客户包含B,由管家独立维护,该客户分配系数申请按40%进行核算提成。
    -- 北京延迟一个月按照原方案执行
	rp_service_user_fp_rate,
	fl_service_user_fp_rate,
	bbc_service_user_fp_rate,     	
	update_time,
	customer_profit_rate,
	city_profit_rate,
	smt
from tmp_customer_info a 
left join 
(select customer_code,customer_name from csx_dim.csx_dim_crm_customer_info where sdt='current' ) b on a.customer_no=b.customer_code
;
