-- ******************************************************************** 
-- @功能描述：
-- @创建者： 饶艳华 
-- @创建者日期：2025-09-30 17:56:40 
-- @修改者日期：
-- @修改人：
-- @修改内容：更新系数,新方案测试
-- ******************************************************************** 

-- 计算管家提成分配系数
-- drop table  csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi 
create table  csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi as 
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
  where sdt>=regexp_replace(trunc(last_day(add_months('${edate}',-1)),'MM'),'-','')
  and sdt<=regexp_replace(last_day(add_months('${edate}',-1)),'-','')
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
  where sdt>=regexp_replace(trunc(last_day(add_months('${edate}',-1)),'MM'),'-','')
  and sdt<=regexp_replace(last_day(add_months('${edate}',-1)),'-','')
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
	concat_ws('-',substr(regexp_replace(add_months('${edate}',-1),'-',''), 1, 6),a.customer_no) as biz_id,
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
	substr(regexp_replace(add_months('${edate}',-1),'-',''), 1, 6) as smt -- 统计日期
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
  left join tmp_sale_detail b on if(a.city_group_name='宁德市','福州市',a.city_group_name)=b.performance_city_name
	
	where sdt=regexp_replace(last_day(add_months('${edate}',-1)),'-','')
)a

) 
-- select * from  tmp_customer_info
,
tmp_position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
tmp_sales_info as (
  select a.*,b.name as user_position_name,c.name as leader_position_name from 
  (select
    a.user_id,
    a.user_number,
    a.user_name,
    a.source_user_position,
    a.leader_user_id,
    b.user_number as leader_user_number,
    b.user_name as leader_user_name,
    b.source_user_position as leader_user_position
  from
       csx_dim.csx_dim_uc_user a
    left join (
      select
        user_id,
        user_number,
        user_name,
        source_user_position
      from
        csx_dim.csx_dim_uc_user a
      where
        sdt = regexp_replace(last_day(add_months('${edate}',-1)),'-','')
        and status = 0
    ) b on a.leader_user_id = b.user_id
  where
    sdt = regexp_replace(last_day(add_months('${edate}',-1)),'-','')
    and status = 0
    )a 
    left join tmp_position_dic b on a.source_user_position=b.code
    left join tmp_position_dic c on a.leader_user_position=c.code
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
	c.user_position_name,
	rp_service_user_id,
	rp_service_user_work_no,		
	rp_service_user_name,

	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,

	bbc_service_user_id,	
	bbc_service_user_work_no,
	bbc_service_user_name,	
    case when strategy_status=1 and coalesce(rp_service_user_work_no,'')='' then 0.5 
         when strategy_status=1 and coalesce(rp_service_user_work_no,'')!='' then 0.2 
     else rp_sales_fp_rate end rp_sales_fp_rate,
    case when strategy_status=1 and coalesce(fl_service_user_work_no,'')='' then 0.5 
         when strategy_status=1 and coalesce(fl_service_user_work_no,'')!='' then 0.2 
     else fl_sales_fp_rate end fl_sales_fp_rate,	
    case when strategy_status=1 and coalesce(bbc_service_user_work_no,'')='' then 0.5 
         when strategy_status=1 and coalesce(bbc_service_user_work_no,'')!='' then 0.2 
     else bbc_sales_fp_rate end bbc_sales_fp_rate,
	-- 按照新方案浙江、华西无销售员的管家按照旧方案系数	 
	-- 安徽(11)、浙江未挂销售的客户包含B,由管家独立维护,该客户分配系数申请按40%进行核算提成。
    -- 北京延迟一个月按照原方案执行
	rp_service_user_fp_rate,
	fl_service_user_fp_rate,
	bbc_service_user_fp_rate,     	
	update_time,
	customer_profit_rate,
	city_profit_rate,
	CASE 
    WHEN a.city_group_name IN ('北京市', '福州市', '重庆主城', '深圳市', '成都市', '上海松江', '南京主城', '合肥市', '西安市', 
									'石家庄市', '江苏苏州', '杭州市','郑州市','广东广州') THEN 'A/B'
    WHEN a.city_group_name IN ('厦门市', '宁波市', '泉州市', '莆田市', '南平市', '南昌市', '贵阳市', '宜宾', '武汉市') THEN 'C'
    WHEN a.city_group_name IN ('三明市','阜阳市','台州市','龙岩市','万州区','江苏盐城','黔江区','永川区') then 'D'
    else 'D'
    END as city_type_name,
	smt
from tmp_customer_info a 
left join 
(select customer_code,
    customer_name,
    strategy_status
from    csx_dim.csx_dim_crm_customer_info where sdt='current' 
) b on a.customer_no=b.customer_code
left join 
tmp_sales_info c on a.work_no=c.user_number
;


select * from csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi