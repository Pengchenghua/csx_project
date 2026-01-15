--大福利季度绩效
-- drop table  csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info ;
create table csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info as
with position_dic as (
  select
    dic_key as code,
    dic_value as name
  from
    csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
  where
    sdt = regexp_replace(date_sub(current_date(), 1), '-', '')
    and dic_type = 'POSITION'
    and del_flag = 0
),
tmp_basic_sale_info as (
  select
    user_id,
    user_number,
    user_name,
    user_position,
    b.name user_position_name,
    distance,
    user_position_type,
    leader_user_id,
    leader_user_number,
    leader_user_name,
    leader_user_position,
    leader_position_type,
    c.name leader_position_name
  from
    csx_dim.csx_dim_uc_user_extend a
    left join position_dic b on a.user_position = b.code
    left join position_dic c on a.leader_user_position = c.code
  where
    sdt = 'current'
),
tmp_sale_info as (
  -- 整理组织结构为"销售员-销售主管-销售总监-城市经理-省区总"的层级关系
  select
    tmp.*,
    b.user_position_name as supervisor_position,
    c.user_position_name as sales_manager_position,
    d.user_position_name as city_manager_position, 
    e.user_position_name as province_manager_position
  from
    (
      select
        user_name AS sales_user_name,
        user_number AS sales_user_number,
        user_position_type AS sales_user_position,
        -- 主管
        first_value(
          case
            when leader_position_type = 'SALES_MANAGER' then leader_user_name
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS supervisor_user_name,
        first_value(
          case
            when leader_position_type = 'SALES_MANAGER' then leader_user_number
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS supervisor_user_number,
        -- 销售经理
        first_value(
          case
            when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_id
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS sales_manager_user_id,
        first_value(
          case
            when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_name
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS sales_manager_user_name,
        first_value(
          case
            when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_number
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS sales_manager_user_number,
        -- 城市经理
        first_value(
          case
            when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_id
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS city_manager_user_id,
        first_value(
          case
            when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_name
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS city_manager_user_name,
        first_value(
          case
            when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_number
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS city_manager_user_number,
        -- 省区总
        first_value(
          case
            when leader_position_type = 'AREA_MANAGER' then leader_user_id
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS province_manager_user_id,
        first_value(
          case
            when leader_position_type = 'AREA_MANAGER' then leader_user_name
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS province_manager_user_name,
        first_value(
          case
            when leader_position_type = 'AREA_MANAGER' then leader_user_number
          end,
          true
        ) over(
          partition by user_id
          order by
            distance
        ) AS province_manager_user_number,
        row_number() over(
          partition by user_id
          order by
            distance DESC
        ) AS rank
      from
        tmp_basic_sale_info
    ) tmp 
    left join tmp_basic_sale_info b on tmp.supervisor_user_number = b.user_number
    left join tmp_basic_sale_info c on tmp.sales_manager_user_number = c.user_number 
    left join tmp_basic_sale_info d on tmp.city_manager_user_number = d.user_number 
    left join tmp_basic_sale_info e on tmp.province_manager_user_number = e.user_number 
  where
    tmp.rank = 1
)
select distinct
  customer_code as customer_no,
  business_attribute_code as attribute_code,
  business_attribute_name as attribute_name,
  cast(business_attribute_user_id as string) as business_attribute_user_id,
  business_attribute_user_name as sale_name,
  business_attribute_user_number as sale_number,
  business_attribute_user_position as sale_position,
  supervisor_user_number,
  supervisor_user_name,
  sales_manager_user_number,
  sales_manager_user_name,
  city_manager_user_number,
  city_manager_user_name,
  province_manager_user_number,
  province_manager_user_name,
  supervisor_position,
  sales_manager_position,
  city_manager_position, 
  province_manager_position
from
  csx_dim.csx_dim_crm_customer_business_ownership a 
  left join tmp_sale_info b on a.business_attribute_user_number = b.sales_user_number
where
  sdt = '${sdt_yes}' 
  and shipper_code = 'YHCSX'
--   and business_attribute_name in ('福利','福利小店','BBC')
;
select * from  csx_analyse_tmp.csx_analyse_tmp_fl_quarterly_sale_detail 
-- B端销售明细
create table csx_analyse_tmp.csx_analyse_tmp_fl_quarterly_sale_detail as 
select
  a.province_name sales_province_name,
  a.city_group_name,
  c.province_manager_user_number,
  c.province_manager_user_name,
  c.province_manager_position,
  c.city_manager_user_number,
  c.city_manager_user_name,
  c.city_manager_position,
  c.sales_manager_user_number,
  c.sales_manager_user_name,
  c.sales_manager_position,
  c.supervisor_user_number,
  c.supervisor_user_name,
  c.supervisor_position,
  c.sale_number,
  c.sale_name,
  a.customer_code customer_no,
  a.customer_name,
  a.smonth,
  a.business_type_name,
  sum(sale_amt) sale_amt,
  sum(profit) profit,
  sum(excluding_tax_sales) excluding_tax_sales,
  sum(excluding_tax_profit) excluding_tax_profit
from
  (
    select
      customer_code,
      customer_name,
      performance_province_name province_name,
      performance_city_name city_group_name,
      substr(sdt, 1, 6) smonth,
      CASE
        WHEN business_type_code in ('2', '10') then '福利'
        else business_type_name
      end business_type_name,
      sum(sale_amt) as sale_amt,
      sum(profit) profit,
      sum(profit_no_tax) excluding_tax_profit,
      sum(sale_amt_no_tax) excluding_tax_sales
    from
      csx_dws.csx_dws_sale_detail_di
    where
      sdt >= '20251001'
      and sdt <= '20251231'
      and (
        business_type_code in ('2', '6', '10')
        or inventory_dc_code in ('WD75', 'WD76', 'WD77', 'WD78', 'WD79', 'WD80', 'WD81')
      )
    group by
      customer_code,
      customer_name,
      performance_province_name,
      performance_city_name,
      substr(sdt, 1, 6),
      CASE
        WHEN business_type_code in ('2', '10') then '福利'
        else business_type_name
      end
  ) a
  LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c on a.customer_code = c.customer_no
  and a.business_type_name = c.attribute_name
group by
  a.province_name,
  a.city_group_name,
  c.province_manager_user_number,
  c.province_manager_user_name,
  c.province_manager_position,
  c.city_manager_user_number,
  c.city_manager_user_name,
  c.city_manager_position,
  c.sales_manager_user_number,
  c.sales_manager_user_name,
  c.sales_manager_position,
  c.supervisor_user_number,
  c.supervisor_user_name,
  c.supervisor_position,
  c.sale_number,
  c.sale_name,
  a.customer_code,
  a.customer_name,
  a.business_type_name,
  a.smonth;

--大宗仓
WD75上海彩食鲜大宗酒水DC
WD76安徽彩食鲜大宗酒水DC
WD77北京彩食鲜大宗酒水DC
WD78湖北彩食鲜大宗酒水DC
WD79福建彩食鲜大宗酒水DC
WD80四川彩食鲜大宗酒水DC
WD81重庆彩食鲜大宗酒水DC

--B端销售汇总
select
    a.sales_province_name      ,
    a.city_group_name,
    a.supervisor_user_number,
    a.supervisor_user_name,
    a.supervisor_position,
    a.sales_manager_user_number,
    a.sales_manager_user_name,
    a.sales_manager_position,
    a.city_manager_user_number,
    a.city_manager_user_name,
    a.city_manager_position,
    a.province_manager_user_number,
    a.province_manager_user_name,
    a.province_manager_position   ,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(excluding_tax_sales) excluding_tax_sales,
    sum(excluding_tax_profit) excluding_tax_profit,
    sum(excluding_tax_profit)/sum(excluding_tax_sales) as excluding_tax_profit_rate
from   csx_analyse_tmp.csx_analyse_tmp_fl_quarterly_sale_detail a
group by 
      a.sales_province_name,
      a.city_group_name,
      a.supervisor_user_number,
      a.supervisor_user_name,
      a.sales_manager_user_number,
      a.sales_manager_user_name,
      a.city_manager_user_number,
      a.city_manager_user_name,
      a.province_manager_user_number,
      a.province_manager_user_name,
      a.supervisor_position,
      a.sales_manager_position,
      a.city_manager_position, 
      a.province_manager_position ;

-- 应收周转


-----应收周转天数用期末城市 销售取含税计算
  select
    performance_region_name,
    performance_province_name ,
    performance_city_name ,
    province_manager_user_number,
    province_manager_user_name,
    province_manager_position,
    city_manager_user_number,
    city_manager_user_name,
    city_manager_position,
    sales_manager_user_number,
    sales_manager_user_name,
    sales_manager_position,
    supervisor_user_number,
    supervisor_user_name,
    supervisor_position,
    max(accounting_cnt) accounting_cnt,
	sum(sale_amt) sale_amt,
    sum(excluding_tax_sales) excluding_tax_sales,
    sum(qc_amt) qc_amt,
    sum(qm_amt) qm_amt,
    avg(qc_amt+qm_amt) receivable_amount,
    if(avg(qc_amt+qm_amt)=0 or coalesce(max(sale_amt),0)=0,0,max(accounting_cnt)/(coalesce(max(sale_amt),0)/avg(qc_amt+qm_amt))) as turnover_days
  from csx_analyse_tmp.csx_analyse_tmp_fl_quarterly_sap_detail a
  group by  performance_region_name,
        performance_province_name ,
        performance_city_name ,
        province_manager_user_number,
        province_manager_user_name,
        province_manager_position,
        city_manager_user_number,
        city_manager_user_name,
        city_manager_position,
        sales_manager_user_number,
        sales_manager_user_name,
        sales_manager_position,
        supervisor_user_number,
        supervisor_user_name,
        supervisor_position
 ;
		
--应收周转明细
-- select * from csx_analyse_tmp.csx_analyse_tmp_fl_quarterly_sap_detail
-- --应收周转明细
-- drop table csx_analyse_tmp.csx_analyse_tmp_fl_quarterly_sap_detail;
create table csx_analyse_tmp.csx_analyse_tmp_fl_quarterly_sap_detail as 
------------明细数据 周转 销售取含税计算
with temp_company_credit as (
  select
    distinct
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_code,
    a.credit_code,
    a.customer_name,
    a.business_attribute_code,
    a.business_attribute_name,
    a.company_code,
    a.status,
    a.is_history_compensate,
    c.province_manager_user_number,
    c.province_manager_user_name,
    c.province_manager_position,
    c.city_manager_user_number,
    c.city_manager_user_name,
    c.city_manager_position,
    c.sales_manager_user_number,
    c.sales_manager_user_name,
    c.sales_manager_position,
    c.supervisor_user_number,
    c.supervisor_user_name,
    c.supervisor_position,
    c.sale_number,
    c.sale_name
  from
      csx_dim.csx_dim_crm_customer_company_details a
  LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c on a.customer_code = c.customer_no
    and a.business_attribute_name = c.attribute_name
  where
    sdt = 'current' -- and status=1

),
tmp_sap_customer_credit_detail as 
(
   select
     b.performance_region_name,
     b.performance_province_name ,
     b.performance_city_name ,
     b.province_manager_user_number,
     b.province_manager_user_name,
     b.province_manager_position,
     b.city_manager_user_number,
     b.city_manager_user_name,
     b.city_manager_position,
     b.sales_manager_user_number,
     b.sales_manager_user_name,
     b.sales_manager_position,
     b.supervisor_user_number,
     b.supervisor_user_name,
     b.supervisor_position,
     b.sale_number,
     b.sale_name,
     a.customer_code,
     b.customer_name,   
     b.business_attribute_name,
     sum(if(sdt='20250930',receivable_amount,0)) as qc_amt,
     sum(if(sdt='20251231',receivable_amount,0)) as qm_amt --应收账款
   from
     -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
     -- 取SAP
     csx_dws.csx_dws_sap_customer_credit_settle_detail_di a
     left join temp_company_credit b on a.customer_code = b.customer_code
     and a.credit_code = b.credit_code
     and a.company_code = b.company_code -- csx_dws_sss_customer_invoice_bill_settle_stat_di
   where
   sdt in ('20250930','20251231')
    and b.business_attribute_code in ('2','5','10')
   group by
      b.performance_region_name,
     b.performance_province_name ,
     b.performance_city_name ,
     b.province_manager_user_number,
     b.province_manager_user_name,
     b.province_manager_position,
     b.city_manager_user_number,
     b.city_manager_user_name,
     b.city_manager_position,
     b.sales_manager_user_number,
     b.sales_manager_user_name,
     b.sales_manager_position,
     b.supervisor_user_number,
     b.supervisor_user_name,
     b.supervisor_position,
     b.sale_number,
     b.sale_name,
     a.customer_code,
     b.customer_name  ,
     b.business_attribute_name
    )
   
    select
         performance_region_name,
        performance_province_name ,
        performance_city_name ,
        province_manager_user_number,
        province_manager_user_name,
        province_manager_position,
        city_manager_user_number,
        city_manager_user_name,
        city_manager_position,
        sales_manager_user_number,
        sales_manager_user_name,
        sales_manager_position,
        supervisor_user_number,
        supervisor_user_name,
        supervisor_position,
        sale_number,
        sale_name,
        a.customer_code,
        a.customer_name,  
        DATEDIFF('2025-12-31','2025-09-30') as accounting_cnt,
        sum(b.sale_amt) sale_amt,
        sum(b.excluding_tax_sales) excluding_tax_sales,
        sum(a.qc_amt) qc_amt,
        sum(a.qm_amt) qm_amt
    from 
       tmp_sap_customer_credit_detail  a   
      LEFT join (
        select
          customer_code,
          CASE
        WHEN business_type_code in ('2', '10') then '福利'
        else business_type_name
      end new_business_type_name,
          sum(sale_amt) sale_amt,
          sum(sale_amt_no_tax) as excluding_tax_sales
        from
          csx_dws.csx_dws_sale_detail_di
        where
          sdt >= '20251001'
          and sdt <= '20251231'
          and (business_type_code in ('2', '6', '10') 
                or inventory_dc_code  in ('WD75', 'WD76', 'WD77', 'WD78', 'WD79', 'WD80', 'WD81')
              )
        group by
          customer_code,
           CASE
        WHEN business_type_code in ('2', '10') then '福利'
        else business_type_name   end 
      ) b on a.customer_code = b.customer_code and a.business_attribute_name=b.new_business_type_name
      group by  
        performance_region_name,
        performance_province_name ,
        performance_city_name ,
        province_manager_user_number,
        province_manager_user_name,
        province_manager_position,
        city_manager_user_number,
        city_manager_user_name,
        city_manager_position,
        sales_manager_user_number,
        sales_manager_user_name,
        sales_manager_position,
        supervisor_user_number,
        supervisor_user_name,
        supervisor_position,
        sale_number,
        sale_name,
        a.customer_code,
        a.customer_name
    having coalesce(sum(b.sale_amt),0)+ coalesce(sum(a.qc_amt),0) + coalesce(sum(a.qm_amt),0) <>0


-- 当期客户期初期末对外B端应收账款 统一取中台
-- 取SAP应收表 输出表：csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
-- 中台核销 输出表：csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
 select
         sdt,
         sdt,
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
		     customer_name,
         sum(receivable_amount)  receivable_amount -- 应收账款
       from 
         csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di   中台核销
      --  csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
		  -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20241231'  or sdt='20250331')  
         and channel_name  in ('大客户','项目供应商','虚拟销售员','业务代理','','其他') -- and province_name='福建省'
       group by  
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
         customer_name;



-- 当期客户期初期末对外B端应收账款 
-- 取SAP应收表 输出表：csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
-- Q2取中台
  select
         sdt,
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
		     customer_name,
         sum(receivable_amount)  receivable_amount -- 应收账款
       from 
        csx_dws.csx_dws_sap_subject_customer_settle_detail
      --  csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
		  -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20241231'  or sdt='20250331')  
         and channel_code  in ('1','7','9','13') -- and province_name='福建省'
       group by  sdt,
         channel_name,
         performance_province_name ,
         performance_city_name ,
         customer_code,
         customer_name;


-- 商机新客异常查找：
select
  a.*,
  b.min_sdt,
  b.max_sdt
from
  (
    select
      sdt,
      customer_code,
      business_number,
      cast(business_type_code as STRING) business_type_code,
      from_unixtime(
        unix_timestamp(business_sign_time, 'yyyy-MM-dd HH:mm:ss')
      ) business_sign_time,
      regexp_replace(substr(business_sign_time, 1, 10), '-', '') start_date,
      case
        when business_type_code = 6
        and other_needs_code = '1' then '餐卡'
        when business_type_code = 6
        and (
          other_needs_code <> '1'
          or other_needs_code is null
        ) then '非餐卡'
        else '其他'
      end as other_needs_code,
      business_stage
    from
      csx_dim.csx_dim_crm_business_info
    where
      sdt = 'current' --   business_stage = 5
      --  and to_date(business_sign_time )>='2024-04-01'
      and business_type_code in (1, 2, 6)
      and customer_code in ('211611')
  ) a
  left join (
    select
      customer_code,
      business_type_code,
      max(sdt) max_sdt,
      min(sdt) min_sdt
    from
      csx_dws.csx_dws_sale_detail_di
    where
      sdt >= '20240101'
    group by customer_code,
      business_type_code
  )b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code