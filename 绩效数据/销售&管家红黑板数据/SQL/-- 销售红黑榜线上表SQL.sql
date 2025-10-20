-- 销售红黑榜线上表SQL
调度最晚日期为10号，
0、销售员信息表：csx_analyse_fr_hr_red_black_sale_info  调度日期1-3号止
1、销售明细表：csx_analyse_fr_sales_red_black_sale_detail
2、销售员商机明细表：csx_analyse_fr_hr_sales_red_black_business_detail
3、销售员逾期明细表：csx_analyse_fr_hr_sales_red_black_over_detail
4、保证金明细表：csx_analyse_fr_hr_red_black_break_deposit_overdue
5、销售经理应收周转明细表：csx_analyse_fr_hr_sales_red_black_receiveable_turnover_detail
6、销售员评分结果表： csx_analyse_fr_hr_red_balck_sales_score_result_mf
7、销售经理评分结果表： csx_analyse_fr_hr_red_balck_sales_manager_score_result_mf
8、新客信息： csx_analyse.csx_analyse_hr_red_black_new_customer_info_mf   按照福利取当月的履约金额，日配、BBC取当月1-20号，20号后到下月20号。
9、销售员目标表：csx_analyse.csx_analyse_source_write_hr_sales_red_black_target_mf
10、销售经理目标表：csx_analyse.csx_analyse_source_write_hr_sales_manager_red_black_target_mf
-- ******************************************************************** 
-- @功能描述：红黑榜销售员信息表
-- @创建者： 彭承华 
-- @创建者日期：2025-02-05 15:24:25 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


with position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
leader_info as 
  (select a.*,
    c.name as leader_user_position_name,
    b.name as leader_source_user_position_name 
    from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id,sdt ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt =regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    left join position_dic b on user_position=b.code
    left join position_dic c on a.user_position_type=c.code
    where rank=1
  )
 
 insert overwrite table csx_analyse.csx_analyse_fr_hr_red_black_sale_info partition(smt)
select 
  a.sdt,
  a.user_id,
  a.user_number,
  a.user_name,
  coalesce(a.user_position,source_user_position)user_position ,
  replace(c.name,'（旧）','') user_position_name,
  d.name as sub_position_name,
  a.begin_date,
  a.source_user_position,
  a.leader_user_id,
  a.new_leader_user_id,
  f.user_number as new_leader_user_number,
  f.user_name as new_leader_user_name,
  a.province_id,
  a.province_name,
  a.city_code,
  a.city_name,
  b.user_number leader_user_number,
  b.user_name leader_user_name,
  b.user_position_type leader_user_position,
  b.leader_user_position_name,
  b.user_position leader_source_user_position,
  b.leader_source_user_position_name,
  current_timestamp(),
  substr(a.sdt,1,6) smt
  
from 
 (
select
  a.sdt,
  user_id,
  user_number,
  user_name,
  coalesce(user_position,source_user_position)  user_position,
  begin_date,
  source_user_position,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'), user_id, leader_user_id ) leader_user_id,
   case when a.province_id='6' then '1000000565219'
      when a.city_code='320500' then '1000000567463'
      when a.province_id='26' then '1000000426003'
      when a.city_code='440300' then '1000000426252'
      when a.city_code='340100' then '1000000596953'
      else ''
  end new_leader_user_id,
  province_id,
  province_name,
  city_code,
  city_name
  from 
     csx_dim.csx_dim_uc_user a 
  left  join 
    (select distinct
        employee_name,
        employee_code,
        begin_date,
        record_type_name,
        sdt
    from csx_dim.csx_dim_basic_employee 
        where sdt = regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        and card_type=0 
      --  and record_type_code	!=4
    )b on a.user_number=b.employee_code 
    -- and a.sdt=b.sdt
    where
    a.sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
  --  and status=0 
 -- and (user_position like 'SALES%'
-- and user_name in ('江苏B','许佳惠')
  )a 
 left join leader_info  b on a.leader_user_id=b.user_id 
--  and a.sdt=b.sdt
 left join position_dic c on a.user_position=c.code
 left join position_dic d on a.source_user_position=d.code
 left join leader_info f on a.new_leader_user_id=f.user_id  
--  and a.sdt=f.sdt

 ;
 
-- ******************************************************************** 
-- @功能描述：红黑榜-销售员&经理销售明细
-- @创建者： 彭承华 
-- @创建者日期：2025-01-27 16:21:13 
-- @修改者日期：
-- @修改人：
-- @修改内容：csx_analyse_fr_sales_red_black_sale_detail
-- ******************************************************************** 

-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;
-- 1.0 -- 销售明细
 
with 
    sale as 
    (select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        -- sales_user_position,
        case when a.customer_code in ('245127') then  '81001273' 
            when a.customer_code in ('252180','252393','252460') then '80946479'
            else sales_user_number end new_sales_user_number,
        case when a.customer_code in ('245127') then  '徐培召' 
            when a.customer_code in ('252180','252393','252460') then '於佳'
            else sales_user_name end new_sales_user_name,    
        if(b.customer_code is not null, 1, 0) as new_customer_flag,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    left join 
    -- 关联商机新客
      (select a.customer_no customer_code,
              business_type_code
        from
        (
        select customer_no,business_type_code from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di 
        where smonth = substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        union all
        select customer_no,business_type_code from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
        where smonth =substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')
        and sdt <= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        and (a.business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC
            or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209',
                                      '80946479','81102471','81254457','81119082','81149084',
                                      '81103064','81029025','81013168','81149084','81103064','81254457')
               and a.business_type_code =4)
            )
        and a.customer_code not in ('234036','224656','247525','243799','244172','237768')
        and sales_user_number not in ('81208614') -- 202502 剔除 汪平81206921
        and shipper_code='YHCSX'
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        -- sales_user_position,
        case when a.customer_code in ('245127') then  '81001273' 
            when a.customer_code in ('252180','252393','252460') then '80946479'
            else sales_user_number end ,
        case when a.customer_code in ('245127') then  '徐培召' 
            when a.customer_code in ('252180','252393','252460')then  '於佳'
            else sales_user_name end,
            
        if(b.customer_code is not null, 1, 0) 
    )
insert overwrite table csx_analyse.csx_analyse_fr_sales_red_black_sale_detail partition(smt)
    select  sale_month,  
        performance_region_name,
        performance_province_name,
        performance_city_name,
        a.business_type_name,
        a.customer_code,
        customer_name,
        new_sales_user_number,
        new_sales_user_name,
        -- sales_user_position,
        user_position_name,
        sub_position_name,
        begin_date,
        if(customer_code in ('235479'),'81180572',leader_user_number) leader_user_number,
        if(customer_code in ('235479'),'余杰', leader_user_name) leader_user_name,
        if(customer_code in ('235479'),'销售经理', leader_source_user_position_name) leader_source_user_position_name,
        new_leader_user_number,
        new_leader_user_name,
        new_customer_flag,
        sale_amt,
        profit,
        profit/sale_amt as profit_rate,
        current_timestamp,
        sale_month as smt
    from sale a 
    left join 
    (select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    ) b on a.new_sales_user_number=b.user_number
 ;
 

-- ******************************************************************** 
-- @功能描述：红黑榜销售经理应收周转
-- @创建者： 彭承华 
-- @创建者日期：2025-02-13 17:56:13 
-- @修改者日期：
-- @修改人：
-- @修改内容：日期参数为当前日期取上月末日期
-- ******************************************************************** 

-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;

-- 应收周转天数用期末城市 销售取含税计算
with tmp_receiveable_turnover_detail  as (
select
    c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name,
  DATEDIFF(last_day(add_months('${sdt_yes_date}',-1)),date_sub(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),1))as accounting_cnt,
  coalesce(sum(sale_amt),0) sale_amt,
  coalesce(sum(excluding_tax_sales),0) excluding_tax_sales,
  sum(qm_receivable_amount) qm_receivable_amount,
  sum(qc_receivable_amount) qc_receivable_amount,
  sum(qm_receivable_amount+qc_receivable_amount)/2 receivable_amount,
  if(sum(qm_receivable_amount+qc_receivable_amount)/2 =0 or coalesce(sum(sale_amt),0)=0,0,
        DATEDIFF(last_day(add_months('${sdt_yes_date}',-1)),date_sub(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),1))/(coalesce(sum(sale_amt),0)/(sum(qm_receivable_amount+qc_receivable_amount)/2 ))) as turnover_days
from 
( select
    c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name,
    a.customer_code,
    c.customer_name,
    sum(b.excluding_tax_sales) excluding_tax_sales,
    sum(sale_amt) sale_amt,
    sum(a.qm_receivable_amount) qm_receivable_amount,
    sum(a.qc_receivable_amount) qc_receivable_amount
  from 
   ( 
  	 select
         channel_name,
         customer_code,
         sum(if(sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),receivable_amount,0))  qm_receivable_amount,
         sum(if(sdt=regexp_replace(date_sub(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),1),'-',''),receivable_amount,0))  qc_receivable_amount
         --应收账款
       from 
         -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
	      csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
       where sdt in (regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') ,regexp_replace(date_sub(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),1),'-','') )  
    --   select regexp_replace(date_sub(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),1),'-',''),regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        group by  
         channel_name ,
         customer_code
  	   )a
  LEFT join (
  	select 			
          customer_code,
          sum(sale_amt) sale_amt,
  	      sum(sale_amt_no_tax) as excluding_tax_sales
      from   csx_dws.csx_dws_sale_detail_di
           where sdt >=regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')  and sdt <=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
  		      and  channel_code !='2'
  		group by customer_code
  		)b on a.customer_code=b.customer_code 
  LEFT join
          (select customer_code,
                customer_name,
                channel_code,
                channel_name,
                m.sales_user_id,
                m.sales_user_number,
                m.sales_user_name,
                p.user_position_name as sales_user_position,
                performance_region_code,
                performance_region_name,
                performance_province_code,
                performance_province_name,
                performance_city_code,
                performance_city_name,
                p.leader_user_number,
                p.leader_user_name,
                p.leader_user_position,
                p.leader_source_user_position_name leader_source_user_position,
                p.new_leader_user_number,
                p.new_leader_user_name
            from  csx_dim.csx_dim_crm_customer_info m 
            left join
            (select *
                from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
                    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
                    and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
            ) p on m.sales_user_number=p.user_number
              where m.sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
                and (dev_source_code !=3 
                    or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209',
                                      '80946479','81102471','81254457','81119082','81149084',
                                      '81103064','81029025','81013168','81149084','81103064','81254457') 
                    and dev_source_code=3)
                    or customer_code='239912'
                    )
                and sales_user_number not in ('81208614' )  -- 202502 剔除汪平 81206921
                and channel_code !='2'
           ) c on a.customer_code=c.customer_code 
 where c.customer_code is not null 
  and a.customer_code not in ('118808',
                              '118856',
                              '121248',
                              '122093',
                              '121259',
                              '121274',
                              '106241',
                              '121286',
                              '121305',
                              '115396',
                              '121244',
                              '116401',
                              '234036','224656','247525','243799','244172','237768'
                              )
  group by  c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name,
    a.customer_code,
    c.customer_name
 )c	
group by  performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name
)
insert overwrite table  csx_analyse.csx_analyse_fr_hr_sales_red_black_receiveable_turnover_detail partition(smt)
select
    substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) sale_months,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    -- sales_user_id,
    sales_user_number,
    sales_user_name,
    sales_user_position,
    leader_user_number,
    leader_user_name,
    leader_user_position,
    leader_source_user_position,
    new_leader_user_number,
    new_leader_user_name,
    accounting_cnt,
    sale_amt,
    excluding_tax_sales,
    qm_receivable_amount,
    qc_receivable_amount,
    receivable_amount,
    turnover_days,
    current_timestamp(),
    substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) smt
from tmp_receiveable_turnover_detail
;

-- ******************************************************************** 
-- @功能描述：销售经理评分结果表
-- @创建者： 彭承华 
-- @创建者日期：2025-02-14 16:43:11 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


   
-- 输出结果集
-- select last_day(add_months('${sdt_yes_date}',-1)),date_sub(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),1),regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-4)),'MM'),'-','')

with tmp_sales_manager_sale as 
(select sale_month,
      performance_region_name,
      sales_manager_number,
      new_leader_user_number,
      new_leader_user_name,
      sum(if (new_sales_flag=1 and begin_date is not null ,1,0)) as sales_cnt,
      sum(sale_amt) sale_amt,
      sum(profit) profit,
      sum(new_customer_sale_amt) new_customer_sale_amt,
      sum(new_customer_profit) new_customer_profit
from 
(select sale_month,
      performance_region_name,
      sales_user_number as sales_user_number,
      begin_date,
    --   sales_user_position,
      user_position_name,
      sub_position_name,
      if(substr(begin_date,1,6)>=regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-4)),'MM'),'-',''),0,1 ) new_sales_flag,
      leader_user_number sales_manager_number,
      leader_user_name,
      leader_source_user_position_name leader_user_position_name,
      new_leader_user_number,
      new_leader_user_name,
      sum(sale_amt) sale_amt,
      sum(profit)profit,
      sum(if(new_customer_flag=1,sale_amt,0)) as new_customer_sale_amt,
      sum(if(new_customer_flag=1,profit,0)) as new_customer_profit
 from    
 csx_analyse.csx_analyse_fr_sales_red_black_sale_detail
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
 -- where sales_user_position in ('SALES_MANAGER','SALES','SALES_CITY_MANAGER')
  group by sale_month,
      performance_region_name,
      sales_user_number,
      begin_date,
    --   sales_user_position,
      user_position_name,
      sub_position_name,
      if(substr(begin_date,1,6)>=regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-4)),'MM'),'-',''),0,1 ) ,
      leader_user_number ,
      leader_user_name,
      leader_source_user_position_name ,
      new_leader_user_number,
      new_leader_user_name
      )a 
      group by sale_month,
      performance_region_name,
      sales_manager_number,
      new_leader_user_number,
      new_leader_user_name
),
tmp_sales_sale as  (
select  sale_month,
    performance_region_name,
    sales_user_number,
    b.user_name sales_user_name,
    b.user_position_name user_position	,
    b.sub_position_name,
    begin_date,
    max(sales_team_number) sales_team_number,
    sum(sales_user_base_profit)  sales_user_base_profit,
    sum(plan_sales_amt)   plan_sales_amt,
    sum(plan_profit)   plan_profit,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    coalesce(sum(new_customer_sale_amt),0) as new_customer_sale_amt,
    sum(new_customer_profit) as new_customer_profit,
    sum(sales_cnt) as sales_cnt,
    sum(trunover_sale_amt )trunover_sale_amt,
    sum(qc_receivable_amount)qc_receivable_amount,
    sum(qm_receivable_amount) qm_receivable_amount,
    if(sum(qm_receivable_amount+qc_receivable_amount)/2 =0 or coalesce(sum(trunover_sale_amt),0)=0,0,
        DATEDIFF(last_day(add_months('${sdt_yes_date}',-1)),date_sub(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),1))/(coalesce(sum(trunover_sale_amt),0)/(sum(qm_receivable_amount+qc_receivable_amount)/2 ))) as turnover_days,
    max(lave_customer_cn) lave_customer_cn,
    max(lave_write_off_amount) as lave_write_off_amount
from 
(-- 目标表
    select sale_month ,
    if(performance_region_name like '%大区',performance_region_name,concat(performance_region_name,'大区')) as performance_region_name,
    sales_user_number,
    cast(sales_team_number as decimal(26,6)) sales_team_number,
    cast(sales_user_base_profit as decimal(26,6)) sales_user_base_profit,
    cast(plan_sales_amt as decimal(26,6)) plan_sales_amt,
    cast(plan_profit as decimal(26,6)) plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    0 as sales_cnt,
    0 trunover_sale_amt,
    0 qm_receivable_amount,
    0 qc_receivable_amount,
    0 turnover_days
from 
     csx_analyse.csx_analyse_source_write_hr_sales_manager_red_black_target_mf a 
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) 
    and sale_month=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) 
union all 
-- 上一级是经理
select sale_month,
    performance_region_name,
    sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    (sale_amt) sale_amt,
    (profit )profit,
    new_customer_sale_amt,
    new_customer_profit,
    sales_cnt,
    0 trunover_sale_amt,
    0 qm_receivable_amount,
    0 qc_receivable_amount,
    0 turnover_days
from 
    tmp_sales_manager_sale    
  where  coalesce(new_leader_user_number,'')!= sales_manager_number

 union all
 
 select sale_month,
    performance_region_name,
    leader_user_number sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 new_customer_sale_amt,
    0 new_customer_profit,
    0 sales_cnt,
    sum(sale_amt) as trunover_sale_amt,
    sum(qm_receivable_amount) qm_receivable_amount,
    sum(qc_receivable_amount) qc_receivable_amount,
    0 turnover_days
from  csx_analyse.csx_analyse_fr_hr_sales_red_black_receiveable_turnover_detail
      where  smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) 
        and leader_user_number != coalesce(new_leader_user_number,'')
      group by  performance_region_name,
    leader_user_number ,
    sale_month
 union all 
  -- 单独处理城市经理
select sale_month,
    performance_region_name,
    new_leader_user_number as sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    (sale_amt) sale_amt,
    (profit )profit,
    new_customer_sale_amt,
    new_customer_profit,
    sales_cnt,
    0 trunover_sale_amt,
    0 qm_receivable_amount,
    0 qc_receivable_amount,
    0 turnover_days
from 
    tmp_sales_manager_sale    
  where new_leader_user_number is not null 
union all 
   select sale_month,
    performance_region_name,
    new_leader_user_number as  sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 new_customer_sale_amt,
    0 new_customer_profit,
    0 sales_cnt,
    sum(sale_amt) as trunover_sale_amt,
    sum(qm_receivable_amount) qm_receivable_amount,
    sum(qc_receivable_amount) qc_receivable_amount,
    0 as turnover_days
from  csx_analyse.csx_analyse_fr_hr_sales_red_black_receiveable_turnover_detail
      where  smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
      and new_leader_user_number is not null 
      group by performance_region_name,
      new_leader_user_number,
      sale_month
)a 
left join 
(select  
  leader_user_number,
  leader_user_name,
  sum(lave_write_off_amount)lave_write_off_amount,
  count(distinct customer_code) as lave_customer_cn  
from
   csx_analyse.csx_analyse_fr_hr_red_black_break_deposit_overdue a
 where leader_user_number != coalesce(new_leader_user_number,'') 
    and is_oveder_flag='是'
    and a.smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
 group by leader_user_number,
        leader_user_name

union all   
select  
  new_leader_user_number leader_user_number,
  new_leader_user_name leader_user_name,

  sum(lave_write_off_amount)lave_write_off_amount,
  count(distinct customer_code) as lave_customer_cn  
from
   csx_analyse.csx_analyse_fr_hr_red_black_break_deposit_overdue a
  where new_leader_user_number is not null
      and a.smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)

    and is_oveder_flag='是'
    group by  
  new_leader_user_number,
  new_leader_user_name
  
  ) c on a.sales_user_number=c.leader_user_number
left join   
   (select *
                from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
                    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
                    and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
            ) b on a.sales_user_number=b.user_number	

group by sale_month,
    performance_region_name,
    sales_user_number,
    b.user_name,
    b.user_position_name	,
    b.sub_position_name,
    begin_date
),
tmp_sales_manager_cnt as 
(select count(sales_user_number) as cnt ,
    max(sale_rank) max_sale_rank,
    max(profit_rank) max_profit_rank,
    max(new_cust_rank) max_cust_rank,
    max(turnover_rank) max_turnover_rank
from 
(select sales_user_number,
    dense_rank()over( order by sale_amt/10000/plan_sales_amt  desc ) sale_rank,
    dense_rank()over( order by profit/10000/plan_profit  desc ) profit_rank,
    dense_rank()over( order by nvl(new_customer_sale_amt/10000/sales_team_number,0) desc)  as new_cust_rank,
    dense_rank()over( order by turnover_days asc)  as turnover_rank
from tmp_sales_sale
 where plan_sales_amt<>0
-- group by sales_user_number
)a 
),
tmp_score as  (
select sale_month,
    performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    sales_team_number,
    sales_user_base_profit,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    sale_rank,
    sale_weight,
    CASE  WHEN sale_rank = 1 THEN 30
        when sale_rank=max_sale_rank then 0 
        ELSE 30 - (sale_rank - 1) *(30/(max_sale_rank-1) )
     END  AS sale_score,
    plan_profit,
    profit,
    profit_achieve_rate,
    profit_rank,
    profit_weight,
    CASE  WHEN profit_rank = 1 THEN 30
        when profit_rank=max_profit_rank then 0 
        ELSE 30 - (profit_rank - 1) *(30/(max_profit_rank-1) )
     END  AS profit_score,
    new_customer_sale_amt,
    sales_cnt,
    avg_new_customer_amt,
    new_cust_rank,
    new_customer_weight,
    CASE  WHEN new_cust_rank = 1 THEN 20
        when new_cust_rank=max_cust_rank then 0 
        ELSE 20 - (new_cust_rank - 1) *(20/(max_cust_rank-1) )
     END  AS new_cust_score,
     turnover_days,
     turnover_rank,
     turnover_weight,
     CASE  WHEN turnover_rank = 1 THEN 20
        when turnover_rank=max_turnover_rank then 0 
        ELSE 20 - (turnover_rank - 1) *(20/(max_turnover_rank-1) )
     END  AS turnover_score,
    trunover_sale_amt,
    qc_receivable_amount,
    qm_receivable_amount,
    lave_customer_cn,
    lave_write_off_amount
  from  (
select sale_month,
    performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    (sales_team_number) sales_team_number,
    (sales_user_base_profit)  sales_user_base_profit,
    (plan_sales_amt)   plan_sales_amt,
    (sale_amt)/10000 sale_amt,
    if(plan_sales_amt=0,0,sale_amt/10000 /plan_sales_amt) as sale_achieve_rate,
    dense_rank()over( order by sale_amt/10000/plan_sales_amt  desc ) sale_rank,
    0.3 as sale_weight,
    (plan_profit)   plan_profit,
    (profit)/10000 profit,
    if(plan_profit=0,0, profit/10000/plan_profit)  profit_achieve_rate,
    dense_rank()over(order by profit/10000/plan_profit  desc ) profit_rank,
    0.3 as profit_weight,
    coalesce(new_customer_sale_amt,0)/10000 as new_customer_sale_amt,
    (sales_team_number) as sales_cnt,
    nvl(new_customer_sale_amt/10000/sales_team_number,0) as avg_new_customer_amt,
    dense_rank()over( order by nvl(new_customer_sale_amt/10000/sales_team_number,0) desc)  as new_cust_rank,
    0.2 as new_customer_weight,
    (turnover_days) turnover_days,
    dense_rank()over( order by turnover_days asc)  as turnover_rank,
    0.2 as turnover_weight,
    (trunover_sale_amt )/10000 trunover_sale_amt,
    (qc_receivable_amount)/10000  qc_receivable_amount,
    (qm_receivable_amount)/10000 qm_receivable_amount,
    cnt,
    max_turnover_rank,
    max_cust_rank,
    max_profit_rank,
    max_sale_rank,
    lave_customer_cn,
    lave_write_off_amount
from tmp_sales_sale 
left join tmp_sales_manager_cnt on 1=1 
where plan_sales_amt<>0
)a
)
-- select * from sales_manager_cnt
insert overwrite table csx_analyse.csx_analyse_fr_hr_red_balck_sales_manager_score_result_mf partition(smt)
select sale_month,
    performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    -- sales_team_number,
    -- sales_user_base_profit,
    case when (total_rank/max(total_rank)over()<=0.10)  then '红榜'
        when (last_total_rank/max(last_total_rank)over()<=0.10) then '黑榜'
        else '' end  as top_rank,
    total_rank,
    last_total_rank,
    total_score,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    sale_rank,
    sale_weight,
    sale_score,
    plan_profit,
    profit,
    profit_achieve_rate,
    profit_rank,
    profit_weight,
    profit_score,
    new_customer_sale_amt,
    sales_cnt,
    avg_new_customer_amt,
    new_cust_rank,
    new_customer_weight,
    new_cust_score,
    turnover_days,
    turnover_rank,
    turnover_weight,
    turnover_score,
    trunover_sale_amt,
    qc_receivable_amount,
    qm_receivable_amount,
    coalesce(lave_customer_cn,'')lave_customer_cn,
    coalesce(lave_score,'')lave_score,
    coalesce(lave_write_off_amount,'')lave_write_off_amount,
    current_timestamp() update_time,
    -- last_total_rank,
    sale_month
  from 
(select
    sale_month,
    performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    -- sales_team_number,
    sales_user_base_profit,
    dense_rank()over( order by (sale_score+profit_score+new_cust_score+turnover_score+if(lave_customer_cn>0,turnover_score*0.2*-1,0)) desc  ) as total_rank,
    dense_rank()over( order by (sale_score+profit_score+new_cust_score+turnover_score+if(lave_customer_cn>0,turnover_score*0.2*-1,0)) asc  ) as last_total_rank,
    (sale_score+profit_score+new_cust_score+turnover_score+if(lave_customer_cn>0,turnover_score*0.2*-1,0)) total_score,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    sale_rank,
    sale_weight,
    sale_score,
    plan_profit,
    profit,
    profit_achieve_rate,
    profit_rank,
    profit_weight,
    profit_score,
    new_customer_sale_amt,
    sales_team_number as sales_cnt,
    avg_new_customer_amt,
    new_cust_rank,
    new_customer_weight,
    new_cust_score,
    turnover_days,
    turnover_rank,
    turnover_weight,
    turnover_score,
    trunover_sale_amt,
    qc_receivable_amount,
    qm_receivable_amount,
    lave_customer_cn,
    lave_write_off_amount,
    if(lave_customer_cn>0,turnover_score*0.2*-1,0) lave_score
  from  tmp_score a 
) a 
order by total_rank asc ;


-- ******************************************************************** 
-- @功能描述：销售员评分结果表
-- @创建者： 彭承华 
-- @创建者日期：2025-02-14 12:14:48 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


-- select * from csx_analyse_tmp.csx_analyse_tmp_hr_full_total
drop table csx_analyse_tmp.csx_analyse_tmp_hr_full_total ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_full_total as 
with tmp_sales_sale as 
(
select sale_month,
    performance_region_name,
    sales_user_number,
    b.user_name sales_user_name,
    b.user_position_name user_position	,
    b.sub_position_name,
    begin_date,
    leader_user_name,
    sum(sales_user_base_profit)  sales_user_base_profit,
    sum(plan_sales_amt)   plan_sales_amt,
    sum(plan_profit)   plan_profit,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(new_customer_sale_amt) as new_customer_sale_amt,
    sum(new_customer_profit) as new_customer_profit,
    sum(overdue_amount) overdue_amount,
    sum(receivable_amount) receivable_amount,
    sum(middle_customer_cn) as middle_customer_cn,
    sum(middle_contract_amt) middle_contract_amt,
    sum(end_customer_cn) as end_customer_cn,
    sum(end_contract_amt) end_contract_amt
from 
(-- 目标表
  select a.smt as sale_month,
    if(performance_region_name like '%大区',performance_region_name,concat(performance_region_name,'大区')) as performance_region_name,
    sales_user_number,
    cast(sales_user_base_profit as decimal(26,6)) sales_user_base_profit,
    cast(plan_sales_amt as decimal(26,6)) plan_sales_amt,
    cast(plan_profit as decimal(26,6)) plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    0 overdue_amount,
    0 receivable_amount,
    0 as middle_customer_cn,
    0 middle_contract_amt,
    0 as end_customer_cn,
    0 end_contract_amt
from 
   csx_analyse.csx_analyse_source_write_hr_sales_red_black_target_mf a 
   left join 
   (select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') ) b on a.sales_user_number=b.user_number	
where a.smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) 
    and a.sale_month=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
union all 

select sale_month,
    performance_region_name,
    sales_user_number as sales_user_number,

    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    sum(sale_amt) sale_amt,
    sum(profit )profit,
    sum(if(new_customer_flag=1,sale_amt,0)) as new_customer_sale_amt,
    sum(if(new_customer_flag=1,profit,0)) as new_customer_profit,
    0 overdue_amount,
    0 receivable_amount,
    0 as middle_customer_cn,
    0 middle_contract_amt,
    0 as end_customer_cn,
    0 end_contract_amt
from  csx_analyse.csx_analyse_fr_sales_red_black_sale_detail
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
 -- where sales_user_number='81214954'
group by sale_month,
    performance_region_name,
    sales_user_number
    
union all  
select  sale_month,
    performance_region_name,
    sales_user_number as sales_user_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    sum(overdue_amount) overdue_amount,
    sum(receivable_amount) receivable_amount,
    0 as middle_customer_cn,
    0 middle_contract_amt,
    0 as end_customer_cn,
    0 end_contract_amt
from csx_analyse.csx_analyse_fr_hr_sales_red_black_over_detail
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)

    group by   sale_month,
    performance_region_name,
    sales_user_number
    
union all 

select sale_month,
    performance_region_name,
    owner_user_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    0 overdue_amount,
    0 receivable_amount,
    middle_customer_cn,
    middle_contract_amt,
    end_customer_cn,
    end_contract_amt
from (
select sale_month,
    performance_region_name,
    owner_user_number,
    sum(if(days_note='上半月',1,0)) as middle_customer_cn,
    sum(if(days_note='上半月',tran_contract_amount,0)) middle_contract_amt,
    count(business_number) as end_customer_cn,
    sum(tran_contract_amount) end_contract_amt
from   csx_analyse.csx_analyse_fr_hr_sales_red_black_business_detail a 
where rn=1
   and  a.smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
   and ((sale_after_date<regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','') and business_type_code=1) or business_type_code!=1 or sale_after_date is null )
group by 
    performance_region_name,
    owner_user_number,
    sale_month
)a 
)a 
left join   
   (select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') ) b on a.sales_user_number=b.user_number	
group by sale_month,
      performance_region_name,
    sales_user_number,
    b.user_name,
    b.user_position_name	,
    b.sub_position_name,
    begin_date,
    leader_user_name
)
select a.*,performance_province_name,performance_province_code 
from tmp_sales_sale a 
left join 
(select distinct performance_province_name,performance_province_code,sales_user_number
    from   csx_dim.csx_dim_crm_customer_info 
    where sdt='current' ) b on a.sales_user_number=b.sales_user_number
;



-- 计算得分结果集
with tmp_performance as 
(select sale_month smt
,performance_region_name
,sales_user_number
,sales_user_name
,user_position
,sub_position_name
,begin_date
,leader_user_name
,sales_user_base_profit
,plan_sales_amt
,sale_amt/10000 sale_amt
,sale_amt/10000/plan_sales_amt as sale_achieve_rate
,dense_rank()over(partition by performance_region_name order by sale_amt/10000/plan_sales_amt  desc ) sale_rank
,0.2 as sale_weight
,plan_profit
,profit/10000 profit
,profit/10000/plan_profit as profit_achieve_rate
,dense_rank()over(partition by performance_region_name order by profit/10000/plan_profit  desc ) profit_rank
,0.2 as profit_weight
,new_customer_sale_amt/10000  new_customer_sale_amt
,dense_rank()over(partition by performance_region_name order by new_customer_sale_amt desc)  as new_cust_rank
,0.2 as new_customer_weight
,new_customer_profit/10000 new_customer_profit
,overdue_amount/10000  overdue_amount
,receivable_amount/10000 receivable_amount
,coalesce(overdue_amount/receivable_amount,0) as overdue_rate
,dense_rank()over(partition by performance_region_name order by coalesce(overdue_amount/receivable_amount,0) asc ) as overdue_rank
,0.2 as overdue_weight
,middle_customer_cn
,end_customer_cn
,(coalesce(middle_customer_cn,0)+coalesce(end_customer_cn,0))/2 avg_customer_cn
,dense_rank()over(partition by performance_region_name order by (coalesce(middle_customer_cn,0)+coalesce(end_customer_cn,0))/2 desc ) business_cnt_rnk
,0.1 as business_cnt_weight
,middle_contract_amt
,end_contract_amt
,(coalesce(middle_contract_amt,0)+coalesce(end_contract_amt,0))/2 avg_customer_contract_amt
,dense_rank()over(partition by performance_region_name order by (coalesce(middle_contract_amt,0)+coalesce(end_contract_amt,0))/2 desc ) business_amt_rnk
,0.1 as business_amt_weight
from  csx_analyse_tmp.csx_analyse_tmp_hr_full_total
where plan_sales_amt<>0
) ,
tmp_max_rnk as 
(
select smt,
  performance_region_name,
  max(sale_rank) max_sale_rank,
  max(profit_rank) max_profit_rank,
  max(new_cust_rank )max_new_cust_rank,
  max(overdue_rank) max_overdue_rank,
  max(business_cnt_rnk) max_business_cnt,
  max(business_amt_rnk) max_business_amt
  from tmp_performance
  group by smt,
  performance_region_name
)  
,
tmp_score as (
select 
a.smt
,a.performance_region_name
,sales_user_number
,sales_user_name
,user_position
,sub_position_name
,begin_date
,leader_user_name
--,sales_user_base_profit
,plan_sales_amt
,sale_amt
,sale_achieve_rate
,sale_rank
,sale_weight
,CASE
    WHEN sale_rank = 1 THEN 20
    when sale_rank=max_sale_rank then 0 
    ELSE 20 - (sale_rank - 1) *(20/(max_sale_rank-1) )
  END  AS sale_score
,plan_profit
,profit
,profit_achieve_rate
,profit_rank
,profit_weight
,CASE
    WHEN profit_rank = 1 THEN 20
    when profit_rank=max_profit_rank then 0 
    ELSE 20 - (profit_rank - 1) *(20/(max_profit_rank-1) )
  END  AS profit_score
,new_customer_sale_amt
,new_cust_rank
,new_customer_weight
,CASE
    WHEN new_cust_rank = 1 THEN 20
    when new_cust_rank=max_new_cust_rank then 0 
    ELSE 20 - (new_cust_rank - 1) *(20/(max_new_cust_rank-1) )
  END  AS new_cust_score
,new_customer_profit
,overdue_amount
,receivable_amount
,overdue_rate
,overdue_rank
,overdue_weight
,CASE
    WHEN overdue_rank = 1 THEN 20
    when overdue_rank=max_overdue_rank then 0 
    ELSE 20 - (overdue_rank - 1) *(20/(max_overdue_rank-1) )
  END  AS overdue_score
,middle_customer_cn
,end_customer_cn
,avg_customer_cn
,business_cnt_rnk
,business_cnt_weight
,CASE
    WHEN business_cnt_rnk = 1 THEN 10
    when business_cnt_rnk=max_business_cnt then 0 
    ELSE 10 - (business_cnt_rnk - 1) *(10/(max_business_cnt-1) )
  END  AS business_cnt_score
,middle_contract_amt
,end_contract_amt
,avg_customer_contract_amt
,business_amt_rnk
,business_amt_weight
,CASE
    WHEN business_amt_rnk = 1 THEN 10
    when business_amt_rnk=max_business_amt then 0 
    ELSE 10 - (business_amt_rnk - 1) *(10/(max_business_amt-1) )
  END  AS business_amt_score
from tmp_performance a
left join   tmp_max_rnk c on a.smt=c.smt and a.performance_region_name=c.performance_region_name
) 
insert overwrite table csx_analyse.csx_analyse_fr_hr_red_balck_sales_score_result_mf partition(smt)
select a.smt
,a.performance_region_name
,sales_user_number
,sales_user_name
,user_position
,sub_position_name
,begin_date
,leader_user_name
--,sales_user_base_profit
,case when (total_rank/max(total_rank)over( partition by performance_region_name )<0.1) then '红榜'
        when (low_rank/max(low_rank)over(partition by performance_region_name)<0.1) then '黑榜'
        else '' end  as top_rank
,total_rank
,total_score
,plan_sales_amt
,sale_amt
,sale_achieve_rate
,sale_rank
,sale_weight
,sale_score
,plan_profit
,profit
,profit_achieve_rate
,profit_rank
,profit_weight
,profit_score
,new_customer_sale_amt
,new_cust_rank
,new_customer_weight
,new_cust_score
,new_customer_profit
,overdue_amount
,receivable_amount
,overdue_rate
,overdue_rank
,overdue_weight
,overdue_score
,middle_customer_cn
,end_customer_cn
,avg_customer_cn
,business_cnt_rnk
,business_cnt_weight
,business_cnt_score
,middle_contract_amt
,end_contract_amt
,avg_customer_contract_amt
,business_amt_rnk
,business_amt_weight
,business_amt_score
,lave_customer_cn
,lave_score
,current_timestamp() update_time
,smt
from 

(select a.smt
,a.performance_region_name
,sales_user_number
,sales_user_name
,user_position
,a.sub_position_name
,begin_date
,leader_user_name
--,sales_user_base_profit

,dense_rank()over(partition by performance_region_name order by (sale_score+profit_score+new_cust_score+overdue_score+business_cnt_score+business_amt_score+if(lave_customer_cn>0,overdue_score*0.2*-1,0) ) desc ) as total_rank
,dense_rank()over(partition by performance_region_name order by (sale_score+profit_score+new_cust_score+overdue_score+business_cnt_score+business_amt_score+if(lave_customer_cn>0,overdue_score*0.2*-1,0) ) asc  ) as low_rank
,(sale_score+profit_score+new_cust_score+overdue_score+business_cnt_score+business_amt_score) as total_score
,plan_sales_amt
,sale_amt
,sale_achieve_rate
,sale_rank
,sale_weight
,sale_score
,plan_profit
,profit
,profit_achieve_rate
,profit_rank
,profit_weight
,profit_score
,new_customer_sale_amt
,new_cust_rank
,new_customer_weight
,new_cust_score
,new_customer_profit
,overdue_amount
,receivable_amount
,overdue_rate
,overdue_rank
,overdue_weight
,overdue_score
,middle_customer_cn
,end_customer_cn
,avg_customer_cn
,business_cnt_rnk
,business_cnt_weight
,business_cnt_score
,middle_contract_amt
,end_contract_amt
,avg_customer_contract_amt
,business_amt_rnk
,business_amt_weight
,business_amt_score
,lave_customer_cn
,lave_write_off_amount
,if(lave_customer_cn>0,overdue_score*0.2*-1,0) as lave_score

from tmp_score a 
left join 
(select  
  follow_up_user_code,
  follow_up_user_name,
  follow_up_position,
  sum(lave_write_off_amount)lave_write_off_amount,
  count(distinct customer_code) as lave_customer_cn  
from
  csx_analyse.csx_analyse_fr_hr_red_black_break_deposit_overdue a
 where   is_oveder_flag='是'
    and a.smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
group by  follow_up_user_code,
  follow_up_user_name,
  follow_up_position) b on b.follow_up_user_code=a.sales_user_number
)a 
order by performance_region_name,
total_rank
;


-- ******************************************************************** 
-- @功能描述：销售员商机明细表
-- @创建者： 彭承华 
-- @创建者日期：2025-02-13 11:51:48 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;

--  2.0 新签合同金额明细  创建商机时间为半年区间
-- drop table  csx_analyse_tmp.csx_analyse_tmp_business_01 ;
-- create table csx_analyse_tmp.csx_analyse_tmp_business_01 as 
with business as  
(select  business_number,
    customer_id,
    a.customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    htjey/10000 htjey,
    htqsrq,  --  合同起始日期
	  htzzrq,  --  合同终止日期
    yue,
    create_time,
    case when day(to_date(create_time)) between 1 and 15 then '上半月'else '下半月' end days_note,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    business_type_name,
    contract_number,
    -- 年化，不足一年按照一年计算，超一年/12
    if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) as tran_year,
    -- 年化金额，先取合同金额再取商机金额
    (if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,b.htjey/10000 ,a.estimate_contract_amount)/if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) ) tran_contract_amount,
    rn,
    substr(regexp_replace(to_date(create_time),'-',''),1,6) month
from 
 ( select business_number,
    customer_id,
    customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    business_sign_time,
    estimate_contract_amount,
    create_time,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    business_type_name,
    contract_number,
    contract_begin_date,
    contract_end_date,
    business_stage,
    approval_status_code,
    row_number()over(partition by customer_id,business_number,owner_user_number) rn 
  from     csx_dim.csx_dim_crm_business_info a 
    where sdt='current'
     and status=1
     and business_type_code in (1,2,6)
     and business_stage >= 2
     and substr(create_time, 1, 10) >=  to_date(trunc(add_months (last_day(add_months('${sdt_yes_date}',-1)),-5),'MM'))  -- 近半年区间
     and substr(create_time, 1, 10) <= last_day(add_months('${sdt_yes_date}',-1))
     and approval_status_code!=3
    --  and business_number='SJ24111900006'
 )a 
left join 
-- 可以取最新日期关联合同号
(select 
    t1.htbh,--  合同编码
   (case when length(trim(t1.customernumber))>0 then trim(t1.customernumber) else t3.customer_code end) as customer_no,  --  客户编码
  htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
	round(datediff(htzzrq,htqsrq)/30.5,0) yue
from 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
   where sdt= regexp_replace(date_sub(current_date,1),'-','') 
   and length(htbh)>0) t1 
 left join 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_dt4_df 
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t2 
   on t1.id=t2.mainid 
left join 
   (select * 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t3 
   on t2.khmc=t3.customer_name
   )b   on b.customer_no=a.customer_code  and b.htbh=a.contract_number   

)
insert overwrite table csx_analyse.csx_analyse_fr_hr_sales_red_black_business_detail	 partition(smt)
 select 
    substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as sale_month,
    days_note,
    a.business_number,
    customer_id,
    a.customer_code,
    customer_name,
    owner_user_number,
    owner_user_name	,
    -- owner_user_position,
    user_position_name,
    -- sub_position_name,
    begin_date,
    -- owner_province_id,
    -- owner_province_name,
    -- owner_city_code,
    -- owner_city_name	,
    performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.business_type_code,
    business_type_name,
    -- business_attribute_code	,
    -- business_attribute_name	,
    business_stage,
    business_sign_time,
    estimate_contract_amount,   -- 商机签约金额
--     htjey ,          -- 泛微合同金额
--     htqsrq,  --  合同起始日期
-- 	htzzrq,  --  合同终止日期
--     yue,
    create_time,
    contract_cycle_int,
    contract_cycle_desc,
    contract_number,
    tran_year,
    tran_contract_amount,   -- 年化金额
    rn,
    max_sdt,
    after_date,
    current_timestamp() update_time,
    substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
from business a 
left join    
(select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
) s on a.owner_user_number=s.user_number 
 left join 
 -- 判断是否三个月以上断约客户
 (select 	performance_province_name, 
    business_type_code,
	after_date, 
	a.customer_code,
	max_sdt
from
(		select 
		    performance_province_name,
		    business_type_code,
			customer_code,
			max(sdt) max_sdt,
			regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd'),'yyyy-MM-dd'),90) as string),'-','') as after_date
		from 
			csx_dws.csx_dws_sale_detail_di 
		where 
			sdt between '20220101' and regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') -- 取上个月月末最后一天
			and business_type_code in ('1','2','6')           --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9')    --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and order_channel_code not in (4,6)
		group by 
			performance_province_name,
		    business_type_code,
			customer_code
		) a
	   where 1=1
	   -- and after_date>='20241201'  -- 大于当月的正在履约
			group by performance_province_name, 
			business_type_code,
			after_date, 
			a.customer_code,
			max_sdt 
	)b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code and a.performance_province_name=b.performance_province_name
	left join   
( select customer_code,customer_name
    from csx_dim.csx_dim_crm_customer_info
     where sdt= 'current'
        and channel_code  in ('1','7','9')
) c on a.customer_code=c.customer_code     
-- 	where after_date<'20241201' or after_date is null    
;


-- ******************************************************************** 
-- @功能描述：保证金逾期明细
-- @创建者： 彭承华 
-- @创建者日期：2025-02-13 19:27:07 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;
-- 保证金断约客户 3.0
-- 查找实际中没有包含主的
-- 查找实际中没有包含主的
drop table csx_analyse_tmp.csx_analyse_tmp_incidental;
create table csx_analyse_tmp.csx_analyse_tmp_incidental as 
with temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and status=1
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
  is_history_compensate
) 
--  select * from temp_company_credit where customer_code='243205'
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    a.customer_name,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    business_type_name,
    -- business_attribute_name,
    break_contract_date,
    break_contract,
    d.create_time,
    coalesce(business_type_name,f.business_attribute_name,j.business_attribute_name) as new_business_type_name
from (
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    customer_name,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    real_perform_custom2 new_real_customer_code,
    business_type_name,
    break_contract_date,
    break_contract
from (
  select
    belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    receiving_customer_code as customer_code,
    receiving_customer_name as customer_name,
    sum(cast(lave_write_off_amount as decimal(26, 2))) lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    coalesce(b.business_type_name,c.business_type_name) business_type_name,
    break_contract_date,
    break_contract,
    coalesce(real_perform_customer_code,'') real_perform_customer_code,
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code) as new_real_customer_code
  from
     csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di a 
    left join 
    -- 历史业务类型手工导入
    (  select incidental_expenses_no,
      customer_code,
      busniess_type_code business_type_name 
    from csx_analyse_tmp.csx_analyse_tmp_incidental_customer_history
    ) b on a.receiving_customer_code=b.customer_code and a.incidental_expenses_no=b.incidental_expenses_no
    left join 
    (select incidental_expenses_no,
        purchase_code,
        purchase_name,
        company_code,
        company_name,
        customer_code,
        customer_name,
        business_type,
        regexp_replace(business_type_name,'单','') as business_type_name
    from csx_ods.csx_ods_csx_b2b_sss_sss_incidental_config_ss_df  
      where sdt=regexp_replace(date_sub(current_date(),1),'-','')
    ) c on a.incidental_expenses_no= c.incidental_expenses_no 
        and c.customer_code=a.receiving_customer_code

  where
    self_employed = 1
    and cast(lave_write_off_amount as decimal(26, 2)) > 0
    and business_scene_code in (2,3)
    -- 是否回款
  --  and break_contract =1
  group by
    belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    receiving_customer_name,
    responsible_person,
    responsible_person_number,
    payment_company_code,
    receiving_customer_code,
    credit_customer_code,
    follow_up_user_code,
    follow_up_user_name	,
    coalesce(real_perform_customer_code,''),
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code),
    coalesce(b.business_type_name,c.business_type_name) ,
    break_contract_date,
    break_contract
) a 
LATERAL VIEW EXPLODE(
    SPLIT(
      new_real_customer_code, ',')
    )t as real_perform_custom2
)a 
   left join 
   (select customer_code,create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') 
   ) d
   on a.new_real_customer_code=d.customer_code
   left join 
  (select * 
  from  temp_company_credit
    where status=1
    ) f on a.customer_code=f.customer_code and a.credit_customer_code=f.credit_code  and f.company_code=a.payment_company_code
   left join 
  (select * 
  from  temp_company_credit
    where is_history_compensate=1
     and status!=1
    ) j on a.customer_code=j.customer_code and a.credit_customer_code=j.credit_code  and j.company_code=a.payment_company_code
   )
--select * from temp_incidental
;

-- 处理主客户没有在实际履约客户
drop table  csx_analyse_tmp.csx_analyse_tmp_incidental_01;
create table csx_analyse_tmp.csx_analyse_tmp_incidental_01 as 

with temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and status=1
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
  is_history_compensate
) ,
  temp_incidental_01 as 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    credit_code as new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name
from 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    credit_customer_code credit_code,
    t.business_attribute_name,
    break_contract_date,
    break_contract,
    a.customer_name
from csx_analyse_tmp.csx_analyse_tmp_incidental a 
left join 
  temp_company_credit t on a.customer_code=t.customer_code and a.credit_customer_code=t.credit_code  and t.company_code=a.payment_company_code
 where coalesce(real_perform_customer_code,'')='' 

  union all 

select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    f.credit_code,
    f.business_attribute_name,
    break_contract_date,
    break_contract,
    a.customer_name
from csx_analyse_tmp.csx_analyse_tmp_incidental a 
left join 
  temp_company_credit f on a.new_real_customer_code=f.customer_code and a.new_business_type_name=f.business_attribute_name  and f.company_code=a.payment_company_code
  where coalesce(real_perform_customer_code,'') !='' 
)a
),

temp_incidental_02 as (
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    business_type_name,
    new_business_type_name,
    new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name,
    '2' aa
from (
select  a.belong_region_code,
    a.belong_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.payment_company_code,
    a.credit_customer_code,
    a.customer_code,
    a.lave_write_off_amount,
    a.follow_up_user_code,
    a.follow_up_user_name	,
    a.responsible_person,
    a.responsible_person_number,
    a.real_perform_customer_code,
    a.customer_code as new_real_customer_code,
    a.create_time,
    a.business_type_name,
    a.new_business_type_name,
    a.credit_customer_code as new_real_credit_code,
    a.business_attribute_name,
    a.break_contract_date,
    a.customer_name,
    break_contract,
    '2' as aa
from 
  temp_incidental_01 a
left join 
(select credit_customer_code,
        payment_company_code,
        new_real_customer_code,
        lave_write_off_amount
from temp_incidental_01) b on a.payment_company_code=b.payment_company_code 
    and a.credit_customer_code=b.credit_customer_code 
    and a.customer_code=b.new_real_customer_code 
    and a.lave_write_off_amount=b.lave_write_off_amount
where b.new_real_customer_code is null
    and a.new_real_customer_code !=''
) a 
)
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    business_type_name,
    new_business_type_name,
    new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name 
from temp_incidental_02
union all
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    business_type_name,
    new_business_type_name,
    new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name 
from temp_incidental_01 a 

;

-- 创建临时表结果
drop table csx_analyse_tmp.csx_analyse_tmp_hr_red_black_break_contract;
create table csx_analyse_tmp.csx_analyse_tmp_hr_red_black_break_contract as 
with 
tmp_receive_amt as (
select  a.company_code,
    a.customer_code,
    a.credit_code,
    max_paid_date,
    receivable_amount
from 
  (select
    a.company_code,
    a.customer_code,
    credit_code ,
    sum(receivable_amount) receivable_amount
  from
     csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df a
  where
    1=1
   -- and receivable_amount<=0
   -- and a.sdt>=c.max_paid_date
   and sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
  group by
    a.company_code,
    a.customer_code,
    a.credit_code
    ) a 
join 
    (select
          customer_code,
          company_code,
          credit_code,
          regexp_replace(to_date(max(paid_time)),'-','') max_paid_date
        from
          csx_dwd.csx_dwd_sss_close_bill_account_record_di
          where pay_amt>0
        group by
          customer_code,
          company_code,
          credit_code) c on a.company_code=c.company_code and a.customer_code=c.customer_code and a.credit_code=c.credit_code
),
-- 归档合同 
temp_contract_info as 
(select 
    htbh,--  合同编码
    company_code, -- 签约主体
    customer_no,  --  客户编码
    customer_name,
    htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
    ywlx,
	case when ywlx=0 then '日配'
	   when ywlx=1 then '福利'
	   when ywlx=2 then '大宗'
	   when ywlx=3 then '内购批发'
	   when ywlx=4 then 'BBC'
	   when ywlx=5 then 'M端'
	   when ywlx =6 then 'OEM代工'
	   when ywlx=7 then '代仓代配'
	   end business_type_name,
	create_time,
	yue
from 
   (
select 
    t1.htbh,--  合同编码
    wfqyztxz company_code, -- 签约主体
   (case when length(trim(t1.customernumber))>0 then trim(t1.customernumber) else t3.customer_code end) as customer_no,  --  客户编码
    cast(htjey as decimal(26,6)) htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
	coalesce(t1.ywlx,t2.ywlx) as ywlx,
	round(datediff(htzzrq,htqsrq)/30.5,0) yue
from 
   (select * 
   from   csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
   where sdt= regexp_replace(date_sub(current_date,1),'-','') 
   and length(htbh)>0) t1 
 left join 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_dt4_df 
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t2 
   on t1.id=t2.mainid 
left join 
   (select * 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t3 
   on t2.khmc=t3.customer_name
   union all 
   -- 旧合同归档
 select htbh,
    company_code,
    customer_no,
    cast(htjey as decimal(26,6)) htjey,
    htqsrq,
    htzzrq,
    ywlx,
    round(datediff(htzzrq,htqsrq)/30.5,0) yue
from csx_analyse.csx_analyse_dws_crm_w_a_uf_xshttz  
        where sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
   )a 
   left join 
   (select customer_code,
        customer_name,
        create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t3 
   on a.customer_no=t3.customer_code
   ),
-- -- 取断约客户
-- 合同结束日期
 tmp_business as  
(select  a.company_code,
    business_number,
    credit_code,
    customer_id,
    a.customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    -- approval_status_name,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    htjey/10000 htjey,
    if(b.customer_no is not null ,htqsrq, contract_begin_date) contract_begin_date,  --  合同起始日期
	if(b.customer_no is not null ,htzzrq, contract_end_date) contract_end_date, --  合同终止日期
    yue,
    d.create_time,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    contract_number,
    -- 年化，不足一年按照一年计算，超一年/12
    if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) as tran_year,
    -- 年化金额，先取合同金额再取商机金额
    (if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,b.htjey/10000 ,a.estimate_contract_amount)/if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) ) tran_contract_amount
from 
 ( select business_number,
    customer_id,
    customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    create_time,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    contract_number,
    contract_begin_date,
    contract_end_date,
    credit_code,
    a.company_code
  from   csx_dim.csx_dim_crm_business_info a 
    where sdt='current'
     and status=1
     and business_type_code in (1,2,6)
     and business_stage = 5
 )a 
left join 
-- 可以取最新日期关联合同号
   temp_contract_info b   on b.customer_no=a.customer_code  and b.htbh=a.contract_number  
   left join 
   (select customer_code,create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') 
   ) d
   on a.customer_code=d.customer_code

),
temp_sale as 
(select customer_code,
    credit_code,
    company_code,
    sign_company_code,
    max(sdt) max_sdt
 from csx_dws.csx_dws_sale_detail_di 
   group by 
    customer_code,
    credit_code,
    company_code,
    sign_company_code
)
-- select * from tmp_business where customer_code='103207'

,
temp_result as 
(select 
    a.belong_region_name,
    performance_province_code,
    a.performance_province_name,
    a. payment_company_code,
    a.credit_customer_code,
    a.customer_code as sign_customer_code,
    a.new_real_customer_code as customer_code,
    a.customer_name,
    create_time,
    a.follow_up_user_code,
    a.follow_up_user_name	,
    e.sales_user_number,
    e.sales_user_name,
    a.new_business_type_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    a.new_real_credit_code,
    b.max_paid_date  as receive_sdt,
    coalesce(regexp_replace(to_date(c.contract_end_date),'-','') , regexp_replace(to_date(f.contract_end_date),'-',''),regexp_replace(to_date(h.contract_end_date),'-','')) contract_end_date,
    coalesce(regexp_replace(to_date(a.break_contract_date),'-',''),'') break_contract_date,
    max_sdt max_sale_sdt,
    receivable_amount
from 
    (select  belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        payment_company_code,
        credit_customer_code,
        a.customer_code,
        lave_write_off_amount,
        follow_up_user_code,
        follow_up_user_name	,
        responsible_person,
        responsible_person_number,
        real_perform_customer_code,
        new_real_customer_code,
        create_time,
        new_business_type_name,
        new_real_credit_code,
        business_attribute_name,
        break_contract_date,
        customer_name
    from csx_analyse_tmp.csx_analyse_tmp_incidental_01 a
        where break_contract=1
    )a  
left join 
    (select * from tmp_receive_amt where receivable_amount<=0) b on a.new_real_customer_code=b.customer_code and a.payment_company_code=b.company_code and a.new_real_credit_code=b.credit_code

left join 
(select * from 
(select
  company_code,
  customer_code,
  credit_code,
  contract_end_date,
  contract_begin_date,
  business_attribute_code	,
  business_attribute_name	,
  business_type_code, 
  row_number() over(partition by customer_code,credit_code,company_code,business_attribute_code order by contract_end_date desc) rn
from tmp_business
  where create_time>='2023-02-09'
 )a 
  where rn=1 
 )c  on a.new_real_customer_code=c.customer_code and a.payment_company_code=c.company_code and a.new_real_credit_code=c.credit_code 
  -- 客户创建时间23年2月9号，关联按照客户+公司+日配+业务
  left join 
(select * from 
(select
  company_code,
  customer_no ,
  htqsrq contract_begin_date,
  htzzrq contract_end_date,
  business_type_name, 
  row_number() over(partition by customer_no,company_code,business_type_name order by coalesce(htzzrq,'') desc) rn
from temp_contract_info a 
-- left join 
-- temp_contract_info b on a.customer_code=b.customer_code and a.company_code=b.company_code and a.
 where create_time<'2023-02-09'
 )a 
  where rn=1 
  )f  on a.new_real_customer_code=f.customer_no and a.payment_company_code=f.company_code  and f.business_type_name=a.new_business_type_name
left join 
(select * from 
(select
  company_code,
  customer_no ,
  htqsrq contract_begin_date,
  htzzrq contract_end_date,
  business_type_name, 
  row_number() over(partition by customer_no,company_code,business_type_name order by coalesce(htzzrq,'') desc) rn
from temp_contract_info a 
 )a 
  where rn=1 
  )h  on a.new_real_customer_code=h.customer_no and a.payment_company_code=h.company_code  and h.business_type_name=a.new_business_type_name
 left join 
(select customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        performance_province_name,
        performance_city_name
from   csx_dim.csx_dim_crm_customer_info
where sdt='current') e on a.new_real_customer_code=e.customer_code
left join temp_sale j on a.new_real_customer_code=j.customer_code and a.new_real_credit_code=j.credit_code and a.payment_company_code=j.sign_company_code
 -- where a.new_real_customer_code='112189'
)

select * ,
  if(receivable_amount>0,'否','是') is_receive_oveder_flag,
  case when date_add(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'),'yyyy-MM-dd'),30)>last_day(add_months('${sdt_yes_date}',-1)) or receivable_amount>0 then '否'
    when date_add(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'),'yyyy-MM-dd'),30)<=last_day(add_months('${sdt_yes_date}',-1))
        and coalesce(receivable_amount,0) <= 0   then '是'
    else '否' end  as is_oveder_flag
  
from 
(
select 
    a.belong_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.payment_company_code,
    a.credit_customer_code,
    a.sign_customer_code,
    a.customer_code,
    a.customer_name,
    a.create_time,
    a.follow_up_user_name,
    a.follow_up_user_code,
    a.sales_user_number,
    a.sales_user_name,
    a.new_business_type_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    a.new_real_credit_code,
    a.receive_sdt,
    a.contract_end_date,
    a.break_contract_date,
    sort_array(array(receive_sdt,contract_end_date,break_contract_date))[size(array(receive_sdt,contract_end_date,break_contract_date))-1] as max_sdt,
    a.max_sale_sdt,
    b.receivable_amount
 from temp_result a 
 left join 
 (select * from tmp_receive_amt ) b on a.customer_code=b.customer_code and a.payment_company_code=b.company_code and a.new_real_credit_code=b.credit_code
)a
 
 ;
 
 
insert overwrite table csx_analyse.csx_analyse_fr_hr_red_black_break_deposit_overdue partition(smt)
 select 
    substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) sale_month,
    
    a.belong_region_name performance_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    sign_customer_code,
    customer_code,
    customer_name,
    create_time,
    
    follow_up_user_name	,
    follow_up_user_code,
    b.user_position_name,
    -- b.sub_position_name,
    -- b.begin_date,
    b.leader_user_number,
    b.leader_user_name,
    -- b.leader_user_position_name,
    b.leader_source_user_position_name,
    b.new_leader_user_number,
    b.new_leader_user_name ,
    new_business_type_name,
    responsible_person,
    responsible_person_number,
    lave_write_off_amount,
    new_real_credit_code,
    a.receive_sdt,
    a.contract_end_date,
    a.break_contract_date,
    max_sdt,
    max_sale_sdt,
    receivable_amount,
    is_receive_oveder_flag,
    is_oveder_flag,
    current_timestamp() update_time,
    substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) smt
from csx_analyse_tmp.csx_analyse_tmp_hr_red_black_break_contract a 
left join (
    select
      user_position,
      user_position_name,
      sub_position_name,
      begin_date,
      leader_user_number,
      leader_user_name,
      leader_user_position_name,
      leader_source_user_position_name,
      new_leader_user_number,
      new_leader_user_name,
      user_number
    from
      csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
       where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        --  where user_number ='80879367'

  ) b on a.follow_up_user_code = b.user_number 
  -- and a.sale_month=b.sale_month
;

-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2025-02-05 15:52:38 
-- @修改者日期：
-- @修改人：
-- @修改内容： csx_analyse_fr_hr_sales_red_black_over_detail
-- ******************************************************************** 


with 
over_rate as 
(select substr(sdt,1,6) as sale_month,
    performance_region_name as region_name,
    performance_province_name as province_name,
    performance_city_name as city_group_name,
    customer_code, 
    customer_name,
    business_attribute_name as business_attribute_name,
    credit_business_attribute_name,
    channel_name,
    sales_employee_code,
    sales_employee_name,
    case when a.customer_code in ('245127') then  '81001273' 
            when a.customer_code in ('252180','252393','252460') then '80946479'
            else sales_employee_code end new_sales_user_number,
        case when a.customer_code in ('245127') then  '徐培召' 
            when a.customer_code in ('252180','252393','252460') then '於佳'
            else sales_employee_name end new_sales_user_name, 
    sum(overdue_amount) as overdue_amount,
    sum(receivable_amount) as receivable_amount
from 
   -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
    csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df a
    where sdt =regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    and ( channel_name in ('大客户','业务代理') 
            or (sales_employee_code in ('81244592','81079752','80897025','81022821','81190209',
                                      '80946479','81102471','81254457','81119082','81149084',
                                      '81103064','81029025','81013168','81149084','81103064',
                                      '81254457') and a.channel_name in ('项目供应商','前置仓'))
        )
    and sales_employee_code not in ('81208614' )  -- 202502 剔除汪平 81206921
    and a.customer_code not in ('234036','224656','247525','243799','244172','237768')
    and receivable_amount>0
    group by substr(sdt,1,6),
    performance_region_name ,
    performance_province_name ,
    performance_city_name ,
    customer_code, 
    customer_name,
    business_attribute_name,
    credit_business_attribute_name,
    channel_name,
    sales_employee_code,
    sales_employee_name  ,
    case when a.customer_code in ('245127') then  '81001273' 
            when a.customer_code in ('252180','252393','252460') then '80946479'
            else sales_employee_code end ,
        case when a.customer_code in ('245127') then  '徐培召' 
            when a.customer_code in ('252180','252393','252460') then '於佳'
            else sales_employee_name end 
)
insert overwrite table csx_analyse.csx_analyse_fr_hr_sales_red_black_over_detail  partition(smt)
select  sale_month,
    region_name,
    a.province_name,
    city_group_name,
    customer_code, 
    customer_name,
    business_attribute_name,
    credit_business_attribute_name,
    channel_name,
    new_sales_user_number,
    new_sales_user_name,
    user_position_name,
    sub_position_name,
    begin_date,
    leader_user_number,
    leader_user_name,
    leader_source_user_position_name,
    new_leader_user_number,
    new_leader_user_name,
    overdue_amount,
    receivable_amount,
    current_timestamp(),
    sale_month smt
from over_rate a 
left join 
(select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
) b on a.new_sales_user_number=b.user_number
-- where sales_employee_name='谢志晓'
;
