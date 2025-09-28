--销售经理红黑榜
-- 销售员红黑榜1.0
-- 销售员信息 HR信息、销售明细取销售员表
-- 只有一名销售经理的省区/城市：河北（詹娟），陕西（曹杰），苏州（章明新）、广东深圳（何渊）合肥胡艳
-- 公共表取销售员红黑榜文件里的：csx_analyse_tmp_hr_sale_info、csx_analyse_tmp_hr_sales_sale
-- 需要经理取经理的，区域经理也参与
 

 
-- 
select performance_region_name
,performance_province_name
,performance_city_name
,
,sales_user_number
,sales_user_name
,sales_user_position
,leader_user_number
,leader_user_name

,leader_source_user_position
,new_leader_user_number
,new_leader_user_name
,accounting_cnt
,sale_amt
,excluding_tax_sales
,qm_receivable_amount
,qc_receivable_amount
,receivable_amount
,turnover_days
 from csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days;
 
-- 应收周转天数用期末城市 销售取含税计算
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days;
create table csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days as 
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
  DATEDIFF('2024-12-31','2024-11-30')as accounting_cnt,
  coalesce(sum(sale_amt),0) sale_amt,
  coalesce(sum(excluding_tax_sales),0) excluding_tax_sales,
  sum(qm_receivable_amount) qm_receivable_amount,
  sum(qc_receivable_amount) qc_receivable_amount,
  sum(qm_receivable_amount+qc_receivable_amount)/2 receivable_amount,
  if(sum(qm_receivable_amount+qc_receivable_amount)/2 =0 or coalesce(sum(sale_amt),0)=0,0,
        DATEDIFF('2024-12-31','2024-11-30')/(coalesce(sum(sale_amt),0)/(sum(qm_receivable_amount+qc_receivable_amount)/2 ))) as turnover_days
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
         sum(if(sdt='20241231',receivable_amount,0))  qm_receivable_amount,
         sum(if(sdt='20241130',receivable_amount,0))  qc_receivable_amount
         --应收账款
       from 
         -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
	      csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
       where sdt in ('20241231' ,'20241130')  
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
           where sdt >='20241201'   and sdt <='20241231'
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
                m.sales_user_position,
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
            left join csx_analyse_tmp.csx_analyse_tmp_hr_sale_info p on m.sales_user_number=p.user_number
              where m.sdt= '20241231'
                and (dev_source_code !=3 
                    or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209',
                                      '80946479','81102471','81254457','81119082','81149084',
                                      '81103064','81029025','81013168','81149084','81103064','81254457') 
                    and dev_source_code=3)
                    or customer_code='239912'
                    )
                and sales_user_number not in ('81208614','81206921')
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
;
    
-- 输出结果集

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
      new_sales_user_number as sales_user_number,
      begin_date,
    --   sales_user_position,
      user_position_name,
      sub_position_name,
      if(substr(begin_date,1,6)>='202409',0,1 ) new_sales_flag,
      leader_user_number sales_manager_number,
      leader_user_name,
      leader_source_user_position_name leader_user_position_name,
      new_leader_user_number,
      new_leader_user_name,
      sum(sale_amt) sale_amt,
      sum(profit)profit,
      sum(if(new_customer_flag=1,sale_amt,0)) as new_customer_sale_amt,
      sum(if(new_customer_flag=1,profit,0)) as new_customer_profit
 from    csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale
 -- where sales_user_position in ('SALES_MANAGER','SALES','SALES_CITY_MANAGER')
  group by sale_month,
      performance_region_name,
      new_sales_user_number,
      begin_date,
    --   sales_user_position,
      user_position_name,
      sub_position_name,
      if(substr(begin_date,1,6)>='202409',0,1 ) ,
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
select smt,
    performance_region_name,
    sales_user_number,
    b.user_name sales_user_name,
    b.user_position	,
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
        DATEDIFF('2024-12-31','2024-11-30')/(coalesce(sum(trunover_sale_amt),0)/(sum(qm_receivable_amount+qc_receivable_amount)/2 ))) as turnover_days,
    max(lave_customer_cn) lave_customer_cn,
    max(lave_write_off_amount) as lave_write_off_amount
from 
(-- 目标表
    select smt,
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
where smt='202412' 
    and sale_month='202412'
union all 
-- 上一级是经理
select '202412'sale_month,
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
 
 select '202412'sale_month,
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
from  csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days
      where leader_user_number != coalesce(new_leader_user_number,'')
      group by  performance_region_name,
    leader_user_number 
  union all 
  -- 单独处理城市经理
select '202412'sale_month,
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
   select '202412'sale_month,
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
from  csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days
      where new_leader_user_number is not null 
      group by performance_region_name,
      new_leader_user_number
)a 
left join 
(select  
  leader_user_number,
  leader_user_name,
  sum(lave_write_off_amount)lave_write_off_amount,
  count(distinct customer_code) as lave_customer_cn  
from
  csx_analyse_tmp.csx_analyse_tmp_hr_red_black_break_contract a
  left join (
    select
      *,
      substr(sdt, 1, 6) sale_month
    from
      csx_analyse_tmp.csx_analyse_tmp_hr_sale_info --  where user_number ='80879367'

  ) b on a.follow_up_user_code = b.user_number -- and a.sale_month=b.sale_month
         where leader_user_number != coalesce(new_leader_user_number,'') 
            and is_oveder_flag='是'
    group by leader_user_number,
        leader_user_name
union all   
select  
  new_leader_user_number leader_user_number,
  new_leader_user_name leader_user_name,

  sum(lave_write_off_amount)lave_write_off_amount,
  count(distinct customer_code) as lave_customer_cn  
from
  csx_analyse_tmp.csx_analyse_tmp_hr_red_black_break_contract a
  left join (
    select
      *,
      substr(sdt, 1, 6) sale_month
    from
      csx_analyse_tmp.csx_analyse_tmp_hr_sale_info --  where user_number ='80879367'

  ) b on a.follow_up_user_code = b.user_number -- and a.sale_month=b.sale_month
   where new_leader_user_number is not null
    and is_oveder_flag='是'
    group by  
  new_leader_user_number,
  new_leader_user_name
  
  ) c on a.sales_user_number=c.leader_user_number
left join   
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_info b on a.sales_user_number=b.user_number	
--   left join 
--       csx_analyse_tmp.csx_analyse_tmp_hr_sale_info c on a.new=b.user_number	

group by smt,
    performance_region_name,
    sales_user_number,
    b.user_name,
    b.user_position	,
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
select performance_region_name,
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
select performance_region_name,
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
select performance_region_name,
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
    coalesce(lave_write_off_amount,'')lave_write_off_amount
  from 
(select performance_region_name,
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
order by total_rank asc 