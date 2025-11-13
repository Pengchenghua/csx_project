-- 销售红黑榜线上表SQL--服务管家
调度最晚日期为10号，
0、销售管家信息表：csx_analyse_fr_hr_red_balck_service_manager_info  调度日期1-3号止
1、销售管家明细表：csx_analyse_fr_hr_red_balck_service_manager_sale_detail
2、销售管家逾期明细表：csx_analyse.csx_analyse_fr_hr_red_black_service_manager_receivable_overdue
3、销售管家目标表：csx_analyse.csx_analyse_source_write_hr_service_red_black_target_mf
4、保证金明细表：csx_analyse_fr_hr_red_black_break_deposit_overdue_detail
5、管家评价表： csx_analyse.csx_analyse_fr_hr_customer_evaluation_detail_mf、 csx_ods.csx_ods_csx_b2b_study_fm_user_form_data_df a LEFT JOIN csx_ods.csx_ods_csx_b2b_study_questionnaire_paper_df
6、日清日结：job_csx_analyse_report_order_control_oneday_settlement_df_ss_mf
7、结果表：csx_analyse_fr_hr_red_black_service_manager_result_mf


-- ******************************************************************** 
-- @功能描述：销售红黑榜-服务管家信息表
-- @创建者： 彭承华 
-- @创建者日期：2025-02-17 11:30:14 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 




-- 管家信息
insert overwrite table csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info partition(smt)
select sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    customer_no,
    customer_name,
    service_user_work_no,
    service_user_name,
    attribute_code,
    attribute_name,
    business_type_code,
    service_manager_user_position,
    sales_user_name,
    sales_user_number,
    sales_user_position,
    count(*) over(partition by customer_no, attribute_code ) as cnt,
    row_number() over(partition by customer_no,attribute_code order by service_user_work_no asc  ) as ranks_rnk,
    current_timestamp() as update_time,
    smt
from ( 
  select sdt,
    substr(sdt, 1, 6) as smt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    customer_code as customer_no,
    customer_name,
    case when customer_code in ('250879') then '81241667' 
        when customer_code in ('249548') then '81261644'
        -- when customer_code in ('247826') then 
    else service_manager_user_number end  service_user_work_no,
    case when customer_code in ('250879') then '肖少萍' 
        when customer_code in ('249548') then '王毅姣'
    else service_manager_user_name  end service_user_name,
    business_attribute_code attribute_code,
    business_attribute_name attribute_name,
    case
      when business_attribute_code = 1 then 1
      when business_attribute_code = 2 then 2
      when business_attribute_code = 5 then 6
    end business_type_code,
    service_manager_user_position,
    sales_user_name,
    sales_user_number,
    sales_user_position,
    -- count() over(partition by customer_code, business_attribute_code ) as cnt,
    -- row_number() over(partition by customer_code,business_attribute_code order by service_manager_user_number asc  ) as ranks,
    current_timestamp() as update_time
  from csx_dim.csx_dim_crm_customer_business_ownership
  where 
  sdt = regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
  and (
    customer_code not in ('250879', '249548', '127661', '104281')
    or (
      customer_code = '104281' 
      and service_manager_user_number != '81273957'  -- 剔除程智
    )
    or (
      customer_code in ('250879', '249548')
      and service_manager_user_position = 'CUSTOMER_SERVICE_MANAGER'
    )
    )
    
 
)a 
where service_user_work_no not in ('81051586','81095718')
;


-- 服务管家评分结果表


-- 输出结果集


with tmp_service_evaluation as ( 
with tmp_sales_info as   
(  
    select smt,  
        performance_region_name,  
        performance_province_name,
        customer_no, 
        customer_name,
        service_user_work_no,  
        service_user_name,  
        service_manager_user_position,  
        count(*) over(partition by customer_no) as cnt,  
        row_number() over(partition by customer_no order by service_user_work_no asc) as ranks  
    from      csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info a 
    join 
    (select customer_code
    from  csx_dws.csx_dws_sale_detail_di 
        where sdt>='20250201' and sdt<='20250228'
        -- 日配客户
        and business_type_code=1
    group by customer_code ) b on a.customer_no=b.customer_code
    where    smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    group by performance_region_name,  
        performance_province_name,
        customer_no, 
        customer_name,
        service_user_work_no,  
        service_user_name,  
        service_manager_user_position,
        smt
)  
select   
    smt,
    performance_region_name,  
    service_user_work_no,
    customer_no,  
    sum(cast(answer_score as decimal(26,1))) as answer_score  
from (  
    select   
        smt,
        si.performance_region_name,  
        si.service_user_work_no, 
        si.customer_no,
        si.customer_name,
        case   
            when se.answer = '非常满意' then 10  
            when se.answer = '满意' then 8  
            when se.answer = '一般' then 6  
            when se.answer = '不满意' then 2  
            when se.answer = '非常不满意' then 0  
            else 30   
        end as answer_score  
    from tmp_sales_info si  
    left join csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation  se on se.customer_code = si.customer_no  
) a  
where service_user_work_no is not null and  service_user_work_no!=''
group by performance_region_name, 
  service_user_work_no,
  customer_no,
  smt
),
 tmp_full_total as(
select sale_month,
    performance_region_name,
    a.service_user_work_no,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    dense_rank()over(PARTITION BY performance_region_name order by sale_achieve_rate  desc )  sale_rank,
    sale_weight,
    plan_profit,
    profit,
    profit_achieve_rate,
    dense_rank()over(partition by performance_region_name order by profit_achieve_rate  desc ) profit_rank,
    profit_weight,
    coalesce(overdue_rate,0)overdue_rate,
    dense_rank()over(partition by performance_region_name order by coalesce(overdue_rate,0) asc  ) overdue_rank,
    overdue_amount,
    receivable_amount,
    overdue_weight,
    answer_score,
    answer_rank,
    answer_weight,
    customer_cnt,
    all_answer_score,
    lave_customer_cn,
    lave_write_off_amount
from (
select sale_month,
    performance_region_name,
    service_user_work_no,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    dense_rank()over(PARTITION BY performance_region_name order by coalesce(sale_achieve_rate ,0) desc ) sale_rank,
    sale_weight,
    plan_profit,
    profit,
    profit_achieve_rate,
    dense_rank()over(partition by performance_region_name order by coalesce(profit_achieve_rate,0)  desc ) profit_rank,
    profit_weight,
    coalesce(overdue_rate,0) as overdue_rate,
    dense_rank()over(partition by performance_region_name order by coalesce(overdue_rate ,0) asc  ) as overdue_rank,
    overdue_amount,
    receivable_amount,
    overdue_weight,
    answer_score,
    dense_rank()over(partition by performance_region_name order by coalesce(answer_score,0) desc  ) as answer_rank,
    answer_weight,
    customer_cnt,
    all_answer_score
from 
(select sale_month,
    performance_region_name,
    service_user_work_no,
    b.user_name sales_user_name,
    b.user_position	,
    b.sub_position_name,
    begin_date,
    sum(plan_sales_amt)plan_sales_amt,
    coalesce(sum(avg_sale_amt),0)/10000 sale_amt,
    coalesce(sum(avg_sale_amt),0)/10000 /coalesce(sum(plan_sales_amt),0) as sale_achieve_rate,
    0.2 as sale_weight,
    sum(plan_profit) plan_profit,
    sum(avg_profit)/10000 profit,
    sum(avg_profit)/10000/sum(plan_profit) as profit_achieve_rate,
    0.3 as profit_weight,
    coalesce(sum(overdue_amount)/sum(receivable_amount),0) as overdue_rate,
    sum(overdue_amount)/10000 overdue_amount,
    sum(receivable_amount)/10000 receivable_amount,
    0.2 as overdue_weight,
    sum(answer_score) answer_score,
    0.3 as answer_weight,
    sum(customer_cnt) customer_cnt,
    sum(all_answer_score) all_answer_score
from (
-- 目标表
select smt as sale_month,
    if(performance_region_name like '%大区',performance_region_name,concat(performance_region_name,'大区')) as performance_region_name,
    sales_user_number service_user_work_no, 
    cast(plan_sales_amt as decimal(26,6)) plan_sales_amt,
    cast(plan_profit as decimal(26,6))  plan_profit,
    0 avg_sale_amt,
    0 avg_profit,
    0 overdue_amount,
    0 receivable_amount,
    0 answer_score,
    0 customer_cnt,
    0 all_answer_score
from 
     csx_analyse.csx_analyse_source_write_hr_service_red_black_target_mf a 
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
    and sale_month=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
    union all 
select  sale_month,
        performance_region_name,
        new_service_user_work_no  as service_user_work_no,
        0 plan_sales_amt,
        0 plan_profit,
        avg_sale_amt,
        avg_profit,
        0 overdue_amount,
        0 receivable_amount,
        0 answer_score,
        0 customer_cnt,
        0 all_answer_score
    from csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_sale_detail
     where (new_service_user_work_no !='' or new_service_user_work_no is not null )
     and smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
     and new_customer_flag!=1
 union all 
 select  sale_month,
    performance_region_name,
    new_service_user_work_no as service_user_work_no,
    0 plan_sales_amt,
    0 plan_profit,
    0 avg_sale_amt,
    0 avg_profit,
    sum(overdue_amount) overdue_amount,
    sum(receivable_amount) receivable_amount,
    0 answer_score,
    0 customer_cnt,
    0 all_answer_score
from  
  csx_analyse.csx_analyse_fr_hr_red_black_service_manager_receivable_overdue
 where new_service_user_work_no !=''
    and smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
    group by sale_month,
    performance_region_name,
    new_service_user_work_no,
    sale_month
  union all 
  select 
    smt sale_month,
    performance_region_name,  
    service_user_work_no,      
    0 plan_sales_amt,
    0 plan_profit,
    0 avg_sale_amt,
    0 avg_profit,
    0 overdue_amount,
    0 receivable_amount,
    avg(answer_score) answer_score,
    count(distinct customer_no) as customer_cnt,
    sum(answer_score) all_answer_score
  from   
   tmp_service_evaluation a 
   
    group by performance_region_name,  
    service_user_work_no,
    smt
) a 
left join
(select * from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
   ) b on a.service_user_work_no=b.user_number

 -- where plan_sales_amt <> 0
group by sale_month,
    performance_region_name,
    service_user_work_no,
    b.user_name ,
    b.user_position	,
    b.sub_position_name,
    begin_date
) a 
 where plan_sales_amt<>0
)as a
-- 管家保证金
left join 
(
select  
  coalesce(service_user_work_no,'')service_user_work_no,
  sum(lave_write_off_amount)/count(a.customer_code) lave_write_off_amount,
  count(distinct a.customer_code) as lave_customer_cn  
from
(
select  
  customer_code,
  (lave_write_off_amount)lave_write_off_amount,
  service_user_work_no,
  service_user_name
 from (
select  
  customer_code,
  is_oveder_flag,
  (lave_write_off_amount)lave_write_off_amount
from
   csx_analyse.csx_analyse_fr_hr_red_black_break_deposit_overdue a
   where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
   
 )a 
  left join 
   (  
    select    
        performance_region_name,  
        performance_province_name,
        a.customer_code as customer_no, 
        customer_name,
        service_manager_user_number as service_user_work_no,  
        service_manager_user_name as service_user_name,  
        service_manager_user_position,  
        count(*) over(partition by customer_code) as cnt,  
        row_number() over(partition by customer_code order by service_manager_user_number asc) as ranks  
    from      csx_dim.csx_dim_crm_customer_business_ownership  a 
    join 
    (select customer_code
    from  csx_dws.csx_dws_sale_detail_di 
        where sdt>=regexp_replace(last_day(add_months('${sdt_yes_date}',-4)),'-','') 
            and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    group by customer_code 
    ) b on a.customer_code=b.customer_code
    where sdt = substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) 
        and business_attribute_code in ('1','2','5')
      --  and service_manager_user_position='CUSTOMER_SERVICE_MANAGER'
    group by performance_region_name,  
        a.customer_code, 
        customer_name,
        service_manager_user_number ,  
        service_manager_user_name ,  
        -- service_manager_user_id ,
        service_manager_user_position,
        performance_province_name
)   b on a.customer_code = b.customer_no 
    where  a.is_oveder_flag='是'
        and  coalesce(service_user_work_no,'')!=''
) a 
group by coalesce(service_user_work_no,'')
)c on a.service_user_work_no=c.service_user_work_no
)
-- select * from full_total 
,
tmp_max_rnk as 
(
select sale_month,
  performance_region_name,
  max(sale_rank) max_sale_rank,
  max(overdue_rank) max_overdue_rank,
  max(answer_rank) max_answer_rank
  from tmp_full_total
  group by sale_month,
  performance_region_name
) 
select a.sale_month,
  a.performance_region_name,
  a.service_user_work_no,
  sales_user_name,
  user_position	,
  sub_position_name,
  begin_date,
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
  overdue_rate,
  overdue_rank,
  overdue_amount,
  receivable_amount,
  overdue_weight,
  overdue_score,
  answer_score,
  answer_rank,
  answer_weight,
  new_answer_score,
  customer_cnt,
  all_answer_score,
  lave_customer_cn,
  if(lave_customer_cn>0,overdue_score*0.2*-1,0) lave_score,
  lave_write_off_amount
from
(
select a.sale_month,
  a.performance_region_name,
  a.service_user_work_no,
  sales_user_name,
  user_position	,
  sub_position_name,
  begin_date,
  
  dense_rank()over(partition by a.performance_region_name order by (sale_score+profit_score+overdue_score+new_answer_score+if(lave_customer_cn>0,overdue_score*0.2*-1,0)) desc  ) as total_rank,
  dense_rank()over(partition by a.performance_region_name order by (sale_score+profit_score+overdue_score+new_answer_score+if(lave_customer_cn>0,overdue_score*0.2*-1,0)) asc  ) as last_total_rank,
   (sale_score+profit_score+overdue_score+new_answer_score+if(lave_customer_cn>0,overdue_score*0.2*-1,0)) total_score,
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
  overdue_rate,
  overdue_rank,
  overdue_amount,
  receivable_amount,
  overdue_weight,
  overdue_score,
  answer_score,
  answer_rank,
  answer_weight,
  new_answer_score,
  customer_cnt,
  all_answer_score,
  lave_customer_cn,
  if(lave_customer_cn>0,overdue_score*0.2*-1,0) lave_score,
  lave_write_off_amount
from
( select a.sale_month,
    a.performance_region_name,
    service_user_work_no,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    sale_rank,
    sale_weight,
    CASE  WHEN sale_rank = 1 THEN 20
        when sale_rank=max_sale_rank then 0 
        ELSE 20 - (sale_rank - 1) *(20/(max_sale_rank-1) )
     END  AS sale_score,
    plan_profit,
    profit,
    profit_achieve_rate,
    profit_rank,
    profit_weight,
    CASE
    WHEN profit_rank = 1 THEN 30
    when profit_rank=max_sale_rank then 0 
    ELSE 30 - (profit_rank - 1) *(30/(max_sale_rank-1) )
  END  AS profit_score,
    overdue_rate,
    overdue_rank,
    overdue_amount,
    receivable_amount,
    overdue_weight,
    CASE
    WHEN overdue_rank = 1 THEN 20
    when overdue_rank=max_overdue_rank then 0 
    ELSE 20 - (overdue_rank - 1) *(20/(max_overdue_rank-1) )
  END  AS overdue_score,
  answer_score,
  answer_rank,
  answer_weight,
  CASE
    WHEN answer_rank = 1 THEN 30
    when answer_rank=max_answer_rank then 0 
    ELSE 30 - (answer_rank - 1) *(30/(max_answer_rank-1) ) 
    end new_answer_score,
    customer_cnt,
    all_answer_score,
    lave_customer_cn,
    lave_write_off_amount
from
     tmp_full_total a 
     left join tmp_max_rnk b on a.performance_region_name=b.performance_region_name and a.sale_month=b.sale_month
) a 
) a 
-- where performance_region_name='华东大区'
order by performance_region_name,
total_rank
;


-- ******************************************************************** 
-- @功能描述：销售红黑榜-管家销售额毛利额
-- @创建者： 彭承华 
-- @创建者日期：2025-02-17 12:50:36 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


-- 管家销售额毛利额
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_service_sale ;
-- create table desc  csx_analyse_tmp.csx_analyse_tmp_hr_service_sale as 
with tmp_sale_detail as 
    (select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        -- 这样判断主要是管家信息中没有前置仓业务，日配业务中包含前置仓
        if(a.business_type_code='4','1',a.business_type_code) new_business_type_code,
        -- business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        if(substr(first_business_sale_date,1,6)= substr(sdt,1,6), 1, 0) as new_customer_flag,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    left join 
    -- 关联商机新客
      (select customer_code,
              business_type_code,
              first_business_sale_date
       from csx_dws.csx_dws_crm_customer_business_active_di
        where sdt='current' 
      ) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= regexp_replace(trunc(add_months('${sdt_yes_date}',-1),'MM'),'-','')
        and sdt <= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        and (a.business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC 、4-前置仓
             or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209',
                                      '80946479','81102471','81254457','81119082','81149084',
                                      '81103064','81029025','81013168','81149084','81103064','81254457') 
                and a.business_type_code =4)
              
            )
          and sales_user_number not in ('81208614')  -- 202502 剔除汪平 81206921
         and a.customer_code not in ('234036','224656','247525','243799','244172','237768','127768','245029','248021','251856','251888','251812','251897','251895')
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        if(a.business_type_code='4','1',a.business_type_code),
        -- a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        if(substr(first_business_sale_date,1,6)= substr(sdt,1,6), 1, 0) 
    ),
    tmp_sale_detail_hr as 
    (select  sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_business_type_code,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        b.service_user_work_no,
        b.service_user_name,
        b.service_manager_user_position,
        b.service_user_work_no new_service_user_work_no,
        b.service_user_name new_service_user_name,
        b.service_manager_user_position new_service_manager_user_position,
        new_customer_flag,        
        sale_amt,
        profit
    from tmp_sale_detail a 
    left join 
    (select * from  csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info 
    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    )b on a.customer_code=b.customer_no and a.new_business_type_code=b.business_type_code
    where a.sales_user_position !='CUSTOMER_SERVICE_MANAGER'
        and b.service_manager_user_position='CUSTOMER_SERVICE_MANAGER'
   union all 
    select  sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_business_type_code,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        b.service_user_work_no,
        b.service_user_name,
        b.service_manager_user_position,
        a.sales_user_number new_service_user_work_no,
        a.sales_user_name as new_service_user_name,
        a.sales_user_position as new_service_manager_user_position,
        new_customer_flag,        
        sale_amt,
        profit
    from tmp_sale_detail a 
     left join 
    (select * from  csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info 
    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    )b on a.customer_code=b.customer_no and a.new_business_type_code=b.business_type_code
    where a.sales_user_position='CUSTOMER_SERVICE_MANAGER'
     and b.service_manager_user_position='CUSTOMER_SERVICE_MANAGER'
    )
    -- insert overwrite table csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_sale_detail partition(smt)
    select a.sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_business_type_code,
        case when a.new_business_type_code=1 then '日配业务'
            when a.new_business_type_code=2 then '福利业务'
            when a.new_business_type_code=6 then 'BBC'
            end  new_business_type_name,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        service_user_work_no,
        service_user_name,
        service_manager_user_position,
        new_service_user_work_no,
        new_service_user_name,
        new_service_manager_user_position,
        new_customer_flag,
        sale_amt,
        profit,
        sale_amt/c.cnt as avg_sale_amt,
        profit/c.cnt as avg_profit,
        c.cnt,
        current_timestamp(),
        sale_month
     from tmp_sale_detail_hr a
    left join 
    (select 
        customer_code,
        new_business_type_code,
        count(new_service_user_work_no) cnt
    from tmp_sale_detail_hr
    group by customer_code,
        new_business_type_code
    ) c on a.customer_code=c.customer_code and a.new_business_type_code=c.new_business_type_code
    ;
  


  -- ******************************************************************** 
-- @功能描述：销售红黑榜-服务管家逾期明细
-- @创建者： 彭承华 
-- @创建者日期：2025-02-17 21:35:01 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


with over_rate as 
(select substr(sdt,1,6) as sale_month,
    performance_region_name as region_name,
    performance_province_name as province_name,
    performance_city_name as city_group_name,
    customer_code, 
    customer_name,
    business_attribute_code,
    business_attribute_name as customer_attribute_code,
    credit_business_attribute_name,
    credit_business_attribute_code,
    channel_name,
    sales_employee_code,
    sales_employee_name,
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
                                      '81254457') 
      and a.channel_name in ('项目供应商','前置仓'))
      )
     and sales_employee_code not in ('81208614')
    and a.customer_code not in ('234036','224656','247525','243799','244172','237768','127768','245029','248021','251856','251888','251812','251897','251895')

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
    sales_employee_name ,
    business_attribute_code,
    credit_business_attribute_code
    
)
insert overwrite table csx_analyse.csx_analyse_fr_hr_red_black_service_manager_receivable_overdue partition(smt)
select  sale_month,
    region_name,
    a.province_name,
    city_group_name,
    a.customer_code, 
    b.customer_name,
    '' customer_attribute_code,
    -- credit_business_attribute_name,
    channel_name,
    sales_employee_code,
    sales_employee_name,
    c.user_position,
    b.service_user_work_no,
    b.service_user_name,
    b.service_manager_user_position,
    case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER'  then b.service_user_work_no
            when c.user_position='CUSTOMER_SERVICE_MANAGER' and coalesce( b.service_user_work_no ,'')!= coalesce(a.sales_employee_code,'')   then a.sales_employee_code 
            else '' end new_service_user_work_no,
        case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER' then b.service_user_name
            when c.user_position='CUSTOMER_SERVICE_MANAGER' and  coalesce( b.service_user_work_no ,'')!= coalesce(a.sales_employee_code,'')  then a.sales_employee_name
            else '' end new_service_user_name,
        case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER' then b.service_manager_user_position
            when  c.user_position='CUSTOMER_SERVICE_MANAGER' and  coalesce( b.service_user_work_no ,'')!= coalesce(a.sales_employee_code,'') then c.user_position
            else '' end new_service_manager_user_position,
    overdue_amount,
    receivable_amount,
    current_timestamp(),
    sale_month
from over_rate a 
left join 
 (select * from  csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info 
    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
   ) b on a.customer_code=b.customer_no and a.credit_business_attribute_code=b.attribute_code
left join 
 (select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    ) c on a.sales_employee_code=c.user_number
 where c.user_position !='CUSTOMER_SERVICE_MANAGER'
union all 
select  sale_month,
    region_name,
    a.province_name,
    city_group_name,
    a.customer_code, 
    b.customer_name,
    '' customer_attribute_code,
    -- credit_business_attribute_name,
    channel_name,
    sales_employee_code,
    sales_employee_name,
    c.user_position,
    b.service_user_work_no,
    b.service_user_name,
    b.service_manager_user_position,
    case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER'  then b.service_user_work_no
            when c.user_position='CUSTOMER_SERVICE_MANAGER' and coalesce( b.service_user_work_no ,'')!= coalesce(a.sales_employee_code,'')   then a.sales_employee_code 
            else '' end new_service_user_work_no,
        case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER' then b.service_user_name
            when c.user_position='CUSTOMER_SERVICE_MANAGER' and  coalesce( b.service_user_work_no ,'')!= coalesce(a.sales_employee_code,'')  then a.sales_employee_name
            else '' end new_service_user_name,
        case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER' then b.service_manager_user_position
            when  c.user_position='CUSTOMER_SERVICE_MANAGER' and  coalesce( b.service_user_work_no ,'')!= coalesce(a.sales_employee_code,'') then c.user_position
            else '' end new_service_manager_user_position,
    overdue_amount,
    receivable_amount,
    current_timestamp(),
    sale_month
from over_rate a 
left join 
 (select * from  csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info 
    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
   ) b on a.customer_code=b.customer_no and a.credit_business_attribute_code=b.attribute_code
left join 
 (select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    ) c on a.sales_employee_code=c.user_number
where c.user_position ='CUSTOMER_SERVICE_MANAGER'
;


