

-- 最后发送结果汇总为两张结果表的合集

-- 商机新客明细
select a.*,c.sales_user_number,c.sales_user_name,b.sale_amt
from
(
select * from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di 
where smonth in ('202307','202308','202309')
union all
select * from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
where smonth in  ('202307','202308','202309')
 )a
LEFT join
  (
     select *
     from csx_dim.csx_dim_crm_customer_info
     where sdt='current'
           and channel_code  in ('1','7','9')
  ) c  on a.customer_no=c.customer_code 
left join 
   (
     select 
              substr(sdt,1,6) smonth,
               customer_code,
               business_type_code,
                sum(sale_amt) as sale_amt
     from   csx_dws.csx_dws_sale_detail_di
     where  sdt>='20230701' and sdt<='20230930'
                and business_type_code in (1,2,6) and channel_code in ('1','7','9') 
     group by substr(sdt,1,6),
			 customer_code,
             business_type_code
             )b on a.customer_no=b.customer_code and a.business_type_code=b.business_type_code 
			   and a.smonth=b.smonth ;

-- ******************************************************************** 
-- @功能描述：有商机成交新客，根据商机签约核销第一次成交情况 
-- @创建者： 高雪芳 
-- @创建者日期：2022-11-11 14:27:52 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
/*核查基本问题：
1、先判断客户的业务类型是否属于项目供应商
2、客户建商机前是否有成交
3、BBC是否属于餐卡客户
涉及的表：csx_analyse.csx_analyse_sale_d_customer_business_number_di 商机新签
*/

insert overwrite table csx_analyse.csx_analyse_sale_d_customer_business_number_di partition (sdt)
select a.customer_code as customer_no,
  a.business_number,
  a.business_type_code,
  a.business_sign_time as sign_time,
  a.start_date,
  if(ook = 1 and b.customer_code is not null, '${i_sdate}', '99990101' ) as end_date,
  if(ook = 1 and b.customer_code is not null,  '已核销',  '未核销' ) as business_type,
  other_needs_code,
  '${i_sdate}' as sdt
from (
    select customer_code,
      business_number,
      business_type_code,
      business_sign_time,
      start_date,
      other_needs_code,
      row_number() over(partition by customer_code, business_type_code, other_needs_code order by business_sign_time  ) ook
    from (
        select customer_no as customer_code,
          business_number,
          business_type_code,
          from_unixtime(unix_timestamp(sign_time, 'yyyy-MM-dd HH:mm:ss')) as business_sign_time,
          start_date,
          other_needs_code
        from csx_analyse.csx_analyse_sale_d_customer_business_number_di
        where sdt = '${i_sdate1}'
          and start_date >= '20210801'
          and business_type = '未核销'
        union all
        select customer_code,
          business_number,
          cast(business_type_code as STRING) business_type_code,
          from_unixtime(unix_timestamp(business_sign_time, 'yyyy-MM-dd HH:mm:ss') ) business_sign_time,
          regexp_replace(substr(business_sign_time, 1, 10), '-', '') start_date,
          case
            when business_type_code = 6
            and other_needs_code = '1' then '餐卡'
            when business_type_code = 6
            and (other_needs_code <> '1' or other_needs_code is null  ) then '非餐卡' else '其他' end as other_needs_code
        from csx_dim.csx_dim_crm_business_info
        where sdt = 'current'
          and business_stage = 5
          and business_type_code in (1, 2, 6)
          and regexp_replace(substr(business_sign_time, 1, 10), '-', '') = '${i_sdate}'
      ) a
  ) a
  left join -- 销售表
  (
    select a.customer_code,
      a.business_type_code,
      if(
        a.business_type_code in (1, 2),
        '其他',
        b.credit_pay_type_name
      ) as credit_pay_type_name
    from csx_dws.csx_dws_sale_detail_di a
      left join (
        SELECT id,
          if(credit_pay_type_name = '餐卡', '餐卡', '非餐卡') as credit_pay_type_name
        from csx_dws.csx_dws_bbc_sale_detail_di
        where sdt = '${i_sdate}'
      ) b on a.id = b.id
    where a.sdt = '${i_sdate}'
      AND a.order_channel_code <> 4
      and a.business_type_code in (1, 2, 6)
    group by customer_code,
      business_type_code,
      if(
        a.business_type_code in (1, 2),
        '其他',
        b.credit_pay_type_name
      )
  ) b on a.customer_code = b.customer_code
  and a.business_type_code = b.business_type_code
  and a.other_needs_code = b.credit_pay_type_name;



-- ******************************************************************** 
-- @功能描述：商机新客明细 处理后的客户商机唯一
-- @创建者： 高雪芳 
-- @创建者日期：2022-11-11 15:28:26 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

-- 商机新签客户明细
insert overwrite table csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di partition (smonth)
select c.performance_province_name sales_province_name,
  c.performance_city_name city_group_name,
  c.province_manager_user_number fourth_supervisor_work_no,
  c.province_manager_user_name fourth_supervisor_name,
  c.city_manager_user_number third_supervisor_work_no,
  c.city_manager_user_name third_supervisor_name,
  c.sales_manager_user_number second_supervisor_work_no,
  c.sales_manager_user_name second_supervisor_name,
  c.supervisor_user_number first_supervisor_work_no,
  c.supervisor_user_name first_supervisor_name,
  a.business_type_code,
  a.customer_code as customer_no,
  c.customer_name,
  current_date as update_date,
  case
    when a.business_type_code = '1' then '日配业务'
    when a.business_type_code = '2' then '福利业务'
    when a.business_type_code = '6' then 'BBC'
  end as business_type_name,
  a.end_date,
  a.smonth
from (
    SELECT substr(sdt, 1, 6) smonth,
      business_type_code,
      customer_no as customer_code,
      min(end_date) end_date
    from csx_analyse.csx_analyse_sale_d_customer_business_number_di
    where sdt >= regexp_replace(trunc('${i_sdate}', 'MM'), '-', '')
      and sdt <= regexp_replace('${i_sdate}', '-', '')
      and business_type = '已核销'
      and other_needs_code in ('非餐卡', '其他')  -- 
      and business_type_code in ('2', '6')       -- 福利、BBC非餐卡
    group by substr(sdt, 1, 6),
      business_type_code,
      customer_no
    union all
    select substr(end_date, 1, 6) as smonth,
      a.business_type_code,
      a.customer_code,
      end_date
    from (
        SELECT business_type_code,
          customer_no as customer_code,
          min(end_date) end_date
        from csx_analyse.csx_analyse_sale_d_customer_business_number_di
        where sdt >= regexp_replace(trunc('${i_sdate}', 'YYYY'), '-', '')
          and sdt <= regexp_replace('${i_sdate}', '-', '')
          and business_type = '已核销'
          and other_needs_code in ('餐卡', '其他')
          and business_type_code in ('1', '6')        -- 日配、BBC餐卡业务
        group by business_type_code,
          customer_no
      ) a
      left join 
      -- 截止上个月度核销的新客已核销
      (
        select customer_no as customer_code,
          business_type_code
        from csx_analyse.csx_analyse_temp_partner_cust_di
         -- sdt='20230331' 原来
        where sdt = regexp_replace(last_day(add_months('${i_sdate}', -1)), '-', '')
          and business_type = '已核销'
        group by customer_no,
          business_type_code
      ) b on a.customer_code = b.customer_code
      and a.business_type_code = b.business_type_code
    where b.customer_code is null
      and substr(end_date, 1, 6) = substr(regexp_replace('${i_sdate}', '-', ''), 1, 6)
  ) a
  LEFT join (
    select *
    from csx_dim.csx_dim_crm_customer_info
    where sdt = regexp_replace('${i_sdate}', '-', '')
      and channel_code in ('1', '7', '9')
  ) c on a.customer_code = c.customer_code
group by c.performance_province_name,
  c.performance_city_name,
  c.province_manager_user_number,
  c.province_manager_user_name,
  c.city_manager_user_number,
  c.city_manager_user_name,
  c.sales_manager_user_number,
  c.sales_manager_user_name,
  c.supervisor_user_number,
  c.supervisor_user_name,
  a.business_type_code,
  a.smonth,
  a.customer_code,
  c.customer_name,
  case
    when a.business_type_code = '1' then '日配业务'
    when a.business_type_code = '2' then '福利业务'
    when a.business_type_code = '6' then 'BBC'
  end,
  a.end_date;




-- ******************************************************************** 
-- @功能描述：csx_analyse_temp_partner_cust_di,日配年至今，BBC非餐卡业务年至今
-- @创建者： 高雪芳 
-- @创建者日期：2023-02-08 16:10:43 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
with csx_analyse_temp_partner_cust_di01 as (
  select customer_no as customer_code,
    business_number,
    business_type_code,
    sign_time as business_sign_time,
    sign_date
  from csx_analyse.csx_analyse_temp_partner_cust_di
  where sdt = regexp_replace(date_sub(trunc('${i_sdate}', 'YYYY'), 1), '-', '')
    and business_type = '未核销'
  union all
  select a.customer_code,
    business_number,
    cast(business_type_code as string) business_type_code,
    cast(business_sign_time as string) business_sign_time,
    regexp_replace(substr(business_sign_time, 1, 10), '-', '')
  FROM csx_dim.csx_dim_crm_business_info a
    left join (
      select customer_id,
        customer_code
      from csx_dim.csx_dim_crm_customer_info
      where sdt = 'current'
        and customer_code <> ''
    ) c on a.customer_id = c.customer_id
  WHERE sdt = 'current'
    and business_stage = 5
    and (business_type_code = 1 or ( business_type_code = 6 and other_needs_code = '1' ))
    AND substr(business_sign_time, 1, 10) >= trunc('${i_sdate}', 'YYYY')
    AND substr(business_sign_time, 1, 10) <= '${i_sdate}'
)
insert overwrite table csx_analyse.csx_analyse_temp_partner_cust_di partition (sdt)
select a.customer_code,
  a.business_number,
  a.business_type_code,
  a.business_sign_time,
  a.sign_date,
  if(b.customer_code is not null,regexp_replace('${i_sdate}', '-', ''),'99990101') as end_date,
  if(b.customer_code is not null, '已核销', '未核销') as business_type,
  regexp_replace('${i_sdate}', '-', '') as sdt
from csx_analyse_temp_partner_cust_di01 a
  left join (
    SELECT a.sdt,
      a.customer_code,
      a.business_type_code
    from (
        select *
        from csx_dws.csx_dws_sale_detail_di
        where sdt >= regexp_replace(trunc('${i_sdate}', 'YYYY'), '-', '')
          and sdt <= regexp_replace('${i_sdate}', '-', '')
          AND order_channel_code <> 4
          and channel_code = '7'
      ) a
      join (
        SELECT id,
          credit_pay_type_name
        from csx_dws.csx_dws_bbc_sale_detail_di
        where sdt >= regexp_replace(trunc('${i_sdate}', 'YYYY'), '-', '')
          and sdt <= regexp_replace('${i_sdate}', '-', '')
          and credit_pay_type_name = '餐卡'
      ) b on a.id = b.id
    group by a.sdt,
      a.customer_code,
      a.business_type_code
    union all
    select sdt,
      customer_code,
      business_type_code
    from csx_dws.csx_dws_sale_detail_di
    where sdt >= regexp_replace(trunc('${i_sdate}', 'YYYY'), '-', '')
      and sdt <= regexp_replace('${i_sdate}', '-', '')
      AND order_channel_code <> 4
      and business_type_code = '1'
    group by sdt,
      customer_code,
      business_type_code
  ) b on a.customer_code = b.customer_code
  and a.business_type_code = b.business_type_code
  and a.sign_date <= b.sdt
group by a.customer_code,
  a.business_number,
  a.business_type_code,
  a.business_sign_time,
  a.sign_date,
  if(b.customer_code is not null, regexp_replace('${i_sdate}', '-', ''), '99990101' ),
  if(b.customer_code is not null, '已核销', '未核销');