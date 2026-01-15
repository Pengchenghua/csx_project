-- =============试配(75%)的商机==========

base
with tmp_business_info as (
select 
	substr(business_sign_time,1,7) month, 
    customer_code,
	customer_name,
	business_number,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	first_category_code,
	first_category_name,
	second_category_code,
	second_category_name,
	third_category_code,
	third_category_name,
	business_attribute_code,
	business_attribute_name,
	estimate_contract_amount,
	to_date(first_sign_time) first_sign_date,
	case when substr(first_sign_time,1,7) = substr(business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark,
	to_date(business_sign_time) business_sign_date,
	to_date(first_business_sign_time) first_business_sign_date,
	contract_cycle_desc,
	case 
	when contract_cycle_desc in('小于1个月') then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') <=12 then estimate_contract_amount
	when regexp_replace(contract_cycle_desc,'个月','') >12 then estimate_contract_amount/regexp_replace(contract_cycle_desc,'个月','')*12
	else estimate_contract_amount end estimate_contract_amount_nh,
	contract_number,
	owner_user_id, --归属人id
	owner_user_number,
	owner_user_name,
	owner_user_position
from csx_dim.csx_dim_crm_business_info	
where 
	sdt='current' 
	and contract_type = 1 
	and business_stage = 5
	and business_attribute_code in ('1', '2', '5')
	and shipper_code = 'YHCSX'
),

-- 商机阶段转化日志信息
csx_tmp_crm_business_number_conversion_log as (
  select
    business_number,
    customer_id,
    before_data,
    after_data,
    round(
      (
        unix_timestamp(
          lag(create_time, 1, current_timestamp()) over(
            partition by business_number
            order by
              create_time desc
          )
        ) - unix_timestamp(create_time)
      ) / 86400,
      2
    ) as business_conversion_days,
    is_repair,
	regexp_replace(substr(create_time,1,10),'-','') create_date
  from
    (
      select
        row_number() over(
          partition by coalesce(a.business_number, b.business_number),
          before_data
          order by
            a.create_time desc
        ) as rank,
        coalesce(a.business_number, b.business_number) as business_number,
        a.customer_id,
        before_data,
        after_data,
        a.create_time,
        if(a.business_number is null, 1, 0) as is_repair
      from
        (
          select
            business_number,
            -- 期初数据商机号为null
            customer_id,
            before_data,
            after_data,
            create_time
          from
            csx_ods.csx_ods_csx_crm_prod_operate_log_df
          where
            sdt = ${s_yesterday}
            and operate_type = '01'
            and cast(after_data as int) - cast(before_data as int) = 1
            and shipper_code = 'YHCSX'
        ) a
        left join (
          -- 下面逻辑是来给补录数据补商机号的
          select
            t1.*
          from
            (
              select
                business_number,
                customer_id,
                create_time
              from
                csx_dim.csx_dim_crm_business_info
              where
                sdt = 'current'
                and create_time < '2021-09-18'
                and shipper_code = 'YHCSX'
            ) t1
            left join (
              select
                distinct business_number
              from
                csx_ods.csx_ods_csx_crm_prod_operate_log_df
              where
                sdt = ${s_yesterday}
                and operate_type = '01'
                and business_number is not null
                and shipper_code = 'YHCSX'
            ) t2 on t1.business_number = t2.business_number
          where
            t2.business_number is null
        ) b on a.customer_id = b.customer_id
    ) tmp
  where
    business_number is not null
    and rank = 1
),	
-- 4变5的时间
csx_tmp_crm_business_4_5 as (
      select
        business_number,
        customer_id,
        before_data,
        after_data,
        is_repair,
		create_date
      from
        csx_tmp_crm_business_number_conversion_log
      where
        before_data = 4
        and after_data = 5
),

tmp_sales as (
-- 销售数据
  select
    a.sdt,
    a.customer_code,
    b.business_type_convert_attribute_code,
    b.business_type_convert_attribute_name,
    sum(sale_amt)/10000 as sale_amt
  from
    (
      select
        sdt,
        customer_code,
        business_type_code,
        sum(sale_amt) as sale_amt
      from
        csx_dws.csx_dws_sale_detail_di
      where 
        sdt>='20240101' and shipper_code='YHCSX' 
      group by
        sdt,
        customer_code,
        business_type_code
    ) a
    join csx_dim.csx_dim_sale_business_type_change_business_attribute_mapping b on a.business_type_code = b.business_type_code
  group by
    sdt,
    customer_code,
    business_type_convert_attribute_code,
    business_type_convert_attribute_name 
)

select 
	a.month, 
  a.customer_code,
	a.customer_name,
	a.business_number,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.first_category_code,
	a.first_category_name,
	a.second_category_code,
	a.second_category_name,
	a.third_category_code,
	a.third_category_name,
	a.business_attribute_code,
	a.business_attribute_name,
	a.estimate_contract_amount,
	a.first_sign_date,
	a.new_or_old_customer_mark,
	a.business_sign_date,
	a.first_business_sign_date,
	a.contract_cycle_desc,
	a.estimate_contract_amount_nh,
	a.contract_number,
	a.create_date,
  max(b.sdt) sale_date,
  coalesce(sum(b.sale_amt), 0) as sale_amt
from 
(select a.*,b.create_date
 from tmp_business_info a
 left join csx_tmp_crm_business_4_5 b on a.business_number=b.business_number
)a
left join tmp_sales b on a.customer_code = b.customer_code
	and a.business_attribute_code = b.business_type_convert_attribute_code
	and if(b.sdt>=a.create_date,true,false)
group by 	
    a.month, 
    a.customer_code,
	a.customer_name,
	a.business_number,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.first_category_code,
	a.first_category_name,
	a.second_category_code,
	a.second_category_name,
	a.third_category_code,
	a.third_category_name,
	a.business_attribute_code,
	a.business_attribute_name,
	a.estimate_contract_amount,
	a.first_sign_date,
	a.new_or_old_customer_mark,
	a.business_sign_date,
	a.first_business_sign_date,
	a.contract_cycle_desc,
	a.estimate_contract_amount_nh,
	a.contract_number,
	a.create_date;