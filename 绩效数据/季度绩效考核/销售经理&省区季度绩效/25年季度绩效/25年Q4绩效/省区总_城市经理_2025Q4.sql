--季度绩效数据优化：
1、客服经理：两个管家共同维护一个客户，他们的上级均为同一名客服经理，目前这部分上级没有取到，逻辑需调整（取其中一个人的上级即可）
2、客服经理：回款率明细增加“城市”字段，到城市维度，回款率包含回款目标为0的，不过滤
3、省区采购：客诉数据剔除 “三级客诉”，不考核这部分
4、断约方案口径：未履约90天算断约；1假设客户A 一季度是A类，二季度因为已经断约但是未超90天，导致二季度就取不到数，但是因为二季度实际已经断约，导致二季度销售额已经达不到A类客户，三季度核算，这个客户依然不会取到。所以调整断约口径为：大于31天未履约算断约
5、三季度销售员按照每个月的关联，主要三季度组织架构调整


-- 当期客户期初期末对外B端应收账款 统一取中台
-- 取SAP应收表 输出表：csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
-- 中台核销 输出表：csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
 select
         sdt,
        --  sdt,
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
		     customer_name,
         sum(receivable_amount)  receivable_amount -- 应收账款
       from 
        --  csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di   中台核销
       csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
		  -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20251231'  or sdt='20250930')  
       and channel_name !='商超'
        --  and channel_name  in ('大客户','项目供应商','虚拟销售员','业务代理','','其他','前置仓') -- and province_name='福建省'
       group by  sdt,
         channel_name,
         performance_province_name ,
         performance_city_name ,
         customer_code,
         customer_name;
         



-- 当期客户期初期末对外B端应收账款 
-- 取SAP应收表 输出表：csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
  select
         sdt,
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
		     customer_name,
         sum(receivable_amount)  receivable_amount -- 应收账款
       from 
         csx_dws.csx_dws_sap_customer_credit_settle_detail_di 
      --  csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
		  -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20240630'  or sdt='20251231')  
         and channel_code  in ('1','7','9','13') -- and province_name='福建省'
       group by  sdt,
         channel_name,
         performance_province_name ,
         performance_city_name ,
         customer_code,
         customer_name;


 select * from csx_analyse_tmp.csx_analyse_tmp_terminate_info 
 
-- A类客户按照等级 省区总
drop table csx_analyse_tmp.csx_analyse_tmp_terminate_info 
create table csx_analyse_tmp.csx_analyse_tmp_terminate_info as 
with   tmp_customer_level as (select
  region_name,
  province_name,
  city_group_name,
  customer_no,
  customer_name,
  first_category_name,
  second_category_name,
  last_order_date,			-- 最后一次下单时间 需要注意这个是全业务的，下季度要调整20250731
  work_no,
  sales_name,
  sales_value,
  profit,
  profit_rate,
  customer_large_level,
  customer_small_level,
  customer_level_tag,
  first_order_date,
  quarter,
  '日配' business_attribute_name
from
    csx_analyse.csx_analyse_report_sale_customer_level_qf
where
  tag = '2'
  and quarter = '20253'  -- 取上一季度
  and customer_large_level in ('A','B')
  ),
tmp_sale_detail as   ( 
    SELECT
        customer_code,
        MAX(sdt) AS max_sdt,
        DATEDIFF('2025-12-31', FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(sdt), 'yyyyMMdd'), 'yyyy-MM-dd')) AS diff_days
    FROM csx_dws.csx_dws_sale_detail_di
    WHERE sdt >= '20251001' AND sdt <= '20251231'
      AND business_type_code = 1
      AND order_channel_detail_code not in ('25','27')  -- --剔除调价返利
      and refund_order_flag !=1 -- 剔除退货
    GROUP BY customer_code
),
tmp_terminate_info as 
(select customer_code,
    terminate_date,
    row_number()over(partition by customer_code order by terminate_date desc ) as rn from 
(select customer_code,terminate_date from csx_dim.csx_dim_crm_terminate_customer  
    where sdt='current' 
        and business_attribute_code=1
        and status=2
 union all 
 select customer_code,
    terminate_date 
 from csx_dim.csx_dim_crm_terminate_customer_attribute  
    where sdt='current'  
        and business_attribute_code=1
        and approval_status=2
) a 
)
 
select  
    a.region_name,
    a.province_name,
    a.city_group_name,
    a.customer_no,
    a.customer_name,
    a.first_category_name,
    a.second_category_name,
    a.last_order_date,			-- 最后一次下单时间 需要注意这个是全业务的，下季度要调整20250731
    a.work_no,
    a.sales_name,
    a.sales_value,
    a.profit,
    a.profit_rate,
    a.customer_large_level,
    a.customer_small_level,
    a.customer_level_tag,
    a.quarter,
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
    b.max_sdt,
    d.terminate_date,
    IF(d.customer_code IS not  NULL  , '断约客户', NULL) AS typeder
from tmp_customer_level a 
left join tmp_sale_detail b on a.customer_no=b.customer_code
left join 
(select * from tmp_terminate_info
    where rn=1 
        and terminate_date between '20251001' and '20251231'
) d on a.customer_no=d.customer_code
LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c on a.customer_no = c.customer_no
    and a.business_attribute_name = c.attribute_name
 
;


select region_name,
    province_name,
    city_group_name,
    a.province_manager_user_number,
    a.province_manager_user_name,
    a.province_manager_position,
    a.city_manager_user_number,
    a.city_manager_user_name,
    a.city_manager_position,
    a.sales_manager_user_number,
    a.sales_manager_user_name,
    a.sales_manager_position,
    a.supervisor_user_number,
    a.supervisor_user_name,
    a.supervisor_position,
    count(customer_no) all_cn,
  count(
    case
      when typeder = '断约客户' then customer_no
    end
  ) cust_cn
  from csx_analyse_tmp.csx_analyse_tmp_terminate_info   a 
group by  region_name,
    province_name,
    city_group_name,
    a.province_manager_user_number,
    a.province_manager_user_name,
    a.province_manager_position,
    a.city_manager_user_number,
    a.city_manager_user_name,
    a.city_manager_position,
    a.sales_manager_user_number,
    a.sales_manager_user_name,
    a.sales_manager_position,
    a.supervisor_user_number,
    a.supervisor_user_name,
    a.supervisor_position
;
 

--  新签合同金额汇总
select  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.province_manager_user_number,
  a.province_manager_user_name,
  a.province_manager_position,
  a.city_manager_user_number,
  a.city_manager_user_name,
  a.city_manager_position,
  a.sales_manager_user_number,
  a.sales_manager_user_name,
  a.sales_manager_position,
  a.supervisor_user_number,
  a.supervisor_user_name,
  a.supervisor_position,
        sum(contract_amount) contract_amount,
        sum(year_contract_amount) year_contract_amount
    from csx_analyse_tmp.csx_analyse_tmp_quarterly_business_ht a
    group by a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.province_manager_user_number,
  a.province_manager_user_name,
  a.province_manager_position,
  a.city_manager_user_number,
  a.city_manager_user_name,
  a.city_manager_position,
  a.sales_manager_user_number,
  a.sales_manager_user_name,
  a.sales_manager_position,
  a.supervisor_user_number,
  a.supervisor_user_name,
  a.supervisor_position
        
;


--  新签合同金额明细
select * from csx_analyse_tmp.csx_analyse_tmp_quarterly_business_ht 

drop table csx_analyse_tmp.csx_analyse_tmp_quarterly_business_ht 
create table csx_analyse_tmp.csx_analyse_tmp_quarterly_business_ht as --  新签合同金额明细
SELECT
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
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
  a.owner_user_number,
  a.owner_user_name,
  --       a.cust_flag,
  a.business_number,
  --		a.business_sign_time,
  a.business_sign_month,
  --	a.business_number,
  a.customer_code,
  a.customer_name,
  a.contract_number,
  --        a.business_type_code,
  a.business_type_name,
  a.business_sign_date,
  --			a.business_sign_date_2,
  --			a.first_business_sign_date,
  -- 当htbh不为null时，但是会出现合同金额等于0，取商机的金额及月份
  if(
    b.htbh is not null
    and coalesce(b.htjey, 0) <> 0,
    yue,
    regexp_extract(a.contract_cycle_desc, '[0-9]+', 0)
  ) yue,
  if(
    if(
      b.htbh is not null
      and coalesce(b.htjey, 0) <> 0,
      yue,
      regexp_extract(a.contract_cycle_desc, '[0-9]+', 0)
    ) > 12,
    if(
      b.htbh is not null
      and coalesce(b.htjey, 0) <> 0,
      yue,
      regexp_extract(a.contract_cycle_desc, '[0-9]+', 0)
    ) / 12,
    1
  ) zhishu,
  if(
    b.htbh is not null
    and coalesce(b.htjey, 0) <> 0,
    b.htjey / 10000,
    a.estimate_contract_amount
  ) contract_amount,
  if(
    b.htbh is not null
    and coalesce(b.htjey, 0) <> 0,
    b.htjey / 10000,
    a.estimate_contract_amount
  ) / if(
    if(
      b.htbh is not null
      and coalesce(b.htjey, 0) <> 0,
      yue,
      regexp_extract(a.contract_cycle_desc, '[0-9]+', 0)
    ) > 12,
    if(
      b.htbh is not null
      and coalesce(b.htjey, 0) <> 0,
      yue,
      regexp_extract(a.contract_cycle_desc, '[0-9]+', 0)
    ) / 12,
    1
  ) as yh_amt,
  b.htbh,
  if(b.htbh is not null, 1, 0) as is_ht
from
  (
    select
      if(
        to_date(business_sign_time) = to_date(first_business_sign_time),
        '新商机',
        '老商机'
      ) cust_flag,
      business_sign_time,
      regexp_replace(substr(to_date(business_sign_time), 1, 7), '-', '') business_sign_month,
      business_number,
      customer_id,
      customer_code,
      customer_name,
      owner_user_number,
      owner_user_name,
      contract_number,
      performance_region_name,
      performance_province_name,
      performance_city_name,
      business_type_code,
      business_type_name,
      business_attribute_name,
      contract_cycle_desc,
      estimate_contract_amount,
      regexp_replace(to_date(business_sign_time), '-', '') business_sign_date,
      to_date(business_sign_time) as business_sign_date_2,
      regexp_replace(to_date(first_business_sign_time), '-', '') first_business_sign_date,
      business_stage
    from
      csx_dim.csx_dim_crm_business_info
    where
      sdt = 'current'
      and business_attribute_code in (1, 2, 5) --  商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
      and status = 1 --  是否有效 0.无效 1.有效 (status=0,'停止跟进')
      and business_stage = 5 -- 			and contract_type = 2
      and (
        belong_approval_status = 2
        or belong_center_flow_id = -1
      )
      and to_date(business_sign_time) >= '2025-10-01'
      and to_date(business_sign_time) <= '2025-12-31'
      and performance_province_name != '平台-B'
  ) a
  left join -- 可以取最新日期关联合同号
  (
    select
      t1.htbh,
      --  合同编码
      (
        case
          when length(trim(t1.customernumber)) > 0 then trim(t1.customernumber)
          else t3.customer_code
        end
      ) as customer_no,
      --  客户编码
      htjey,
      --  合同金额（元）
      htqsrq,
      --  合同起始日期
      htzzrq,
      --  合同终止日期
      ROUND(datediff(htzzrq, htqsrq) / 30.5, 0) yue
    from
      (
        select
          *
        from
          csx_ods.csx_ods_ecology_154_uf_xshttzv2_df
        where
          sdt = regexp_replace(date_sub(current_date, 1), '-', '')
          and length(htbh) > 0
      ) t1
      left join (
        select
          *
        from
          csx_ods.csx_ods_ecology_154_uf_xshttzv2_dt4_df
        where
          sdt = regexp_replace(date_sub(current_date, 1), '-', '')
      ) t2 on t1.id = t2.mainid
      left join (
        select
          *
        from
          csx_dim.csx_dim_crm_customer_info
        where
          sdt = regexp_replace(date_sub(current_date, 1), '-', '')
      ) t3 on t2.khmc = t3.customer_name
  ) b on b.customer_no = a.customer_code  and b.htbh = a.contract_number
  LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c on a.customer_code = c.customer_no
  and a.business_attribute_name = c.attribute_name;



-- 商机汇总
select
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.province_manager_user_number,
  a.province_manager_user_name,
  a.province_manager_position,
  a.city_manager_user_number,
  a.city_manager_user_name,
  a.city_manager_position,
  a.sales_manager_user_number,
  a.sales_manager_user_name,
  a.sales_manager_position,
  a.supervisor_user_number,
  a.supervisor_user_name,
  a.supervisor_position,
  a.business_type_name,
  count(a.customer_no) as cust_cn
from
  csx_analyse_tmp.csx_analyse_tmp_quarterly_business_new_customer a
 
group by
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.province_manager_user_number,
  a.province_manager_user_name,
  a.province_manager_position,
  a.city_manager_user_number,
  a.city_manager_user_name,
  a.city_manager_position,
  a.sales_manager_user_number,
  a.sales_manager_user_name,
  a.sales_manager_position,
  a.supervisor_user_number,
  a.supervisor_user_name,
  a.supervisor_position,
  a.business_type_name
  ;  
  select * from csx_analyse_tmp.csx_analyse_tmp_quarterly_business_new_customer
-- 商机明细
drop table csx_analyse_tmp.csx_analyse_tmp_quarterly_business_new_customer
create table  csx_analyse_tmp.csx_analyse_tmp_quarterly_business_new_customer as 
select
  d.performance_region_name,
  d.performance_province_name,
  d.performance_city_name,
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
  a.business_type_code,
  a.business_type_name,
  a.end_date,
  a.smonth,
  a.customer_no,
  d.customer_name,
  sale_amt
from
  (
    select
      *,
      CASE
        WHEN business_type_code in ('2', '10') then '福利'
        when business_type_code = '1' then '日配'
        when business_type_name = '批发内购' then '内购'
        when business_type_name = '省区大宗' then '大宗贸易'
        else business_type_name
      end new_business_type_name
    from
      csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di
    where
      smonth in ('202510', '202511', '202512')
    union all
    select
      *,
      CASE
        WHEN business_type_code in ('2', '10') then '福利'
        when business_type_code = '1' then '日配'
        when business_type_name = '批发内购' then '内购'
        when business_type_name = '省区大宗' then '大宗贸易'
        else business_type_name
      end new_business_type_name
    from
      csx_analyse.csx_analyse_sale_d_customer_new_about_di
    where
      smonth in ('202510', '202511', '202512')
  ) a
  LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c on a.customer_no = c.customer_no
  and a.new_business_type_name = c.attribute_name
  left join (
    select
      substr(sdt, 1, 6) smonth,
      customer_code,
      business_type_code,
      sum(sale_amt) as sale_amt
    from
      csx_dws.csx_dws_sale_detail_di
    where
      sdt >= '20251001'
      and sdt <= '20251231'
      and business_type_code in (1, 2, 6, 10)
      and partner_type_code not in (1, 3) -- 0-非合伙人  1-城市服务商 2-联营合伙人  3-城市服务商2.0
    group by
      substr(sdt, 1, 6),
      customer_code,
      business_type_code
  ) b on a.customer_no = b.customer_code
  and a.business_type_code = b.business_type_code
  and a.smonth = b.smonth
  LEFT join (
    select
      customer_code,
      performance_region_name,
      performance_province_name,
      performance_city_name,
      customer_name
    from
      csx_dim.csx_dim_crm_customer_info
    where
      sdt = 'current'
      and channel_code != 2
  ) d on a.customer_no = d.customer_code
where
  sale_amt is not null;

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
      and customer_code in ('173961')
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



-- A类客户 202410 
-- A类客户按照等级明细


-- A类客户按照等级
with tmp_level_a as 
(select
  province_code,
  customer_no,
  city_group_name
from
  csx_analyse.csx_analyse_report_sale_customer_level_mf
where
  month in ( '202407','202408','202409')
  AND customer_large_level = 'A'
  AND TAG = 1
group by 
 province_code,
  customer_no,
  city_group_name
  )
  
SELECT
	b.province_name,
	b.city_group_name,
	a.customer_no,
	b.customer_name,
  coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
  coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
  coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
  coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
  if(c.customer_code is null ,'断约客户',null) typeder  
from  tmp_level_a a
left join (select 
              distinct customer_code
            from   csx_dws.csx_dws_sale_detail_di
            where sdt >='20250401'  and sdt <= '20250930'
			and business_type_code='1' -- and channel_code in ('1','7','9') 
			AND  order_channel_code<>4
            )c	on  a.customer_no=c.customer_code 					
join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20250930')
		) b on a.customer_no=b.customer_no
;



with tmp_level_a as 
(select
  province_code,
  customer_no,
  city_group_name
from
  csx_analyse.csx_analyse_report_sale_customer_level_mf
where
  month in ( '202407','202408','202409')
  AND customer_large_level = 'A'
  AND TAG = 1
group by 
 province_code,
  customer_no,
  city_group_name
  )
  SELECT
	   c.performance_province_name      sales_province_name,
       c.performance_city_name     city_group_name,
       c.province_manager_user_number fourth_supervisor_work_no,
       c.province_manager_user_name fourth_supervisor_name,
       c.city_manager_user_number third_supervisor_work_no,
       c.city_manager_user_name third_supervisor_name, 
       c.sales_manager_user_number second_supervisor_work_no,
       c.sales_manager_user_name second_supervisor_name, 
       c.supervisor_user_number first_supervisor_work_no,
       c.supervisor_user_name first_supervisor_name, 
	   a.customer_no,
	   c.customer_name,
       if(b.customer_code is null ,'断约客户',null) typeder  ,
  

       last_business_sale_date
from  tmp_level_a a
left join (select 
              distinct customer_code
            from   csx_dws.csx_dws_sale_detail_di
            where sdt >='20250401'  and sdt <= '20250930'
			and business_type_code='1' -- and channel_code in ('1','7','9') 
			AND  order_channel_code<>4
            )b	on  a.customer_no=b.customer_code 					
left join   (
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= '20250930'
          --  and channel_code  in ('1','7','9')
        ) c on a.customer_no=c.customer_code 
left join 
(select customer_code, 
    last_business_sale_date
 from  csx_dws.csx_dws_crm_customer_business_active_di
    where sdt='20241007' 
    and business_type_code=1)d on a.customer_no=d.customer_code
;

select * from csx_analyse_tmp.csx_analyse_tmp_xiaoshou_ratio

;

-- A类客户占比汇总

-- A类客户按照等级
with tmp_level_a as 
(select
  province_code,
  customer_no,
  city_group_name
from
  csx_analyse.csx_analyse_report_sale_customer_level_mf
where
  month in ( '202407','202408','202409')
  AND customer_large_level = 'A'
  AND TAG = 1
group by 
 province_code,
  customer_no,
  city_group_name
  )
 select  sales_province_name,
       city_group_name,
       fourth_supervisor_work_no,
       fourth_supervisor_name,
       third_supervisor_work_no,
       third_supervisor_name, 
       second_supervisor_work_no,
       second_supervisor_name, 
       first_supervisor_work_no,
       first_supervisor_name, 
	   count(customer_no) all_cn,
	   count(case when typeder='断约客户' then  customer_no end ) cust_cn
from

 ( SELECT
	   c.performance_province_name      sales_province_name,
       c.performance_city_name     city_group_name,
       c.province_manager_user_number fourth_supervisor_work_no,
       c.province_manager_user_name fourth_supervisor_name,
       c.city_manager_user_number third_supervisor_work_no,
       c.city_manager_user_name third_supervisor_name, 
       c.sales_manager_user_number second_supervisor_work_no,
       c.sales_manager_user_name second_supervisor_name, 
       c.supervisor_user_number first_supervisor_work_no,
       c.supervisor_user_name first_supervisor_name, 
	   a.customer_no,
	   c.customer_name,
       if(b.customer_code is null ,'断约客户',null) typeder  ,
       last_business_sale_date
from  tmp_level_a a
left join (select 
              distinct customer_code
            from   csx_dws.csx_dws_sale_detail_di
            where sdt >='20250401'  and sdt <= '20250930'
			and business_type_code='1' -- and channel_code in ('1','7','9') 
			AND  order_channel_code<>4
            )b	on  a.customer_no=b.customer_code 					
left join   (
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= '20250930'
          --  and channel_code  in ('1','7','9')
        ) c on a.customer_no=c.customer_code 
left join 
(select customer_code, 
    last_business_sale_date
 from  csx_dws.csx_dws_crm_customer_business_active_di
    where sdt='20241007' 
    and business_type_code=1)d on a.customer_no=d.customer_code
)a 
group by sales_province_name,
       city_group_name,
       fourth_supervisor_work_no,
       fourth_supervisor_name,
       third_supervisor_work_no,
       third_supervisor_name, 
       second_supervisor_work_no,
       second_supervisor_name, 
       first_supervisor_work_no,
       first_supervisor_name
       ;
;
