-- 销售经理季度绩效考核

--B端销售汇总
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
  sum(sale_amt) sale_amt,
  sum(excluding_tax_profit) / sum(excluding_tax_sales) as excluding_tax_profit_rate,
  sum(profit) profit,
  sum(excluding_tax_sales) excluding_tax_sales,
  sum(excluding_tax_profit) excluding_tax_profit,
  sum(partner_sale_amt) partner_sale_amt,
  sum(partner_profit) partner_profit,
  sum(partner_excluding_tax_sales) partner_excluding_tax_sales,
  sum(excluding_tax_profit) partner_excluding_tax_profit,
  sum(if(new_type_flag=1,sale_amt,0)) bbc_card_sale,
  sum(if(new_type_flag=1,profit,0)) bbc_card_profit
from
  csx_analyse_tmp.csx_analyse_tmp_sales_sale_detail a
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
  a.supervisor_position;
-- drop table  csx_analyse_tmp.csx_analyse_tmp_sales_sale_detail ;
create table csx_analyse_tmp.csx_analyse_tmp_sales_sale_detail as 
-- B端销售明细
with tmp_sale_detail as 
 (
    select 
      '其他' type_flag,
      customer_code,
      substr(sdt, 1, 6) smonth,
      business_type_name,
      CASE
        WHEN business_type_code in ('2', '10') then '福利'
        when business_type_code = '1' then '日配'
        when business_type_name = '批发内购' then '内购'
        when business_type_name = '省区大宗' then '大宗贸易'
        else business_type_name end  new_business_type_name,
      partner_type_code,
      sum(sale_amt) as sale_amt,
      sum(profit) profit,
      sum(profit_no_tax) excluding_tax_profit,
      sum(sale_amt_no_tax) excluding_tax_sales
    from csx_dws.csx_dws_sale_detail_di
    where sdt >= '20251001'
      and sdt <= '20251231' -- and customer_code not in ('120459','121206')
      and (
        business_type_code in ('1', '2', '6', '10')
        or inventory_dc_code in (
          'WD75',
          'WD76',
          'WD77',
          'WD78',
          'WD79',
          'WD80',
          'WD81'
        )
      ) 
       and business_type_code !='6' 
    group by customer_code,
      performance_province_name,
      performance_city_name,
      substr(sdt, 1, 6),
      business_type_name,
      partner_type_code,
       CASE
        WHEN business_type_code in ('2', '10') then '福利'
        when business_type_code = '1' then '日配'
        when business_type_name = '批发内购' then '内购'
        when business_type_name = '省区大宗' then '大宗贸易'
        else business_type_name end 
    union all 
    select if(
          credit_pay_type_name = '餐卡'
          or credit_pay_type_code = 'F11',
          '餐卡',
          '福利'
        ) type_flag,
        customer_code,
        substr(sdt, 1, 6) smonth,
        'BBC' AS business_type_name,
        'BBC' new_business_type_name,
        0 partner_type_code,
        
        sum(sale_amt) as sale_amt,
        sum(profit) profit,
        sum(profit_no_tax) excluding_tax_profit,
        sum(sale_amt_no_tax) excluding_tax_sales
    from
          csx_dws.csx_dws_bbc_sale_detail_di
      where
        sdt >= '20251001'
        and sdt <= '20251231' -- and customer_code not in ('120459','121206')
 
    group by if(
          credit_pay_type_name = '餐卡'
          or credit_pay_type_code = 'F11',
          '餐卡',
          '福利'
        )  ,
        customer_code,
        substr(sdt, 1, 6) 
		) 
select
    b.performance_region_name,
    b.performance_province_name,
    b.performance_city_name,
    b.city_type_name,
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
    a.customer_code ,
    b.customer_name,
    a.smonth,
    a.business_type_name,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(excluding_tax_sales) excluding_tax_sales,
    sum(excluding_tax_profit) excluding_tax_profit,
    sum(if(partner_type_code in (1, 3), sale_amt, 0)) partner_sale_amt,
    sum(if(partner_type_code in (1, 3), profit, 0)) partner_profit,
    sum(
      if(
        partner_type_code in (1, 3),
        excluding_tax_sales,
        0
      )
    ) partner_excluding_tax_sales,
    sum(
      if(
        partner_type_code in (1, 3),
        excluding_tax_profit,
        0
      )
    ) partner_excluding_tax_profit,
    -- type_flag,
    if(type_flag='餐卡'  and bbc_sale_number=rp_sale_number and b.city_type_name in ('A/B') ,1,0)  AS new_type_flag
from tmp_sale_detail a	
left join 
(select customer_code,	
    customer_name,
    performance_region_name,
    performance_province_name,
    performance_city_name ,
     CASE
    WHEN a.performance_city_name IN ('北京市','福州市','重庆主城','深圳市','成都市','上海松江','南京主城','合肥市','西安市','石家庄市','江苏苏州','杭州市','郑州市','广东广州') THEN 'A/B'
    WHEN a.performance_city_name IN ('厦门市','宁波市','泉州市','莆田市','南平市','南昌市','贵阳市','宜宾','武汉市' ) THEN 'C'
    WHEN a.performance_city_name IN ('三明市', '阜阳市', '台州市', '龙岩市', '万州区', '江苏盐城', '黔江区', '永川区') then 'D'
    else 'D'
  END as city_type_name
from csx_dim.csx_dim_crm_customer_info  a
where sdt='current' 
) b on a.customer_code=b.customer_code
LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c 
  on a.customer_code = c.customer_no  and a.new_business_type_name = c.attribute_name
LEFT join  
(SELECT   customer_no,
    max(case when attribute_name='BBC' then coalesce(sale_number,'') end  ) bbc_sale_number,
    max(case when attribute_name='日配' then coalesce(sale_number,'') end ) rp_sale_number 
FROM csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info 
where attribute_name in ('BBC','日配')
group by customer_no ) d  on a.customer_code = d.customer_no  
group by 
      b.performance_region_name,
    b.performance_province_name,
    b.performance_city_name,
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
    b.customer_name,
    a.business_type_name,
	a.smonth,
-- 	type_flag,
	city_type_name,
	if(type_flag='餐卡'  and bbc_sale_number=rp_sale_number and b.city_type_name in ('A/B') ,1,0) ;



select * from csx_analyse_tmp.csx_analyse_tmp_sales_sale_detail
 
 
-- 应收周转
	
--应收周转明细
-- select * from csx_analyse_tmp.csx_analyse_tmp_manager_quarterly_sap_detail
-- --应收周转明细
-- drop table csx_analyse_tmp.csx_analyse_tmp_manager_quarterly_sap_detail;
create table csx_analyse_tmp.csx_analyse_tmp_manager_quarterly_sap_detail as 
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
    and  b.business_attribute_name in ('日配','福利','BBC')
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
        when business_type_code='1' then '日配'
        when business_type_name='批发内购' then '内购'
        when business_type_name='省区大宗' then '大宗贸易'
        else business_type_name
      end new_business_type_name,
          sum(sale_amt) sale_amt,
          sum(sale_amt_no_tax) as excluding_tax_sales
        from
          csx_dws.csx_dws_sale_detail_di
        where
          sdt >= '20251001'
          and sdt <= '20251231'
          and  business_type_code in ('1','2','10','6')
        group by
          customer_code,
              CASE
        WHEN business_type_code in ('2', '10') then '福利'
        when business_type_code='1' then '日配'
        when business_type_name='批发内购' then '内购'
        when business_type_name='省区大宗' then '大宗贸易'
        else business_type_name
      end 
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

	
---应收周转天数 汇总
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
  from csx_analyse_tmp.csx_analyse_tmp_manager_quarterly_sap_detail a
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
         
-- 销售经理A、B类客户断约明细
-- AB类客户按照等级 
create table csx_analyse_tmp.csx_analyse_tmp_terminate_info as with tmp_customer_level as (
	select region_name,
		province_name,
		city_group_name,
		customer_no,
		customer_name,
		first_category_name,
		second_category_name,
		last_order_date,
		-- 最后一次下单时间 需要注意这个是全业务的，下季度要调整20250731
		work_no,
		sales_name,
		sales_value,
		profit,
		profit_rate,
		customer_large_level,
		customer_small_level,
		customer_level_tag,
		if_new_order_cus,
		first_order_date,
		quarter,
		'日配' business_attribute_name
	from csx_analyse.csx_analyse_report_sale_customer_level_qf
	where tag = '2'
		and quarter = '20253' -- 取上一季度
		and customer_large_level in ('A', 'B')
),
tmp_sale_detail as (
	SELECT customer_code,
		MAX(sdt) AS max_sdt,
		DATEDIFF(
			'2025-12-31',
			FROM_UNIXTIME(
				UNIX_TIMESTAMP(MAX(sdt), 'yyyyMMdd'),
				'yyyy-MM-dd'
			)
		) AS diff_days
	FROM csx_dws.csx_dws_sale_detail_di
	WHERE sdt >= '20251001'
		AND sdt <= '20251231'
		AND business_type_code = 1
		AND order_channel_detail_code not in ('25', '27') -- --剔除调价返利
		and refund_order_flag != 1 -- 剔除退货
	GROUP BY customer_code
),
tmp_terminate_info as (
	select customer_code,
		terminate_date,
		row_number() over(
			partition by customer_code
			order by terminate_date desc
		) as rn
	from (
			select customer_code,
				terminate_date
			from csx_dim.csx_dim_crm_terminate_customer
			where sdt = 'current'
				and business_attribute_code = 1
				and status = 2
			union all
			select customer_code,
				terminate_date
			from csx_dim.csx_dim_crm_terminate_customer_attribute
			where sdt = 'current'
				and business_attribute_code = 1
				and approval_status = 2
		) a
)
select region_name,
	province_name,
	city_group_name,
	customer_no,
	customer_name,
	first_category_name,
	second_category_name,
	last_order_date,
	-- 最后一次下单时间 需要注意这个是全业务的，下季度要调整20250731
	work_no,
	sales_name,
	sales_value,
	profit,
	profit_rate,
	customer_large_level,
	customer_small_level,
	customer_level_tag,
	--   if_new_order_cus,
	--   first_order_date,
	--   quarter,
	--   '日配' business_attribute_name,
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
	IF(	d.customer_code IS not NULL,'断约客户',	NULL) AS typeder
from tmp_customer_level a
	left join tmp_sale_detail b on a.customer_no = b.customer_code
	left join (
		select *
		from tmp_terminate_info
		where rn = 1
			and terminate_date between '20251001' and '20251231'
	) d on a.customer_no = d.customer_code
	LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c on a.customer_no = c.customer_no
	and a.business_attribute_name = c.attribute_name
	;

-- AB类客户断约率汇总

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
from csx_analyse_tmp.csx_analyse_tmp_terminate_info a
group by region_name,
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
	a.supervisor_position;

