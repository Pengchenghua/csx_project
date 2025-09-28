--财务TOP客户销售、毛利、逾期，全国及分2级行业维度
--B+BBC含合伙人、B+BBC不含合伙人（注释项）
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t'
select *
from 
(
  select
    a.customer_no,b.customer_name,
    b.first_category_name,b.second_category_name,
    a.excluding_tax_sales,a.excluding_tax_sales_B,a.excluding_tax_sales_BBC,
    a.excluding_tax_profit,a.excluding_tax_profit_B,a.excluding_tax_profit_BBC,c.province_name,
    a.excluding_tax_sales_2021,a.excluding_tax_sales_2021_B,a.excluding_tax_sales_2021_BBC,
    a.excluding_tax_profit_2021,a.excluding_tax_profit_2021_B,a.excluding_tax_profit_2021_BBC,c.province_name_2021,
    a.excluding_tax_sales_2020,a.excluding_tax_sales_2020_B,a.excluding_tax_sales_2020_BBC,
    a.excluding_tax_profit_2020,a.excluding_tax_profit_2020_B,a.excluding_tax_profit_2020_BBC,c.province_name_2020,
    coalesce(d.receivable_amount,0) as receivable_amount,coalesce(d.overdue_amount,0) as overdue_amount,
    coalesce(d.overdue_amount,0)/coalesce(d.receivable_amount,0) as overdue_rate,   --逾期率
    rank() over (order by a.excluding_tax_sales desc ) as rno_excluding_tax_sales, --销售金额排名
    rank() over (partition by b.second_category_name order by a.excluding_tax_sales desc ) as rno_excluding_tax_sales_category, --二级行业销售金额排名
    rank() over (order by a.excluding_tax_sales_2021 desc ) as rno_excluding_tax_sales_2021, --2021销售金额排名
    rank() over (partition by b.second_category_name order by a.excluding_tax_sales_2021 desc ) as rno_excluding_tax_sales_category_2021, --2021二级行业销售金额排名  
    rank() over (order by a.excluding_tax_sales_2020 desc ) as rno_excluding_tax_sales_2020, --2020销售金额排名
    rank() over (partition by b.second_category_name order by a.excluding_tax_sales_2020 desc ) as rno_excluding_tax_sales_category_2020 --2020二级行业销售金额排名 
  from
  --销售业绩
    (
      select 
        customer_no,
  	    sum(excluding_tax_sales) as excluding_tax_sales,
  	    sum(case when channel_code in('1','9') then excluding_tax_sales end) excluding_tax_sales_B,
        sum(case when channel_code in('7') then excluding_tax_sales end) excluding_tax_sales_BBC,	  
  	    sum(excluding_tax_profit) as excluding_tax_profit,
  	    sum(case when channel_code in('1','9') then excluding_tax_profit end) excluding_tax_profit_B,
        sum(case when channel_code in('7') then excluding_tax_profit end) excluding_tax_profit_BBC,	
  	    
  	    sum(case when substr(sdt,1,4)='2021' then excluding_tax_sales end) excluding_tax_sales_2021,
  	    sum(case when substr(sdt,1,4)='2021' and channel_code in('1','9') then excluding_tax_sales end) excluding_tax_sales_2021_B,
        sum(case when substr(sdt,1,4)='2021' and channel_code in('7') then excluding_tax_sales end) excluding_tax_sales_2021_BBC,
  	    sum(case when substr(sdt,1,4)='2021' then excluding_tax_profit end) excluding_tax_profit_2021,
  	    sum(case when substr(sdt,1,4)='2021' and channel_code in('1','9') then excluding_tax_profit end) excluding_tax_profit_2021_B,
        sum(case when substr(sdt,1,4)='2021' and channel_code in('7') then excluding_tax_profit end) excluding_tax_profit_2021_BBC,
  	    
        sum(case when substr(sdt,1,4)='2020' then excluding_tax_sales end) excluding_tax_sales_2020,
  	    sum(case when substr(sdt,1,4)='2020' and channel_code in('1','9') then excluding_tax_sales end) excluding_tax_sales_2020_B,
        sum(case when substr(sdt,1,4)='2020' and channel_code in('7') then excluding_tax_sales end) excluding_tax_sales_2020_BBC,
  	    sum(case when substr(sdt,1,4)='2020' then excluding_tax_profit end) excluding_tax_profit_2020,
  	    sum(case when substr(sdt,1,4)='2020' and channel_code in('1','9') then excluding_tax_profit end) excluding_tax_profit_2020_B,
        sum(case when substr(sdt,1,4)='2020' and channel_code in('7') then excluding_tax_profit end) excluding_tax_profit_2020_BBC
      from csx_dw.dws_sale_r_d_detail
      where sdt>='20200101'
      and sdt<'20210701'
      and channel_code in('1','7','9')
      --and business_type_code <>'4'  --不含合伙人
      group by customer_no
    )a
  --客户信息、行业
  left join 
    (
      select dev_source_name,sales_province_name,sales_city_name,channel_name,
        customer_no,customer_name,sales_name,work_no,
        first_category_name,second_category_name,third_category_name,first_sign_time
      from csx_dw.dws_crm_w_a_customer
      where sdt='current' 
    )b on a.customer_no=b.customer_no
  --业绩省区
  left join
    ( 
      select customer_no,
  	    concat_ws(';', collect_set(province_name)) as province_name,
  	    concat_ws(';', collect_set(case when syear='2021' then province_name end)) as province_name_2021,
  	    concat_ws(';', collect_set(case when syear='2020' then province_name end)) as province_name_2020
	  from 
	    (select distinct customer_no,substr(sdt,1,4) syear,province_name
  	    from csx_dw.dws_sale_r_d_detail
        where sdt>='20200101'
  	    and sdt<'20210701'
  	    and channel_code in('1','7','9')
        --and business_type_code <>'4'  --不含合伙人		
        )a	
  	  group by customer_no
    )c on a.customer_no=c.customer_no
  --逾期率
  left join
    ( 
      select customer_no,
  	    sum(receivable_amount) receivable_amount,
  	    sum(overdue_amount) overdue_amount
      from csx_dw.dws_sss_r_a_customer_accounts
      where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
      group by customer_no
    )d on a.customer_no=d.customer_no
)a
where (rno_excluding_tax_sales<=20 or rno_excluding_tax_sales_category<=5
or rno_excluding_tax_sales_2021<=20 or rno_excluding_tax_sales_category_2021<=5
or rno_excluding_tax_sales_2020<=20 or rno_excluding_tax_sales_category_2020<=5);