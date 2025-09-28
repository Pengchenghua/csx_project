---大区经营分析---
drop table csx_analyse_tmp.tmp_cust_yuedu_sale_business_sq
create  table csx_analyse_tmp.tmp_cust_yuedu_sale_business_sq
as
select years,
	concat(substr(a.smonth, 1, 4),"Q",floor(substr(a.smonth, 5, 2)/3.1) + 1) as sale_quarter,
	a.smonth,a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	c.customer_name,
	c.first_category_name,
	c.second_category_name,
	d.new_classify_name,
	a.channel_code,
	a.channel_name,
	regexp_replace(split(c.first_sign_time, ' ')[0], '-', '') as first_sign_date,
	e.first_sale_date,
	a.business_type_code,
	a.business_type_name,
	a.business_type_name_new,
	f.first_business_sale_date,
	if (f.first_business_sale_date is null ,1,0) as fanli,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit
from
  (
	  select 
		 substr(sdt,1,4) years,
	     substr(sdt,1,6) smonth,
		 channel_code,channel_name,
		 performance_region_name,performance_province_name,performance_city_name,
		 customer_code,second_category_code,
		 goods_code,
		 business_type_code,business_type_name,
		 case when (business_type_code='1' and inventory_dc_code in ('W0K4','W0Z7','WB38','WB26'))then '99' else business_type_code end as business_type_code_new,
		 case when (business_type_code='1' and inventory_dc_code in ('W0K4','W0Z7','WB38','WB26'))then '直送仓' else business_type_name end as business_type_name_new,
		 sum(sale_qty) sale_qty,
		 sum(sale_amt) sale_amt,
		 sum(profit) profit,
		 sum(sale_cost) sale_cost
	  from csx_dws.csx_dws_sale_detail_di 
	  where sdt>='20240101'
		  and sdt<'20240901'
	  group by 
	  substr(sdt,1,4),substr(sdt,1,6),channel_code,channel_name,performance_region_name,performance_province_name,performance_city_name,customer_code,  		    second_category_code,goods_code,business_type_code,business_type_name,
			case when (business_type_code='1' and inventory_dc_code in ('W0K4','W0Z7','WB38','WB26')) then '99' else business_type_code end,
			case when (business_type_code='1' and inventory_dc_code in ('W0K4','W0Z7','WB38','WB26')) then '直送仓' else business_type_name end
  )a
left join (select *  from  csx_dim.csx_dim_basic_goods where sdt = 'current') b on b.goods_code = a.goods_code			
left join (select * from csx_dim.csx_dim_crm_customer_info where sdt = 'current' )c on c.customer_code=a.customer_code
left join csx_analyse.csx_analyse_fr_new_customer_classify_mf d on c.second_category_code = d.second_category_code	
--客户最早销售
left join 
  (
    select customer_code,first_sale_date
    from csx_dws.csx_dws_crm_customer_active_di
    where sdt = 'current'
  )e on a.customer_code=e.customer_code	
--客户业务类型最早销售日期
left join
	(select customer_code,business_type_code,first_business_sale_date
	 from csx_dws.csx_dws_crm_customer_business_active_di
	 where sdt = 'current'
	)f on a.customer_code=f.customer_code and a.business_type_code=f.business_type_code
group by 
	years,
	concat(substr(a.smonth, 1, 4),"Q",floor(substr(a.smonth, 5, 2)/3.1) + 1),
	a.smonth,a.performance_region_name,a.performance_province_name,a.performance_city_name,a.customer_code,c.customer_name,
	c.first_category_name,c.second_category_name,
	d.new_classify_name,a.channel_code,a.channel_name,
	regexp_replace(split(c.first_sign_time, ' ')[0], '-', ''),e.first_sale_date,
	a.business_type_code,a.business_type_name,a.business_type_name_new,f.first_business_sale_date;
	

====================================================================================================================================
-- 月度数据	
select years,
	sale_quarter,
	smonth,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	channel_code,
	channel_name,
	business_type_name,
	business_type_name_new,
	sum(sale_amt) sale_amt,
	sum(profit) profit,
	count(distinct customer_code) cust_count,
	count(
		distinct case
			when smonth = substr(first_business_sale_date, 1, 6) then customer_code
		end
	) cust_count_new,
	count(
		distinct case
			when smonth > substr(first_business_sale_date, 1, 6)
			or fanli = '1' then customer_code
		end
	) cust_count_old,
	sum(
		case
			when smonth = substr(first_business_sale_date, 1, 6) then sale_amt
		end
	) sales_new,
	sum(
		case
			when smonth > substr(first_business_sale_date, 1, 6)
			or fanli = '1' then sale_amt
		end
	) sales_old
from csx_analyse_tmp.tmp_cust_yuedu_sale_business_sq
where smonth >= '202404' -- and channel_code in ('1','2','7','9')
	and performance_region_name like '%大区'
group by years,
	sale_quarter,
	smonth,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	channel_code,
	channel_name,
	business_type_name,
	business_type_name_new;


----断约客户
select 			
	c.performance_province_name,c.performance_city_name, 
	after_month, 
	concat(substr(a.after_month, 1, 4),"Q", floor(substr(a.after_month, 5, 2)/3.1) + 1) as sale_quarter,
	count(distinct a.customer_code) as break_this_month
from
	(
	 select 
		customer_code,
		substr(regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90) as string),'-',''),1,6) as after_month
	 from 
		csx_dws.csx_dws_sale_detail_di 
	 where 
		sdt  between '20230101' and '20241030'
		and business_type_code in ('1') 
		and channel_code in('1','7','9')
		and order_channel_code not in (4,6)			
	 group by 
		customer_code
	) a
	left join   
		(
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= 'current'
            and channel_code  in ('1','7','9')
        ) c on a.customer_code=c.customer_code 
where after_month >= '202301'
group by c.performance_province_name,c.performance_city_name, after_month,concat(substr(a.after_month, 1, 4),"Q", floor(substr(a.after_month, 5, 2)/3.1) + 1);
		


======断约客户明细==========
select 	performance_province_name, 
			after_month, 
			a.customer_code,
			c.customer_name
			from
				(
				select 
					customer_code,
					substr(regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90) as string),'-',''),1,6) as after_month
				from 
					csx_dws.csx_dws_sale_detail_di 
				where 
					sdt between '20230101' and '20241031'
					and business_type_code=1 --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and order_channel_code not in (4,6)

				group by 
					customer_code
				) a
		left join   (
			  select *
			  from csx_dim.csx_dim_crm_customer_info
			  where sdt= 'current'
				and channel_code  in ('1','7','9')
			        ) c on a.customer_code=c.customer_code 
	   where after_month='202410'
			group by performance_province_name, 
			after_month, 
			a.customer_code,
			c.customer_name;
		
		
----目标

select * from csx_ods.csx_ods_csx_data_market_dws_basic_w_a_business_target_manage_df ---kpi目标
where month='202301'



select * from  csx_ods.csx_ods_csx_data_market_dws_basic_w_a_business_target_manage_df --战报目标
where month ='202410';




====================================================================================================================================
季度数据

select 
  sale_quarter,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  channel_code,channel_name,
  business_type_name,
  business_type_name_new,
  sum(sale_amt) sale_amt,
  sum(profit) profit,
  count(distinct customer_code) cust_count,
  count(distinct case when sale_quarter=concat(substr(first_business_sale_date, 1, 4), "Q",floor(substr(first_business_sale_date, 5, 2)/3.1) + 1) then customer_code end) cust_count_new,
  count(distinct case when sale_quarter>concat(substr(first_business_sale_date, 1, 4), "Q",floor(substr(first_business_sale_date, 5, 2)/3.1) + 1)  or fanli ='1' then customer_code end) cust_count_old,
  sum(case when sale_quarter=concat(substr(first_business_sale_date, 1, 4), "Q",floor(substr(first_business_sale_date, 5, 2)/3.1) + 1) then sale_amt end) sales_new,
  sum(case when sale_quarter>concat(substr(first_business_sale_date, 1, 4),"Q", floor(substr(first_business_sale_date, 5, 2)/3.1) + 1)  or fanli ='1' then sale_amt end) sales_old
from csx_analyse_tmp.tmp_cust_yuedu_sale_business_sq
where sale_quarter>='2022Q1' and sale_quarter<='2023Q3'
and channel_code in ('1','2','7','9')
and performance_region_name like '%大区'
group by sale_quarter,performance_region_name,performance_province_name,performance_city_name,channel_code,channel_name,business_type_name,business_type_name_new;



---季度断约
select 			
	c.performance_province_name,c.performance_city_name, 
	after_month, 
	concat(substr(a.after_month, 1, 4),"Q", floor(substr(a.after_month, 5, 2)/3.1) + 1) as sale_quarter,
	count(distinct a.customer_code) as break_this_month
from
	(
	 select 
		customer_code,
		substr(regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90) as string),'-',''),1,6) as   after_month
	 from 
		csx_dws.csx_dws_sale_detail_di 
	 where 
	 -- 判断取三个季度
		sdt between '20240101' and '20240630'
		and business_type_code in ('1') 
		and channel_code in('1','7','9')
		and order_channel_code!='4'				
	 group by 
		customer_code
	) a
	left join   
		(
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= 'current'
            and channel_code  in ('1','7','9')
        ) c on a.customer_code=c.customer_code 
where after_month >= '202404'	-- 判断取当前季度
group by c.performance_province_name,c.performance_city_name, after_month;




====================================================================================================================================
年度数据	
select years,years,years,performance_region_name,performance_province_name,performance_city_name,channel_code,channel_name,business_type_name,business_type_name_new,
  sum(sale_amt) sale_amt,
  sum(profit) profit,
  count(distinct customer_code) cust_count,
  count(distinct case when years=substr(first_business_sale_date,1,4) then customer_code end) cust_count_new,
  count(distinct case when years>substr(first_business_sale_date,1,4) or fanli ='1' then customer_code end) cust_count_old,
  sum(case when years=substr(first_business_sale_date,1,4) then sale_amt end) sales_new,
  sum(case when years>substr(first_business_sale_date,1,4) or fanli ='1'  then sale_amt end) sales_old
from csx_analyse_tmp.tmp_cust_yuedu_sale_business_sq
where smonth >='202001'
and channel_code in ('1','2','7','9')
and performance_region_name like '%大区'
group by years,years,years,performance_region_name,performance_province_name,performance_city_name,channel_code,channel_name,business_type_name,business_type_name_new;


====================================================================================================================================
断约客户	
select 			
	c.performance_province_name,
	c.performance_city_name,
	syear, 
	count(distinct a.customer_code) as break_this_month
from
	(
	 select 
		customer_code,
		substr(regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90) as string),'-',''),1,4) as syear
	 from 
		csx_dws.csx_dws_sale_detail_di 
	 where 
		sdt  between '20200101' and '20231231'
		and business_type_code in ('1') 
		and channel_code in('1','7','9')
		and order_channel_code not in (4)			
	 group by 
		customer_code
	) a
	left join   
		(
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= 'current'
            and channel_code  in ('1','7','9')
        ) c on a.customer_code=c.customer_code 
where syear >= '2020'
group by 
	c.performance_province_name,
	c.performance_city_name,
	syear
	
