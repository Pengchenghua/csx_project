-- 汇总
select
	smonth,
    province_name,
    business_type_name,
	count(distinct if(profit<0,customer_no,null)) fukehus,
    sum(if(profit<0,sales_value,0)) as sales_value,
	round(sum(if(profit<0,sales_value,0))/sum(sales_value),6) zhanbi,
    sum(if(profit<0,profit,0)) as profit,
    round(sum(if(profit<0,profit,0))/abs(sum(if(profit<0,sales_value,0))),6) profitlv
from
	(
	select
		substr(sdt,1,6) smonth,
		province_name,
		business_type_name,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20220101' and sdt <= '20220630'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		substr(sdt,1,6),
		province_name,
		business_type_name,
		customer_no
	union all
	select
		substr(sdt,1,6) smonth,
		province_name,
		'B自营' as business_type_name,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20220101' and sdt <= '20220630'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		substr(sdt,1,6),
		province_name,
		customer_no
	union all
	select
		'202201-03' as smonth,
		province_name,
		business_type_name,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20220101' and sdt <= '20220331'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		province_name,
		business_type_name,
		customer_no
	union all
	select
		'202201-03' as smonth,
		province_name,
		'B自营' as business_type_name,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20220101' and sdt <= '20220331'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		province_name,
		customer_no

	union all
	select
		'202204-06' as smonth,
		province_name,
		business_type_name,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20220401' and sdt <= '20220630'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		province_name,
		business_type_name,
		customer_no
	union all
	select
		'202204-06' as smonth,
		province_name,
		'B自营' as business_type_name,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20220401' and sdt <= '20220630'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		province_name,
		customer_no
	)a
group by 
	smonth,
    province_name,
    business_type_name
;
	
--==============================================================================================================================================================================
--含税销售额
select
	substr(sdt,1,6) smonth,
	province_name,
	business_type_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20220101' and sdt <= '20220630'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	substr(sdt,1,6),
	province_name,
	business_type_name
union all
select
	substr(sdt,1,6) smonth,
	province_name,
	'B自营' as business_type_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20220101' and sdt <= '20220630'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	substr(sdt,1,6),
	province_name
union all
select
	'202201-03' as smonth,
	province_name,
	business_type_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20220101' and sdt <= '20220331'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	province_name,
	business_type_name
union all
select
	'202201-03' as smonth,
	province_name,
	'B自营' as business_type_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20220101' and sdt <= '20220331'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	province_name

union all
select
	'202204-06' as smonth,
	province_name,
	business_type_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20220401' and sdt <= '20220630'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	province_name,
	business_type_name
union all
select
	'202204-06' as smonth,
	province_name,
	'B自营' as business_type_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20220401' and sdt <= '20220630'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	province_name
;
--==============================================================================================================================================================================	
--明细

insert overwrite directory '/tmp/zhangyanpeng/20211209_linshi_1' row format delimited fields terminated by '\t' 

select
    substr(sdt,1,6) smonth,
    a.customer_no,
	c.customer_name,
    province_name,
    a.business_type_name,
	a.channel_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.sales_name,	
	sum(sales_value) sales_value,
    sum(excluding_tax_sales) as excluding_tax_sales,
	sum(sales_cost) sales_cost,
	sum(excluding_tax_cost) excluding_tax_cost,
	sum(profit) profit ,
    sum(excluding_tax_profit) as excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail a 
	left join   
		(
		select 
			customer_no,
			customer_name,
			first_category_name,
			second_category_name,
			third_category_name,
			sales_name
        from 
			csx_dw.dws_crm_w_a_customer
        where 
			sdt= '20220630'
            and channel_code in ('1','7','9')
		) c on a.customer_no=c.customer_no 
    left join 
		(
		select 
			customer_no,first_order_date
        from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current' 
		) f on a.customer_no=f.customer_no	
where 
	sdt >= '20220101' and sdt <= '20220630'
    and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by  
	substr(sdt,1,6),
    a.customer_no,
	c.customer_name,
    province_name, 
	a.business_type_name,
	a.channel_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.sales_name
having 
	sum(excluding_tax_profit) <0
	
union all

select
    '202201-03' as  smonth,
    a.customer_no,
	c.customer_name,
    province_name,
    a.business_type_name,
	a.channel_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.sales_name,	
	sum(sales_value) sales_value,
    sum(excluding_tax_sales) as excluding_tax_sales,
	sum(sales_cost) sales_cost,
	sum(excluding_tax_cost) excluding_tax_cost,
	sum(profit) profit ,
    sum(excluding_tax_profit) as excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail a 
	left join   
		(
        select 
			customer_no,
			customer_name,
			first_category_name,
			second_category_name,
			third_category_name,
			sales_name
        from 
			csx_dw.dws_crm_w_a_customer
        where 
			sdt= '20220630'
            and channel_code  in ('1','7','9')
        ) c on a.customer_no=c.customer_no 
    left join 
		(
		select 
			customer_no,first_order_date
        from 
			csx_dw.dws_crm_w_a_customer_active
        where 
			sdt='current' 
		) f on a.customer_no=f.customer_no	
where 
	sdt >= '20220101' and sdt <= '20220331'
    and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by  
    a.customer_no,
	c.customer_name,
    province_name,
	a.business_type_name,
	a.channel_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.sales_name
having 
	sum(excluding_tax_profit) <0

union all

select
    '202204-06' as  smonth,
    a.customer_no,
	c.customer_name,
    province_name,
    a.business_type_name,
	a.channel_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.sales_name,	
	sum(sales_value) sales_value,
    sum(excluding_tax_sales) as excluding_tax_sales,
	sum(sales_cost) sales_cost,
	sum(excluding_tax_cost) excluding_tax_cost,
	sum(profit) profit ,
    sum(excluding_tax_profit) as excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail a 
	left join   
		(
        select 
			customer_no,
			customer_name,
			first_category_name,
			second_category_name,
			third_category_name,
			sales_name
        from 
			csx_dw.dws_crm_w_a_customer
        where 
			sdt= '20220630'
            and channel_code  in ('1','7','9')
        ) c on a.customer_no=c.customer_no 
    left join 
		(
		select 
			customer_no,first_order_date
        from 
			csx_dw.dws_crm_w_a_customer_active
        where 
			sdt='current' 
		) f on a.customer_no=f.customer_no	
where 
	sdt >= '20220401' and sdt <= '20220630'
    and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by  
    a.customer_no,
	c.customer_name,
    province_name,
	a.business_type_name,
	a.channel_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	c.sales_name
having 
	sum(excluding_tax_profit) <0