-- 汇总
select
	smonth,
    province_name,
    business_type_name,
	count(distinct if(excluding_tax_profit<0,customer_no,null)) fukehus,
    sum(if(excluding_tax_profit<0,excluding_tax_sales,0)) as excluding_tax_sales,
	round(sum(if(excluding_tax_profit<0,excluding_tax_sales,0))/sum(excluding_tax_sales),6) zhanbi,
    sum(if(excluding_tax_profit<0,excluding_tax_profit,0)) as excluding_tax_profit,
    round(sum(if(excluding_tax_profit<0,excluding_tax_profit,0))/abs(sum(if(excluding_tax_profit<0,excluding_tax_sales,0))),6) excluding_tax_profitlv
from
	(
	select
		substr(sdt,1,6) smonth,
		province_name,
		business_type_name,
		customer_no,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20211101' and sdt <= '20211231'
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
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20211101' and sdt <= '20211231'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		substr(sdt,1,6),
		province_name,
		customer_no
	union all
	select
		'202101-12' as smonth,
		province_name,
		business_type_name,
		customer_no,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' and sdt <= '20211231'
		and channel_code in ('1', '7', '9')
		and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
	group by 
		province_name,
		business_type_name,
		customer_no
	union all
	select
		'202101-12' as smonth,
		province_name,
		'B自营' as business_type_name,
		customer_no,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' and sdt <= '20211231'
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
--不含税销售额
select
	substr(sdt,1,6) smonth,
	province_name,
	business_type_name,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20211101' and sdt <= '20211231'
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
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20211101' and sdt <= '20211231'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	substr(sdt,1,6),
	province_name
union all
select
	'202101-12' as smonth,
	province_name,
	business_type_name,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20210101' and sdt <= '20211231'
	and channel_code in ('1', '7', '9')
	and business_type_code in ('1','2','5','6')	--1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 
group by 
	province_name,
	business_type_name
union all
select
	'202101-12' as smonth,
	province_name,
	'B自营' as business_type_name,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20210101' and sdt <= '20211231'
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
			sdt= '20211231'
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
	sdt >= '20211101' and sdt <= '20211231'
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
    '202101-12' as  smonth,
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
			sdt= '20211231'
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
	sdt >= '20210101' and sdt <= '20211231'
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