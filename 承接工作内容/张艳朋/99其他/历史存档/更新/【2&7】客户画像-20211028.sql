
insert overwrite directory '/tmp/zhangyanpeng/shangpinby' row format delimited fields terminated by '\t'
select
	smonth,
	coalesce(province_code,'00') province_code,
	coalesce(province_name,'全国') province_name,
	coalesce(channel,'B+BBC')  channel,
	second_category_code,
	second_category_name,
	kehushu,
	excluding_tax_sales,
	excluding_tax_sales/kehushu arpu,
	excluding_tax_profit,
	excluding_tax_profit/excluding_tax_sales excluding_tax_profitlv
from 
	(
	select
		smonth,
		province_code,
		province_name,
		channel,
		second_category_code,
		second_category_name,
		count(distinct a.customer_no) kehushu,
		sum(excluding_tax_sales) excluding_tax_sales,
		sum(excluding_tax_profit) excluding_tax_profit
	from  
		(
		select 
			substr(sdt,1,6) smonth,
			province_code,
			province_name,
			case when channel_code in ('1', '9') then 'B'
				when channel_code in ('7') then 'BBC' end channel,
			customer_no,
			customer_name,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) excluding_tax_profit
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt >= '20190101' and sdt<'20211001'
			and channel_code in ('1', '7', '9')
		group by 
			substr(sdt,1,6),
			province_code,
			province_name,
			case when channel_code in ('1', '9') then 'B'
	  	       when channel_code in ('7') then 'BBC' end,
			customer_no,
			customer_name
		)a 
		left join 
			(
			select 
				customer_no,
				second_category_code,
				second_category_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current'
			)b on a.customer_no=b.customer_no
	group by 
		smonth,
		province_code,
		province_name,channel,
		second_category_code,
		second_category_name
		GROUPING SETS ((smonth,second_category_code,second_category_name),
		(smonth,second_category_code,second_category_name,channel),
		(smonth,province_code,province_name,second_category_code,second_category_name),
		(smonth,province_code,province_name,channel,second_category_code,second_category_name))
	
	union all

	select
		smonth,
		province_code,
		province_name,
		channel,
		second_category_code,
		second_category_name,
		count(distinct a.customer_no) kehushu,
		sum(excluding_tax_sales) excluding_tax_sales,
		sum(excluding_tax_profit) excluding_tax_profit
	from  
		(
		select 
			substr(sdt,1,6) smonth,
			province_code,
			province_name,
			business_type_name channel,
			customer_no,
			customer_name,
			sum(excluding_tax_sales) excluding_tax_sales,
			sum(excluding_tax_profit) excluding_tax_profit
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt >= '20190101' and sdt<'20211001'
			and business_type_code='1'
		group by 
			substr(sdt,1,6),
			province_code,
			province_name,
			business_type_name,
			customer_no,
			customer_name
		)a 
		left join 
			(
			select 
				customer_no,
				second_category_code,
				second_category_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current'
			)b on a.customer_no=b.customer_no
	group by 
		smonth,
		province_code,
		province_name,channel,
		second_category_code,
		second_category_name
		GROUPING SETS (--(smonth,second_category_code,second_category_name),
		(smonth,second_category_code,second_category_name,channel),
		--(smonth,province_code,province_name,second_category_code,second_category_name),
		(smonth,province_code,province_name,channel,second_category_code,second_category_name))
	)a
--where province_name is null or province_name in ('重庆市','福建省','安徽省')


-------明细
insert overwrite directory '/tmp/gaoxuefang/price_202101' row format delimited fields terminated by '\t' 
select
	a.smonth,
	a.customer_no,
	b.customer_name,
	b.city_group_name,
	b.sales_province_name,
	b.channel_name,
	a.business_type_name,
	b.second_category_code,
	b.second_category_name,
	a.excluding_tax_sales,
	a.excluding_tax_profit
from  
	(
    select 
		substr(sdt,1,6) smonth,
		business_type_name,
		customer_no,
		sum(excluding_tax_sales) excluding_tax_sales,
		sum(excluding_tax_profit) excluding_tax_profit
    from 
		csx_dw.dws_sale_r_d_detail 
    where 
		(sdt >= '20200101' and sdt<'20200701') or (sdt >= '20210101' and sdt<'20210701')
		and channel_code in ('1', '7', '9')
	group by   
		substr(sdt,1,6),
		business_type_name,
		customer_no
	)a 
	join 
		(
		select 
			customer_no,customer_name,city_group_name,sales_province_name,channel_name,
			second_category_code,
			second_category_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
			and second_category_name in ('交通运输','政府机关','监狱')
		)b on a.customer_no=b.customer_no












