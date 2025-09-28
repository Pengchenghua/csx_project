--=============================================================================================================================================================================

	--left join
	--	(
	--	select --应收预期
	--		customer_no,
	--		sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	--		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
	--	from
	--		csx_dw.dws_sss_r_a_customer_accounts
	--	where
	--		sdt='20211010'
	--	group by 
	--		customer_no
	--	) c on c.customer_no=a.customer_no

-- 月均大于等于15万
select
	q_type,
	count(distinct customer_no) as customer_cnt,
	sum(sales_value)/10000 as sales_value,
	sum(profit)/10000 as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate
from
	(
	select 
		customer_no,
		substr(sdt,1,6) as s_month,
		case when sdt between '20200701' and '20200930' then '2020Q3'
			when sdt between '20201001' and '20201231' then '2020Q4'
			when sdt between '20210101' and '20210331' then '2021Q1'
			when sdt between '20210401' and '20210630' then '2021Q2'
			when sdt between '20210701' and '20210930' then '2021Q3'
		else '其他' end as q_type,
		sum(sales_value) sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20200701' and sdt<='20210930'
		and channel_code in('1','7','9')
		and business_type_code in ('1') --日配业务
	group by 
		customer_no,
		substr(sdt,1,6),
		case when sdt between '20200701' and '20200930' then '2020Q3'
			when sdt between '20201001' and '20201231' then '2020Q4'
			when sdt between '20210101' and '20210331' then '2021Q1'
			when sdt between '20210401' and '20210630' then '2021Q2'
			when sdt between '20210701' and '20210930' then '2021Q3'
		else '其他' end
	having
		sales_value>=150000
	) a
group by 
	q_type
;


-- 月均大于等于15万
select
	q_type,
	count(distinct customer_no) as customer_cnt,
	sum(sales_value)/10000 as sales_value,
	sum(profit)/10000 as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate
from
	(
	select 
		customer_no,
		substr(sdt,1,6) as s_month,
		case when sdt between '20200701' and '20200930' then '2020Q3'
			when sdt between '20201001' and '20201231' then '2020Q4'
			when sdt between '20210101' and '20210331' then '2021Q1'
			when sdt between '20210401' and '20210630' then '2021Q2'
			when sdt between '20210701' and '20210930' then '2021Q3'
		else '其他' end as q_type,
		sum(sales_value) sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20200701' and sdt<='20210930'
		and channel_code in('1','7','9')
		and business_type_code in ('1') --日配业务
	group by 
		customer_no,
		substr(sdt,1,6),
		case when sdt between '20200701' and '20200930' then '2020Q3'
			when sdt between '20201001' and '20201231' then '2020Q4'
			when sdt between '20210101' and '20210331' then '2021Q1'
			when sdt between '20210401' and '20210630' then '2021Q2'
			when sdt between '20210701' and '20210930' then '2021Q3'
		else '其他' end
	having
		sales_value>=50000
	) a
group by 
	q_type
;





全国的top20；
top5行业里面的top3客户;
每个省区top10;


-- 全国的top20
select
	a.customer_no,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.sales_province_name	
	--row_number() over(order by a.sales_value desc) as rn
from
	(
	select 
		customer_no,
		sum(sales_value) sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		row_number() over(order by sum(sales_value) desc) as rn
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20201001' and sdt<='20210930'
		and channel_code in('1','7','9')
		and business_type_code ='1'
	group by 
		customer_no
	) a 
	left join 
		(
		select 
			customer_no,customer_name,dev_source_name,sales_province_name,sales_city_name,channel_name,sales_name,work_no,
			first_category_name,second_category_name,third_category_name,first_sign_time
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current' 
		)b on a.customer_no=b.customer_no
where
	rn<=20
;

-- top5行业里面的top3客户
select 
	* 
from 
	(
	select
		a.customer_no,
		b.customer_name,
		b.sales_province_name,		
		b.first_category_name,
		b.second_category_name,
		sum(sales_value) as sales_value,
		row_number() over(partition by b.second_category_name order by sum(sales_value) desc) as rn
	from
		(
		select 
			customer_no,
			sum(sales_value) sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
			--row_number() over(order by sum(sales_value) desc) as rn
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201001' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code ='1'
		group by 
			customer_no
		) a 
		left join 
			(
			select 
				customer_no,customer_name,dev_source_name,sales_province_name,sales_city_name,channel_name,sales_name,work_no,
				first_category_name,second_category_name,third_category_name,first_sign_time
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current' 
			)b on a.customer_no=b.customer_no
	where
		b.second_category_name in ('部队','教育','电力燃气水供应','监狱','事业单位')
	group by 
		a.customer_no,
		b.customer_name,
		b.sales_province_name,
		b.first_category_name,
		b.second_category_name) a 
where
	rn<=3
;

-- 每个省区top10
select 
	* 
from 
	(
	select
		a.customer_no,
		b.customer_name,
		b.sales_province_name,
		b.first_category_name,
		b.second_category_name,
		sum(sales_value) as sales_value,
		row_number() over(partition by b.sales_province_name order by sum(sales_value) desc) as rn
	from
		(
		select 
			customer_no,
			sum(sales_value) sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
			--row_number() over(order by sum(sales_value) desc) as rn
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201001' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code ='1'
		group by 
			customer_no
		) a 
		left join 
			(
			select 
				customer_no,customer_name,dev_source_name,sales_province_name,sales_city_name,channel_name,sales_name,work_no,
				first_category_name,second_category_name,third_category_name,first_sign_time
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current' 
			)b on a.customer_no=b.customer_no
	group by 
		a.customer_no,
		b.customer_name,
		b.sales_province_name,
		b.first_category_name,
		b.second_category_name) a 
where
	rn<=10
;