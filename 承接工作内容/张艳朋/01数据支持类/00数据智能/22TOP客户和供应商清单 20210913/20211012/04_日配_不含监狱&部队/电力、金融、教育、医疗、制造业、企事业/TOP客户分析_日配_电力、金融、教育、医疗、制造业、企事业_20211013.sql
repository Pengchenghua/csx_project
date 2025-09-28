-- 电力、金融、教育、医疗、制造业、企事业 里面的top4客户
select 
	* 
from 
	(
	select
		a.customer_no,
		b.customer_name,	
		b.first_category_name,
		b.second_category_name,
		b.province_name,	
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
			sdt>='20210701' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code ='1'  
		group by 
			customer_no
		) a 
		left join 
			(
			select 
				customer_no,customer_name,dev_source_name,province_name,sales_city_name,channel_name,sales_name,work_no,
				first_category_name,second_category_name,third_category_name,first_sign_time
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current' 
			)b on a.customer_no=b.customer_no
	where
		b.second_category_name in ('电力燃气水供应','金融业','教育','医疗卫生','制造业','事业单位')
	group by 
		a.customer_no,
		b.customer_name,
		b.first_category_name,
		b.second_category_name,
		b.province_name
	) a 
where
	rn<=4
;


--==================================================================================================================================================================================、
-- 指标
select 
	customer_no,
	case when sdt between '20201001' and '20201231' then '2020Q4'
		when sdt between '20210101' and '20210331' then '2021Q1'
		when sdt between '20210401' and '20210630' then '2021Q2'
		when sdt between '20210701' and '20210930' then '2021Q3'
	else '其他' end as date_type,
	sum(sales_value) sales_value,
	sum(profit) as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	count(distinct sdt) as days_cnt,
	count(distinct goods_code) as goods_cnt
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt>='20201001' and sdt<='20210930'
	and channel_code in('1','7','9')
	and business_type_code ='1'  
	and customer_no in 
	('104086','117068','117797','112072','104901','106898','118094','118610','115906','110807','115656','109401','119833','104402','115909','104885','121061','106521',
	'106709','121070','106287','104493','120459','119760')
group by 
	customer_no,
	case when sdt between '20201001' and '20201231' then '2020Q4'
		when sdt between '20210101' and '20210331' then '2021Q1'
		when sdt between '20210401' and '20210630' then '2021Q2'
		when sdt between '20210701' and '20210930' then '2021Q3'
	else '其他' end
;	
	
select --应收预期
	customer_no,
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
from
	csx_dw.dws_sss_r_a_customer_accounts
where
	sdt='20211010'
	and customer_no in 
	('104086','117068','117797','112072','104901','106898','118094','118610','115906','110807','115656','109401','119833','104402','115909','104885','121061','106521',
	'106709','121070','106287','104493','120459','119760')
group by 
	customer_no