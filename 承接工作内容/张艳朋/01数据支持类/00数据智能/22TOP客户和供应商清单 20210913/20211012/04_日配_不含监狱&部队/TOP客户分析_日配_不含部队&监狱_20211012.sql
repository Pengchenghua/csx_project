-- 全国的top20
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
		row_number() over(order by a.sales_value desc) as rn,
		a.sales_value
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
				customer_no,customer_name,dev_source_name,province_name,sales_city_name,channel_name,sales_name,work_no,
				first_category_name,second_category_name,third_category_name,first_sign_time
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current' 
			)b on a.customer_no=b.customer_no
	where
		b.second_category_name not in ('部队','监狱')
	) tmp
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
			sdt>='20201001' and sdt<='20210930'
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
		b.second_category_name in ('教育','电力燃气水供应','事业单位','政府机关','制造业')
	group by 
		a.customer_no,
		b.customer_name,
		b.first_category_name,
		b.second_category_name,
		b.province_name
	) a 
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
		b.first_category_name,
		b.second_category_name,
		b.province_name,
		sum(sales_value) as sales_value,
		row_number() over(partition by b.province_name order by sum(sales_value) desc) as rn
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
				customer_no,customer_name,dev_source_name,province_name,sales_city_name,channel_name,sales_name,work_no,
				first_category_name,second_category_name,third_category_name,first_sign_time
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current' 
			)b on a.customer_no=b.customer_no
	where
		b.second_category_name not in ('部队','监狱')
	group by 
		a.customer_no,
		b.customer_name,
		b.first_category_name,
		b.second_category_name,
		b.province_name
	) a 
where
	rn<=10
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
	('104922','111148','104779','105507','115378','115519','115489','117472','117419','115631','106287','113643','104493','104538','115971','116099','113387','105186','107469',
	'111430','108095','111137','115544','113609','111093','107910','105753','105643','106477','114494','113461','117327','108589','106566','110926','102901','112718','102784',
	'106953','114584','107807','106862','107307','112754','113522','107565','116475','119069','117230','108389','104402','104885','102534','115630','114650','115909','108096',
	'104086','108557','111954','107851','112072','107806','105525','119177','116401','104198','105669','107986','104268','112380','112609','112570','116690','107093','104758',
	'108722','104840','105717','111834','118849','105242','104666','113194','106719','107070','117351','117308','117481','107784','104865','111805','107356','106594','112622',
	'116135','107428','105446','105821','104322','108818','104596','115696','113743','112611','104340','110927','113957','113067','115202','114318','PF1205','103876','104275',
	'109342','106587','102633','103859','113463','115986','113439','106521','111200','108003','103044','108102','115732','112492','103784','105293','105005','104621','110546',
	'115156','116512','117303','104357','110785','104477','104281','108267','112067','106881','110807','109401','115906','115935','115656','105164','105165','105156','106721',
	'105182','107404','106805','111100','102961','108259','112477','110797','119132','117526','111366','111326','104872','102225','105163','105085','115237','113152','105221',
	'102755','106775','106081','105696','113758','113809','114548','104901','106898','111118','113260','100326','101585','115885','117332','112628','114662','112875','112135',
	'104966','115330','116604','117395','103377','114362','104576','103609','105167','112857','115234','115300','114723','114646','118780','121061','113188','111880','115006',
	'116052','105381','115520','106989','116740','116383','118366','118094','105856','105265','113720','115549','113918','113878','113935','115790','113873','113686','112820',
	'121018','111296','109367','108709','114896','117109','119020','107207','122422','121680','120154','119935','112024','113780','112681','111764','112914','114590','112019',
	'119885','119743','122320','121604','122329','121655','120865','114205','108905','111864','117758','111855','113595','112822','109017','106976','115018','119925','103995',
	'105415','104192','113090','115299','117007','112920','117538','112232','115833','115269','117572','106709','107716','111691','119760','111511','120325','120948','120584',
	'121030','121207','120102','120684','121354','120962','121380','113028','114691','113195','113554','115167','115154','112846','118815','119053','115229','118816','119890',
	'115042','113672','116170')
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
	('104922','111148','104779','105507','115378','115519','115489','117472','117419','115631','106287','113643','104493','104538','115971','116099','113387','105186','107469',
	'111430','108095','111137','115544','113609','111093','107910','105753','105643','106477','114494','113461','117327','108589','106566','110926','102901','112718','102784',
	'106953','114584','107807','106862','107307','112754','113522','107565','116475','119069','117230','108389','104402','104885','102534','115630','114650','115909','108096',
	'104086','108557','111954','107851','112072','107806','105525','119177','116401','104198','105669','107986','104268','112380','112609','112570','116690','107093','104758',
	'108722','104840','105717','111834','118849','105242','104666','113194','106719','107070','117351','117308','117481','107784','104865','111805','107356','106594','112622',
	'116135','107428','105446','105821','104322','108818','104596','115696','113743','112611','104340','110927','113957','113067','115202','114318','PF1205','103876','104275',
	'109342','106587','102633','103859','113463','115986','113439','106521','111200','108003','103044','108102','115732','112492','103784','105293','105005','104621','110546',
	'115156','116512','117303','104357','110785','104477','104281','108267','112067','106881','110807','109401','115906','115935','115656','105164','105165','105156','106721',
	'105182','107404','106805','111100','102961','108259','112477','110797','119132','117526','111366','111326','104872','102225','105163','105085','115237','113152','105221',
	'102755','106775','106081','105696','113758','113809','114548','104901','106898','111118','113260','100326','101585','115885','117332','112628','114662','112875','112135',
	'104966','115330','116604','117395','103377','114362','104576','103609','105167','112857','115234','115300','114723','114646','118780','121061','113188','111880','115006',
	'116052','105381','115520','106989','116740','116383','118366','118094','105856','105265','113720','115549','113918','113878','113935','115790','113873','113686','112820',
	'121018','111296','109367','108709','114896','117109','119020','107207','122422','121680','120154','119935','112024','113780','112681','111764','112914','114590','112019',
	'119885','119743','122320','121604','122329','121655','120865','114205','108905','111864','117758','111855','113595','112822','109017','106976','115018','119925','103995',
	'105415','104192','113090','115299','117007','112920','117538','112232','115833','115269','117572','106709','107716','111691','119760','111511','120325','120948','120584',
	'121030','121207','120102','120684','121354','120962','121380','113028','114691','113195','113554','115167','115154','112846','118815','119053','115229','118816','119890',
	'115042','113672','116170')
group by 
	customer_no