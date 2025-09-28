-- ================================================================================================================
-- 断约客户明细
select
	a.region_name,
	a.province_name,
	a.customer_no,
	b.customer_name,
	a.add_day,
	b.work_no,
	b.sales_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	coalesce(a.sales_value,0) as sales_value,
	coalesce(a.profit,0) as profit,
	coalesce(a.profit_rate,0) as profit_rate,
	coalesce(a.goods_cnt,0) as goods_cnt,
	coalesce(a.days_cnt) as days_cnt,
	(case when a.days_cnt=1 then '1次'
		when a.days_cnt>=2 and a.days_cnt<=3 then '2~3次'
		when a.days_cnt>=4 and a.days_cnt<=5 then '4~5次'
		when a.days_cnt>=6 and a.days_cnt<=10 then '6~10次'
		when a.days_cnt>10 then '10次以上'
		else '其他'
	end) as days_cnt_type
from
	(		
	select
		region_name,province_name,customer_no,add_day,sales_value,profit,profit_rate,goods_cnt,days_cnt
	from
		(
		select 
			region_name,province_name,customer_no,
			substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as add_day,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate,
			count(distinct goods_code) as goods_cnt,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and '20210310'
			and business_type_code in ('6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and province_name='福建省'
		group by 
			region_name,province_name,customer_no
		) tmp1
	where
		add_day>='201901'
		and add_day<='202012'	
	) a
	left join
		(
		select 
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		) b on b.customer_no = a.customer_no

