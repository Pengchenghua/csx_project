-- ================================================================================================================
-- 销售员新开客户特殊激励

-- insert overwrite directory '/tmp/zhangyanpeng/20210524_linshi_1' row format delimited fields terminated by '\t' 

-- select
-- 	province_name,work_no,sales_name,
-- 	sum(reward_amount) as reward_amount
-- from
-- 	(
	select
		a.province_name,b.work_no,b.sales_name,a.customer_no,b.customer_name,b.sign_time,a.sales_value,
		case when sales_value>=150000 and sales_value<300000 then '500'
			when sales_value>=300000 and sales_value<500000 then '800'
			when sales_value>=500000 and sales_value<1000000 then '1200'
			when sales_value>=1000000 and sales_value<2000000 then '1500'
			when sales_value>=2000000 and sales_value<3000000 then '2500'
			when sales_value>=3000000 then '3500'
			else '0'
		end as reward_amount
	from
		(
		select 
			province_name,customer_no,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210301' and '20210531'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			province_name,customer_no
		) a
		join
			(
			select 
				customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_time
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
				and customer_no<>''	
				and regexp_replace(substr(sign_time,1,7),'-','') ='202103' -- 202103月新签
			) b on b.customer_no = a.customer_no
-- 	) t1
-- group by 
-- 	province_name,work_no,sales_name
-- having
-- 	sum(reward_amount)>0