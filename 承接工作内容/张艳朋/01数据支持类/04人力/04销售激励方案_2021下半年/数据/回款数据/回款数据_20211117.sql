--=============================================================================================================================================================================
--销售员
select
	b.province_name,
	b.work_no,
	b.sales_name,
	sum(a.source_statement_amount) as source_statement_amount,
	sum(a.paid_amount) as paid_amount
from
	(
	select
		id,source_bill_no,customer_code,happen_date,source_statement_amount,order_time,statement_state,paid_amount,money_back_status,
		account_period_code,account_period_name,account_period_val,overdue_date,overdue_days,unpaid_amount,overdue_amount,source_sys
	from
		csx_dw.dwd_sss_r_d_source_bill
	where
		sdt>='20210701' 
		and sdt<='20210930'
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = '20210930'	
		) b on a.customer_code=b.customer_no	
group by
	b.province_name,
	b.work_no,
	b.sales_name
;	
--=============================================================================================================================================================================
--主管
select
	b.province_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	sum(a.source_statement_amount) as source_statement_amount,
	sum(a.paid_amount) as paid_amount
from
	(
	select
		id,source_bill_no,customer_code,happen_date,source_statement_amount,order_time,statement_state,paid_amount,money_back_status,
		account_period_code,account_period_name,account_period_val,overdue_date,overdue_days,unpaid_amount,overdue_amount,source_sys
	from
		csx_dw.dwd_sss_r_d_source_bill
	where
		sdt>='20210701' 
		and sdt<='20210930'
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = '20210930'	
		) b on a.customer_code=b.customer_no	
group by
	b.province_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name
;	

--==============================================================================================================================================================================
--销售员汇总
select
	a.work_no,a.sales_name,
	sum(a.sales_value)as sales_value,
	sum(a.profit) as profit,
	sum(a.front_profit) as front_profit,
	--sum(a.bill_amount) as bill_amount,
	--sum(a.payment_amount) as payment_amount,
	sum(a.paid_amount) as paid_amount
from
	(
	select 
		a.customer_no,a.work_no,a.sales_name,
		sum(a.sales_value)as sales_value,
		sum(a.profit) as profit,
		sum(a.front_profit) as front_profit,
		sum(b.bill_amount) as bill_amount,
		sum(b.payment_amount) as payment_amount,
		sum(b.paid_amount) as paid_amount
	from 
		(
		select 
			customer_no,work_no,sales_name,order_no,
			sum(sales_value)as sales_value,
			sum(profit) as profit,
			sum(front_profit) as front_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			((sdt between '20210701' and '20210731' and work_no not in ('80887605','80935770')) or (sdt between '20210801' and '20210930'))
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_name not rlike 'A|B|C|M' --虚拟销售
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			-- 20211012 剔除部分订单
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
		group by 
			customer_no,work_no,sales_name,order_no
		)a 
		left join
			(
			select
				close_bill_no,customer_code,
				sum(bill_amount) as bill_amount,
				sum(payment_amount) as payment_amount,
				sum(paid_amount) as paid_amount
			from
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where
				sdt>='20210701' 
				and sdt<='20211031'
				--and to_date(happen_date) between '2021-07-01' and '2021-09-30'
				and is_deleted=0
			group by 
				close_bill_no,customer_code
			) b on b.close_bill_no=a.order_no and b.customer_code=a.customer_no
	group by 
		a.customer_no,a.work_no,a.sales_name		
	having
		sum(a.sales_value)>=10000
		and sum(a.front_profit)/abs(sum(a.sales_value))>0
	) a 
group by 
	a.work_no,a.sales_name
;

--==============================================================================================================================================================================
--销售员明细

insert overwrite directory '/tmp/zhangyanpeng/1119_04' row format delimited fields terminated by '\t'

select
	a.customer_no,a.work_no,a.sales_name,a.order_no,
	a.sales_value,
	a.profit,
	a.front_profit,
	a.paid_amount,
	a.total_sales_value,
	a.total_front_profit
from
	(
	select 
		a.customer_no,a.work_no,a.sales_name,a.order_no,
		a.sales_value,
		a.profit,
		a.front_profit,
		b.paid_amount,
		sum(a.sales_value)over(partition by a.customer_no) as total_sales_value,
		sum(a.front_profit)over(partition by a.customer_no) as total_front_profit
	from 
		(
		select 
			customer_no,work_no,sales_name,order_no,
			sum(sales_value)as sales_value,
			sum(profit) as profit,
			sum(front_profit) as front_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			((sdt between '20210701' and '20210731' and work_no not in ('80887605','80935770')) or (sdt between '20210801' and '20210930'))
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_name not rlike 'A|B|C|M' --虚拟销售
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			-- 20211012 剔除部分订单
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
		group by 
			customer_no,work_no,sales_name,order_no
		)a 
		left join
			(
			select
				close_bill_no,customer_code,
				sum(bill_amount) as bill_amount,
				sum(payment_amount) as payment_amount,
				sum(paid_amount) as paid_amount
			from
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where
				sdt>='20210701' 
				and sdt<='20211031'
				--and to_date(happen_date) between '2021-07-01' and '2021-09-30'
				and is_deleted=0
			group by 
				close_bill_no,customer_code
			) b on b.close_bill_no=a.order_no and b.customer_code=a.customer_no
	) a 
where
	total_sales_value>=10000
	and total_front_profit/abs(total_sales_value)>0	
;


--==============================================================================================================================================================================
--主管汇总
select
	a.supervisor_work_no,a.supervisor_name,
	sum(a.sales_value)as sales_value,
	sum(a.profit) as profit,
	sum(a.front_profit) as front_profit,
	--sum(a.bill_amount) as bill_amount,
	--sum(a.payment_amount) as payment_amount,
	sum(a.paid_amount) as paid_amount
from
	(
	select 
		a.customer_no,a.supervisor_work_no,a.supervisor_name,
		sum(a.sales_value)as sales_value,
		sum(a.profit) as profit,
		sum(a.front_profit) as front_profit,
		sum(b.bill_amount) as bill_amount,
		sum(b.payment_amount) as payment_amount,
		sum(b.paid_amount) as paid_amount
	from 
		(
		select 
			customer_no,supervisor_work_no,supervisor_name,order_no,
			sum(sales_value)as sales_value,
			sum(profit) as profit,
			sum(front_profit) as front_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			((sdt between '20210701' and '20210731' and work_no not in ('80887605','80935770')) or (sdt between '20210801' and '20210930'))
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_name not rlike 'A|B|C|M' --虚拟销售
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			-- 20211012 剔除部分订单
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			and supervisor_work_no in ('80946212','80816799','80768089','80764642')
		group by 
			customer_no,supervisor_work_no,supervisor_name,order_no
		)a 
		left join
			(
			select
				close_bill_no,customer_code,
				sum(bill_amount) as bill_amount,
				sum(payment_amount) as payment_amount,
				sum(paid_amount) as paid_amount
			from
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where
				sdt>='20210701' 
				and sdt<='20211031'
				--and to_date(happen_date) between '2021-07-01' and '2021-09-30'
				and is_deleted=0
			group by 
				close_bill_no,customer_code
			) b on b.close_bill_no=a.order_no and b.customer_code=a.customer_no
	group by 
		a.customer_no,a.supervisor_work_no,a.supervisor_name		
	having
		sum(a.sales_value)>=10000
		and sum(a.front_profit)/abs(sum(a.sales_value))>0
	) a 
group by 
	a.supervisor_work_no,a.supervisor_name
;

--==============================================================================================================================================================================
--主管明细

insert overwrite directory '/tmp/zhangyanpeng/1119_05' row format delimited fields terminated by '\t'

select
	a.customer_no,a.supervisor_work_no,a.supervisor_name,a.order_no,
	a.sales_value,
	a.profit,
	a.front_profit,
	a.paid_amount,
	a.total_sales_value,
	a.total_front_profit
from
	(
	select 
		a.customer_no,a.supervisor_work_no,a.supervisor_name,a.order_no,
		a.sales_value,
		a.profit,
		a.front_profit,
		b.paid_amount,
		sum(a.sales_value)over(partition by a.customer_no) as total_sales_value,
		sum(a.front_profit)over(partition by a.customer_no) as total_front_profit
	from 
		(
		select 
			customer_no,supervisor_work_no,supervisor_name,order_no,
			sum(sales_value)as sales_value,
			sum(profit) as profit,
			sum(front_profit) as front_profit
		from
			csx_dw.dws_sale_r_d_detail
		where 
			((sdt between '20210701' and '20210731' and work_no not in ('80887605','80935770')) or (sdt between '20210801' and '20210930'))
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_name not rlike 'A|B|C|M' --虚拟销售
			and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
			-- 20210918 剔除部分客户
			and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
			'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
			'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
			-- 20210923 剔除部分客户
			and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
			-- 20211012 剔除部分订单
			and order_no not in (
			'OM21092500022852','OM21092500018389','OM21091600021970','OM21091800011804','OM21093000003441','OM21081800009334','OM21073000033690','OM21081800016201',
			'OM21072800045844','OM21072700002361','OM21072800045333','OM21072700002378','OM21072700002277','OM21072700002338','OM21091100000260','OM21082300007563')
			and supervisor_work_no in ('80946212','80816799','80768089','80764642')
		group by 
			customer_no,supervisor_work_no,supervisor_name,order_no
		)a 
		left join
			(
			select
				close_bill_no,customer_code,
				sum(bill_amount) as bill_amount,
				sum(payment_amount) as payment_amount,
				sum(paid_amount) as paid_amount
			from
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where
				sdt>='20210701' 
				and sdt<='20211031'
				--and to_date(happen_date) between '2021-07-01' and '2021-09-30'
				and is_deleted=0
			group by 
				close_bill_no,customer_code
			) b on b.close_bill_no=a.order_no and b.customer_code=a.customer_no
	) a 
where
	total_sales_value>=10000
	and total_front_profit/abs(total_sales_value)>0	
;