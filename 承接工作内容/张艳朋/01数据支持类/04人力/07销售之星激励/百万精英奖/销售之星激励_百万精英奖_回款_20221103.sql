--==================================================================================================================================================================================
set current_start_day ='20220801';

set current_end_day ='20220930';

set close_bill_end_day ='20221031';


--01_销售员激励案_Q3福利激励案_百万精英奖

insert overwrite directory '/tmp/zhangyanpeng/20220815_01' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	b.work_no,
	b.sales_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn,
	sum(c.payment_amount) as payment_amount
from 
	(
	select 
		customer_no,
		sum(sales_value)as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
	group by 
		customer_no
	having
		sum(sales_value)>=10000
		and sum(profit)/abs(sum(sales_value))>0.03
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,province_name,city_group_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no
	--join -- 员工信息
	--	(
	--	select 
	--		employee_code,employee_name,begin_date,leader_code,leader_name
	--	from 
	--		csx_dw.dws_basic_w_a_employee_org_m
	--	where 
	--		sdt = ${hiveconf:current_end_day}
	--		and emp_status='on'
	--	) c on c.employee_code=a.work_no
	left join
		(
		select 
			customer_code,
			sum(payment_amount) as payment_amount       
        from 
			csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
        where 
			regexp_replace(to_date(paid_time),'-','') >=${hiveconf:current_start_day}
			and regexp_replace(to_date(paid_time),'-','') <=${hiveconf:close_bill_end_day}
			and regexp_replace(to_date(happen_date),'-','')>=${hiveconf:current_start_day}
			and regexp_replace(to_date(happen_date),'-','')<=${hiveconf:current_end_day}
			and is_deleted ='0'
        group by customer_code
		) c on c.customer_code=a.customer_no			
group by 
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	b.work_no,
	b.sales_name
having
	sum(a.sales_value)>=1000000
;

--===================================================================================================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20220815_02' row format delimited fields terminated by '\t'

select 
	* 
from
	(
	select
		concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
		b.sales_region_name,
		b.province_name,
		b.city_group_name,
		a.customer_no,
		b.customer_name,
		b.work_no,
		b.sales_name,
		a.sales_value,
		a.profit,
		a.profit/abs(a.sales_value) as profit_rate,
		row_number() over(order by a.sales_value desc) as rn,
		c.payment_amount,
		sum(a.sales_value)over(partition by b.work_no) as sales_total_sales_value
	from 
		(
		select 
			customer_no,
			sum(sales_value)as sales_value,
			sum(profit) as profit
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and goods_code not in ('8718','8708','8649')
		group by 
			customer_no
		having
			sum(sales_value)>=10000
			and sum(profit)/abs(sum(sales_value))>0.03
		) a  
		left join   
			(
			select 
				customer_no,customer_name,sales_region_name,province_name,city_group_name,work_no,sales_name
			from 
				csx_dw.dws_crm_w_a_customer 
			where 
				sdt=${hiveconf:current_end_day}
			) b on b.customer_no=a.customer_no
		--join -- 员工信息
		--	(
		--	select 
		--		employee_code,employee_name,begin_date,leader_code,leader_name
		--	from 
		--		csx_dw.dws_basic_w_a_employee_org_m
		--	where 
		--		sdt = ${hiveconf:current_end_day}
		--		and emp_status='on'
		--	) c on c.employee_code=a.work_no
		left join
			(
			select 
				customer_code,
				sum(payment_amount) as payment_amount       
			from 
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where 
				regexp_replace(to_date(paid_time),'-','') >=${hiveconf:current_start_day}
				and regexp_replace(to_date(paid_time),'-','') <=${hiveconf:close_bill_end_day}
				and regexp_replace(to_date(happen_date),'-','')>=${hiveconf:current_start_day}
				and regexp_replace(to_date(happen_date),'-','')<=${hiveconf:current_end_day}
				and is_deleted ='0'
			group by customer_code
			) c on c.customer_code=a.customer_no	
	) a 
where
	sales_total_sales_value>=1000000
order by 
	rn
;

-- ==============================================================================================================================================================================

set current_start_day ='20220801';

set current_end_day ='20220930';

set close_bill_end_day ='20221031';

insert overwrite directory '/tmp/zhangyanpeng/20220815_03' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	b.work_no,
	b.sales_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	row_number() over(order by sum(a.sales_value) desc) as rn,
	sum(payment_amount) as payment_amount
from 
	(
	select 
		a.customer_no,
		sum(a.sales_value)as sales_value,
		sum(a.profit) as profit,
		sum(payment_amount) as payment_amount
	from 
		(
		select
			customer_no,
			if(business_type_code in ('2'),order_no,origin_order_no) as order_no,
			-- business_type_name,
			sum(sales_value)as sales_value,
			sum(profit) as profit	
		from
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and goods_code not in ('8718','8708','8649')
		group by 
			customer_no,if(business_type_code in ('2'),order_no,origin_order_no) -- business_type_name
		) a 
		left join
			(
			select
				case when substr(close_bill_no,1,2)='B2' then substr(close_bill_no,2,16)
					when substr(close_bill_no,1,2)='R2' then substr(close_bill_no,2,16)
					when substr(close_bill_no,1,2)='OC' then substr(close_bill_no,1,13)
					else close_bill_no end as close_bill_no,customer_code,
				sum(payment_amount) as payment_amount
			from
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where
				sdt>='20220801'
				and regexp_replace(to_date(paid_time),'-','') >=${hiveconf:current_start_day}
				and regexp_replace(to_date(paid_time),'-','') <=${hiveconf:close_bill_end_day}
				and is_deleted ='0'
				and invoice_no is not null
			group by 
				case when substr(close_bill_no,1,2)='B2' then substr(close_bill_no,2,16)
					when substr(close_bill_no,1,2)='R2' then substr(close_bill_no,2,16)
					when substr(close_bill_no,1,2)='OC' then substr(close_bill_no,1,13)
					else close_bill_no end,customer_code
			) b on b.close_bill_no=a.order_no and b.customer_code=a.customer_no
	group by 
		a.customer_no
	having
		sum(a.sales_value)>=10000
		and sum(a.profit)/abs(sum(a.sales_value))>0.03
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,province_name,city_group_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no
	--join -- 员工信息
	--	(
	--	select 
	--		employee_code,employee_name,begin_date,leader_code,leader_name
	--	from 
	--		csx_dw.dws_basic_w_a_employee_org_m
	--	where 
	--		sdt = ${hiveconf:current_end_day}
	--		and emp_status='on'
	--	) c on c.employee_code=a.work_no
	-- left join
	-- 	(
	-- 	select 
	-- 		customer_code,
	-- 		sum(payment_amount) as payment_amount       
    --     from 
	-- 		csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
    --     where 
	-- 		regexp_replace(to_date(paid_time),'-','') >=${hiveconf:current_start_day}
	-- 		and regexp_replace(to_date(paid_time),'-','') <=${hiveconf:close_bill_end_day}
	-- 		and regexp_replace(to_date(happen_date),'-','')>=${hiveconf:current_start_day}
	-- 		and regexp_replace(to_date(happen_date),'-','')<=${hiveconf:current_end_day}
	-- 		and is_deleted ='0'
    --     group by customer_code
	-- 	) c on c.customer_code=a.customer_no			
group by 
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	b.work_no,
	b.sales_name
having
	sum(a.sales_value)>=1000000
;

--===================================================================================================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20220815_04' row format delimited fields terminated by '\t'

select 
	* 
from
	(
	select
		concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
		b.sales_region_name,
		b.province_name,
		b.city_group_name,
		a.customer_no,
		b.customer_name,
		b.work_no,
		b.sales_name,
		a.sales_value,
		a.profit,
		a.profit/abs(a.sales_value) as profit_rate,
		row_number() over(order by a.sales_value desc) as rn,
		a.payment_amount,
		sum(a.sales_value)over(partition by b.work_no) as sales_total_sales_value
	from 
		(
		select 
			a.customer_no,
			sum(a.sales_value)as sales_value,
			sum(a.profit) as profit,
			sum(payment_amount) as payment_amount
		from 
			(
			select
				customer_no,
				if(business_type_code in ('2'),order_no,origin_order_no) as order_no,
				sum(sales_value)as sales_value,
				sum(profit) as profit
			from
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
				and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and goods_code not in ('8718','8708','8649')
			group by 
				customer_no,if(business_type_code in ('2'),order_no,origin_order_no)
			) a 
			left join
				(
				select
					case when substr(close_bill_no,1,2)='B2' then substr(close_bill_no,2,16)
						when substr(close_bill_no,1,2)='R2' then substr(close_bill_no,2,16)
						when substr(close_bill_no,1,2)='OC' then substr(close_bill_no,1,13)
						else close_bill_no end as close_bill_no,customer_code,
					sum(payment_amount) as payment_amount
				from
					csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
				where
					sdt>='20220801'
					and regexp_replace(to_date(paid_time),'-','') >=${hiveconf:current_start_day}
					and regexp_replace(to_date(paid_time),'-','') <=${hiveconf:close_bill_end_day}
					and is_deleted ='0'
					and invoice_no is not null
				group by 
					case when substr(close_bill_no,1,2)='B2' then substr(close_bill_no,2,16)
						when substr(close_bill_no,1,2)='R2' then substr(close_bill_no,2,16)
						when substr(close_bill_no,1,2)='OC' then substr(close_bill_no,1,13)
						else close_bill_no end,customer_code
				) b on b.close_bill_no=a.order_no and b.customer_code=a.customer_no
		group by 
			a.customer_no
		having
			sum(a.sales_value)>=10000
			and sum(a.profit)/abs(sum(a.sales_value))>0.03
		) a  
		left join   
			(
			select 
				customer_no,customer_name,sales_region_name,province_name,city_group_name,work_no,sales_name
			from 
				csx_dw.dws_crm_w_a_customer 
			where 
				sdt=${hiveconf:current_end_day}
			) b on b.customer_no=a.customer_no
		--join -- 员工信息
		--	(
		--	select 
		--		employee_code,employee_name,begin_date,leader_code,leader_name
		--	from 
		--		csx_dw.dws_basic_w_a_employee_org_m
		--	where 
		--		sdt = ${hiveconf:current_end_day}
		--		and emp_status='on'
		--	) c on c.employee_code=a.work_no
		-- left join
		-- 	(
		-- 	select 
		-- 		customer_code,
		-- 		sum(payment_amount) as payment_amount       
		-- 	from 
		-- 		csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
		-- 	where 
		-- 		regexp_replace(to_date(paid_time),'-','') >=${hiveconf:current_start_day}
		-- 		and regexp_replace(to_date(paid_time),'-','') <=${hiveconf:close_bill_end_day}
		-- 		and regexp_replace(to_date(happen_date),'-','')>=${hiveconf:current_start_day}
		-- 		and regexp_replace(to_date(happen_date),'-','')<=${hiveconf:current_end_day}
		-- 		and is_deleted ='0'
		-- 	group by customer_code
		-- 	) c on c.customer_code=a.customer_no	
	) a 
where
	sales_total_sales_value>=1000000
order by 
	rn
;



			select
				customer_no,order_no,business_type_name,
				sum(sales_value)as sales_value,
				sum(profit) as profit
			from
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20220801' and '20220930'
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
				and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and goods_code not in ('8718','8708','8649')
				and customer_no='115380'
			group by 
				customer_no,order_no,business_type_name
				
				
				select
					close_bill_no,customer_code,
					sum(payment_amount) as payment_amount
				from
					csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
				where
					regexp_replace(to_date(paid_time),'-','') >='20220801'
					and regexp_replace(to_date(paid_time),'-','') <='20221031'
					and is_deleted ='0'
					and customer_code='115380'
				group by 
					close_bill_no,customer_code
