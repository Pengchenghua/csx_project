--==================================================================================================================================================================================
-- 个人奖项_福利BBC激励方案_百万精英奖

select
	concat('20230501-','20230619') as sdt_s,
	b.performance_region_name,
	b.performance_province_name,
	b.performance_city_name,
	b.sales_user_number,
	b.sales_user_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	row_number() over(order by sum(a.sale_amt) desc) as rn
from 
	(
	select 
		customer_code,
		sum(sale_amt)as sale_amt,
		sum(profit) as profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230501' and '20230619' -- regexp_replace(to_date(date_sub(current_date(),1)),'-','') -- '20221231'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in (2,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
	group by 
		customer_code
	having
		sum(sale_amt)>=10000
		and sum(profit)/abs(sum(sale_amt))>0.03
	) a  
	left join   
		(
		select 
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,sales_user_number,sales_user_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt='current' -- 'current'
		) b on b.customer_code=a.customer_code
group by 
	b.performance_region_name,
	b.performance_province_name,
	b.performance_city_name,
	b.sales_user_number,
	b.sales_user_name
-- having
-- 	sum(a.sale_amt)>=1000000
;

--===================================================================================================================================================================
-- 个人奖项_福利BBC激励方案_百万精英奖明细

select 
	sdt_s,performance_region_name,performance_province_name,performance_city_name,customer_code,customer_name,sales_user_number,sales_user_name,a.sale_amt,a.profit,profit_rate,
	user_number,guide_user_name
from
	(
	select
		concat('20230501-','20230619') as sdt_s,
		b.performance_region_name,
		b.performance_province_name,
		b.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_user_number,
		b.sales_user_name,
		a.sale_amt,
		a.profit,
		a.profit/abs(a.sale_amt) as profit_rate,
		row_number() over(order by a.sale_amt desc) as rn,
		sum(a.sale_amt)over(partition by b.sales_user_number) as sales_total_sale_amt,
		c.user_number,c.guide_user_name
	from 
		(
		select 
			customer_code,
			sum(sale_amt)as sale_amt,
			sum(profit) as profit
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between '20230501' and '20230619' -- regexp_replace(to_date(date_sub(current_date(),1)),'-','') -- '20221231'
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
			and business_type_code in (2,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and goods_code not in ('8718','8708','8649')
		group by 
			customer_code
		having
			sum(sale_amt)>=10000
			and sum(profit)/abs(sum(sale_amt))>0.03
		) a  
		left join   
			(
			select 
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,sales_user_number,sales_user_name
			from 
				csx_dim.csx_dim_crm_customer_info
			where 
				sdt='current' -- 'current'
			) b on b.customer_code=a.customer_code
		left join
			(
			select
				a.business_sign_time,a.business_number,a.customer_id,a.customer_code,a.customer_name,a.guide_user_id,a.guide_user_name,b.user_number
			from
				(
				select
					business_sign_time,business_number,customer_id,customer_code,customer_name,guide_user_id,guide_user_name,
					row_number() over(partition by customer_code order by business_sign_time desc) num --商机顺序
				from 
					csx_dim.csx_dim_crm_business_info
				where 
					sdt='current'
					and channel_code in('1','7','9')
					-- and business_type_code in(1) -- 日配业务
					and status=1  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
					and business_stage=5
					and regexp_replace(to_date(business_sign_time),'-','') between '20190101' and '20230619'
				) a 
				left join
					(
					select 
						user_id,user_number,user_name 
					from 
						-- csx_dw.dws_basic_w_a_user 
						csx_dim.csx_dim_uc_user
					where 
						sdt='current' 
						and delete_flag = '0'
					group by 
						user_id,user_number,user_name
					) b on b.user_id=a.guide_user_id
			where
				num=1
			) c on c.customer_code=a.customer_code
	) a 
where
	1=1
	-- and sales_total_sale_amt>=1000000
;