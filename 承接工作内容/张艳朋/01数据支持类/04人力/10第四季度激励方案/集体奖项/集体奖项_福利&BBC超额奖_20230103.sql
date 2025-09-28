--==================================================================================================================================================================================
-- 集体奖项_福利&BBC超额奖

	
	select 
		concat('20221001-','20221231') as sdt_s,
		performance_region_name,
		if(performance_province_name in ('上海松江','上海宝山'),'上海市',performance_province_name) as performance_province_name,
		sum(sale_amt)as sale_amt,
		sum(profit) as profit,
		sum(if(business_type_code=2,sale_amt,0)) as fl_sale_amt,
		sum(if(business_type_code=6,sale_amt,0)) as bbc_sale_amt,
		sum(if(business_type_code=2,profit,0)) as fl_profit,
		sum(if(business_type_code=6,profit,0)) as bbc_profit	
	from 
		-- csx_dw.dws_sale_r_d_detail 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20221001' and '20221231'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in (2,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
		and performance_province_name !='平台-B'
	group by 
		performance_region_name,if(performance_province_name in ('上海松江','上海宝山'),'上海市',performance_province_name)

;

--===================================================================================================================================================================
-- 个人奖项_福利BBC激励方案_百万精英奖明细

select 
	sdt_s,performance_region_name,performance_province_name,performance_city_name,customer_code,customer_name,sales_user_number,sales_user_name,a.sale_amt,a.profit,profit_rate
from
	(
	select
		concat('20221001-',regexp_replace(to_date(date_sub(current_date(),1)),'-','')) as sdt_s,
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
		sum(a.sale_amt)over(partition by b.sales_user_number) as sales_total_sale_amt
	from 
		(
		select 
			customer_code,
			sum(sale_amt)as sale_amt,
			sum(profit) as profit
		from 
			-- csx_dw.dws_sale_r_d_detail 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between '20221001' and regexp_replace(to_date(date_sub(current_date(),1)),'-','')
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
				-- csx_dw.dws_crm_w_a_customer 
				csx_dim.csx_dim_crm_customer_info
			where 
				sdt='current'
			) b on b.customer_code=a.customer_code
	) a 
where
	sales_total_sale_amt>=1000000
;
