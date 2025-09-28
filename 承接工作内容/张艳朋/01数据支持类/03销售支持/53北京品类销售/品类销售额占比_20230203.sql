select
	a.customer_code,a.customer_name,a.classify_middle_name,a.sale_amt/sum(a.sale_amt)over(partition by a.customer_code) as sale_amt_rate,a.sale_amt,a.profit
from
	(
	select
		a.customer_code,b.customer_name,c.classify_middle_name,sum(a.sale_amt) as sale_amt,sum(a.profit) as profit
	from
		(
		select
			sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax,goods_code
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20220101' and sdt<='20221231'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and customer_code in ('124576','124274','123622','120900','119561','119019','118996','130754','129635','120043','119019','120900','124121','124103','124109','118366','118287','118366','125885','124138','123029','119422')
		) a 
		left join
			(
			select
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
				sales_user_number,sales_user_name,customer_address_full
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) b on b.customer_code=a.customer_code
		left join
			(
			select
				goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
			from
				csx_dim.csx_dim_basic_goods
			where
				sdt='current'
			) c on c.goods_code=a.goods_code
	group by 
		a.customer_code,b.customer_name,c.classify_middle_name
	) a 