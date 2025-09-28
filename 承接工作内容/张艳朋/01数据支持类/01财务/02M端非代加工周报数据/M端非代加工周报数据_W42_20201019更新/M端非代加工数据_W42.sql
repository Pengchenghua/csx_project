					
			select
				a.sdt,
				a.province_name, 
				a.dc_code,
				a.dc_name,
				a.customer_no,
				a.customer_name,
				b.sales_belong_flag,
				a.department_code,
				a.department_name,
				a.goods_code,
				a.goods_name,
				a.sales_value,
				a.sales_cost,
				a.profit,
				a.profit/a.sales_value as profit_rate
			from	
				(	
				select
					sdt,
					province_name, 
					dc_code,
					dc_name,
					customer_no,
					customer_name,					
					department_code,
					department_name,
					goods_code,
					goods_name,
					sum(sales_value) as sales_value,
					sum(sales_cost) as sales_cost,
					sum(profit) as profit
				from 
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between '20201001' and '20201018'
					and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
					and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC 
				group by
					sdt,
					province_name, 
					dc_code,
					dc_name,
					customer_no,
					customer_name,					
					department_code,
					department_name,
					goods_code,
					goods_name
				) as a 
				left join
					(
					select 
						shop_id,company_code,sales_belong_flag
					from 
						csx_dw.dws_basic_w_a_csx_shop_m
					where 
						sdt = 'current'
					) b on a.customer_no = concat('S', b.shop_id)
					
					
					
					
					
				select
					sdt,
					province_name, 
					sum(sales_value) as sales_value, 
					sum(profit) as profit
				from 
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between '20201003' and '20201016'
					and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
					and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC 
					--and province_name='福建省'
				group by
					sdt,province_name
					
					