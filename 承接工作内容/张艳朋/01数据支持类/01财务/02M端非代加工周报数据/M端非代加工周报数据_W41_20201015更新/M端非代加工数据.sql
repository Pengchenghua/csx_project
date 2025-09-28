				select
					sdt,
					province_name, 
					sum(sales_value) as sales_value, 
					sum(profit) as profit
				from 
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between '20200926' and '20201009'
					and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
					and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC 
					--and province_name='北京市'
				group by
					sdt,province_name
					
					
					
					
					
					
					
				select
					sdt,
					department_code,
					department_name,
					province_name, 
					sum(sales_value) as sales_value, 
					sum(profit) as profit
				from 
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between '20200926' and '20201012'
					and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
					and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC 
					and province_name='北京市'
				group by
					sdt,department_code,department_name,province_name
					
					