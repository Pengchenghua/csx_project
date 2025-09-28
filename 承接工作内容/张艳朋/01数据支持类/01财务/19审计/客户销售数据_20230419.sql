-- 122655 区分正向单和逆向单 区分调价和返利 到月份含税销售额 未税销售 未税毛利

		select 
			substr(sdt,1,6) smonth,customer_code,customer_name,if(refund_order_flag=0,'正向单','逆向单') as refund_order_flag_name,
			case when order_channel_code=4 then '客户返利管理' when order_channel_code=6 then '客户调价管理' else '非调价返利' end as order_channel_name,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<=regexp_replace(to_date(date_sub(current_date(),1)),'-','')
			and channel_code in('1','7','9')
			-- and business_type_code in (1,2,6) 
			and customer_code='122655'
		group by 
			substr(sdt,1,6),customer_code,customer_name,if(refund_order_flag=0,'正向单','逆向单'),
			case when order_channel_code=4 then '客户返利管理' when order_channel_code=6 then '客户调价管理' else '非调价返利' end
			
			
			

-- 临时导数
		select 
			substr(sdt,1,6) smonth,customer_code,customer_name,sub_customer_code,sub_customer_name,
			sum(sale_amt) as sale_amt,
			sum(sale_amt_no_tax) sale_amt_no_tax,
			sum(profit_no_tax) as profit_no_tax
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20190101' and sdt<=regexp_replace(to_date(date_sub(current_date(),1)),'-','')
			and channel_code in('1','7','9')
			-- and business_type_code in (1,2,6) 
			and customer_code='109460'
		group by 
			substr(sdt,1,6),customer_code,customer_name,sub_customer_code,sub_customer_name