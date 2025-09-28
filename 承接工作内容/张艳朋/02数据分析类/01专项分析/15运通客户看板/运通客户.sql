
drop table if exists csx_analyse_tmp.csx_analyse_tmp_yuntong_sale;
create table csx_analyse_tmp.csx_analyse_tmp_yuntong_sale
as 
select
	a.smonth,a.sdt,a.performance_province_name,a.performance_city_name,a.business_type_name,a.customer_code,d.customer_name,b.business_division_name,
	b.purchase_group_code,b.purchase_group_name,b.classify_large_code,b.classify_large_name,b.classify_middle_code,b.classify_middle_name,
	b.classify_small_code,b.classify_small_name,a.goods_code,b.goods_name,tiaojia_type,fanli_type,delivery_type_name,inventory_dc_code,types,order_channel_detail_name,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit) profit
from 
	(
	select 
		performance_province_name,performance_city_name,sdt,substr(sdt,1,6) smonth,
	 	business_type_name,a.business_type_code,customer_code,
		if(order_channel_code=6 ,'是','否') tiaojia_type,
		if(order_channel_code=4 ,'是','否') fanli_type,
		delivery_type_name,goods_code,a.inventory_dc_code,
		if(c.shop_code is null,'否','是') types,order_channel_detail_name,
		sum(sale_amt)as sale_amt,
		sum(profit)as profit,		
		sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty
	from 
		(
		select 
			* 
	    from 
			csx_dws.csx_dws_sale_detail_di 
	    where 
			sdt >= '20230101' and sdt <= '20230628'
			and channel_code in('1','7','9')
			and customer_code in ('228190','228748','229231','228516')
		) a
		left join 
			( 
	        select 
				distinct shop_code 
			from 
				csx_dim.csx_dim_shop 
			where 
				sdt='current' and shop_low_profit_flag=1  
			)c on a.inventory_dc_code = c.shop_code
    group by 
		performance_province_name,
		performance_city_name,
		sdt,substr(sdt,1,6),
		business_type_name,business_type_code,
		customer_code,
		a.inventory_dc_code,
		if(c.shop_code is null,'否','是')
		,if(order_channel_code=6 ,'是','否')
		,if(order_channel_code=4 ,'是','否')
		,delivery_type_name,
		goods_code,order_channel_detail_name
	)a  
	left join (select *  from csx_dim.csx_dim_basic_goods where sdt = 'current') b on b.goods_code = a.goods_code 
	left join  
		(
		select
			customer_code,customer_name
		from  
			csx_dim.csx_dim_crm_customer_info 
		where 
			sdt='current'
		)d on d.customer_code=a.customer_code
group by 
	a.smonth,a.sdt,a.performance_province_name,a.performance_city_name,a.business_type_name,a.customer_code,d.customer_name,b.business_division_name,
	b.purchase_group_code,b.purchase_group_name,b.classify_large_code,b.classify_large_name,b.classify_middle_code,b.classify_middle_name,b.classify_small_code,
	b.classify_small_name,a.goods_code,b.goods_name,tiaojia_type,fanli_type,delivery_type_name,inventory_dc_code,types,order_channel_detail_name

;
select * from csx_analyse_tmp.csx_analyse_tmp_yuntong_sale

select * from csx_analyse.csx_analyse_fr_oms_complaint_detail_di where sdt>='20230501' and customer_code in ('228190','228748','229231','228516')

	select 
		customer_code,customer_name,credit_limit,receivable_amount,claim_unclose_bill_amount_all,
		cast(credit_limit as decimal(20,4))-(cast(receivable_amount as decimal(20,4))-cast(claim_unclose_bill_amount_all as decimal(20,4))) as credit_sy
	from 
		csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
	where 
		sdt='20230628' 
		and customer_code in ('228190','228748','229231','228516')