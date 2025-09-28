--云超销售数据_品类

	select
		province,
		--city,
		is_processing,
		manageclass_level2_name,
		sum(taxincluded_sale_mny)/10000 as taxincluded_sale_mny,
		sum(sale_mny)/10000 as sale_mny,
		sum(grossprofit_mny)/10000 as grossprofit_mny,
		sum(grossprofit_mny)/10000 as grossprofit_mny
	from
		csx_dw.report_fis_r_m_sales_detail
	where
		month between '202101' and '202110'
		and channel='商超'
		and business_type like '%云超%'
	group by 
		province,
		--city,
		is_processing,
		manageclass_level2_name		

