

	select
		sales_user_number,sales_user_name,
		substr(sdt,1,4) as syear,
		business_type_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20221124'
		and channel_code in('1','7','9')
		and business_type_code in (1,2,6)
		and sales_user_number in ('81111210','81051686','81027386','81014012','80939468','80796120','81014929','80946351','81165491','80992029','81096879','81062397',
		'81018608','81013079','80992518','80963376','80958894','80928776','80928776','81164225','81160930','80946479','80946479','80928418','80887789','80887786',
		'81124064','81078424','81075241','81017947','81013168','81001920','80971381','80948175','81016757','80989132','80980614','80958648','80929704','80927693',
		'80924363','80895349','80793145','80007454','81180572','80958219','80941964','80879367','80848179','80835420','80803749','81168682','80956511','80930345',
		'80927654','80683693','80978539','81027603','80943162','81163599','81143231','81143442','81167272','81145971','81111614','81155478','81103668','80902387',
		'81048704','81166841','80913408','80943162','80935770','80972242','80913079'
		)
	group by 
		sales_user_number,sales_user_name,
		substr(sdt,1,4),
		business_type_name	


	select
		sales_user_number,sales_user_name,
		substr(sdt,1,4) as syear,
		business_type_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20221124'
		and channel_code in('1','7','9')
		and business_type_code in (1,2,6)
		and sales_user_number in ('81039868')
	group by 
		sales_user_number,sales_user_name,
		substr(sdt,1,4),
		business_type_name				