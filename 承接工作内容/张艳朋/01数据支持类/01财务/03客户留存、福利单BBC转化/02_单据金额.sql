--====================================================================================================================================

	select
		substr(sdt,1,6) as smonth,
		sum(if(business_type_code='1',sales_value,null)) as ripei_sales_value,
		sum(if(business_type_code='2',sales_value,null)) as fuli_sales_value,
		sum(if(business_type_code='6',sales_value,null)) as bbc_sales_value,
		count(distinct customer_no) as customer_cnt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20210131'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code !='4'  -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by
		substr(sdt,1,6)
		
		

	select
		substr(sdt,1,6) as smonth,
		sum(if(business_type_code='1',sales_value,null)) as ripei_sales_value,
		sum(if(business_type_code='2',sales_value,null)) as fuli_sales_value,
		sum(if(business_type_code='6',sales_value,null)) as bbc_sales_value,
		count(distinct customer_no) as customer_cnt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20210131'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('1','2','6')  -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by
		substr(sdt,1,6)