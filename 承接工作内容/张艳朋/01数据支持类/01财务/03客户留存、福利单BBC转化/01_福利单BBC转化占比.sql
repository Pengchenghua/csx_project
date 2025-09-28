--====================================================================================================================================

select
	a.smonth,
	count(distinct a.customer_no) as fuli_customer_cnt,
	count(distinct b.customer_no) as fuli_ripei_customer_cnt,
	count(distinct if(b.customer_no is null,a.customer_no,null)) as fuli_n_ripei_customer_cnt
from
	(
	select
		substr(sdt,1,6) as smonth,customer_no
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20210131'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('2')  -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by
		substr(sdt,1,6),customer_no
	) as a 
	left join
		(
		select
			substr(sdt,1,6) as smonth,customer_no
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20200101' and '20210131'
			and channel_code in('1','7','9') 
			and business_type_code in ('1') 
		group by 
			substr(sdt,1,6),customer_no
		) as b on b.customer_no=a.customer_no and b.smonth=a.smonth
group by 
	a.smonth
	
	
	
--====================================================================================================================================

select
	a.smonth,
	count(distinct a.customer_no) as bbc_customer_cnt,
	count(distinct b.customer_no) as bbc_ripei_customer_cnt,
	count(distinct if(b.customer_no is null,a.customer_no,null)) as bbc_n_ripei_customer_cnt
from
	(
	select
		substr(sdt,1,6) as smonth,customer_no
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20210131'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('6')  -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by
		substr(sdt,1,6),customer_no
	) as a 
	left join
		(
		select
			substr(sdt,1,6) as smonth,customer_no
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20200101' and '20210131'
			and channel_code in('1','7','9') 
			and business_type_code in ('1') 
		group by 
			substr(sdt,1,6),customer_no
		) as b on b.customer_no=a.customer_no and b.smonth=a.smonth
group by 
	a.smonth
		
		
		
