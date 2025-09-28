--===========================================================================================================================================================================
--月度新客数
	select
		b.province_name,
		b.city_group_name,
		a.business_type_name,
		a.first_month,
		count(distinct a.customer_no) as customer_cnt
	from
		(
		select 
			customer_no,
			business_type_name,
			substr(sdt,1,6) as first_month
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210101' and '20211231'
			and channel_code in ('1','7','9')
			and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--group by customer_no,business_type_name
		) a 
		left join
			(
			select
				customer_no,customer_name,sales_id,work_no,sales_name,province_code,province_name,sales_region_code,sales_region_name,city_group_code,city_group_name
			from
				csx_dw.dws_crm_w_a_customer
			where
				sdt='current'
			)b on b.customer_no=a.customer_no
	--where
	--	a.first_month>='202101'
	group by 
		b.province_name,
		b.city_group_name,
		a.business_type_name,
		a.first_month	
;
--===========================================================================================================================================================================
--21年新客收入
select
	c.province_name,
	c.city_group_name,
	a.business_type_name,
	sum(a.sales_value) as sales_value
from
	(
	select 
		customer_no,
		business_type_name,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210101' and '20211231'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		customer_no,
		business_type_name
	)a
	left join
		(
		select
			customer_no,customer_name,sales_id,work_no,sales_name,province_code,province_name,sales_region_code,sales_region_name,city_group_code,city_group_name
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt='current'
		)c on c.customer_no=a.customer_no
--where b.first_month>='202101'
group by 
	c.province_name,
	c.city_group_name,
	a.business_type_name
;
--===========================================================================================================================================================================
--21年累计新客数（每月履约新客加总），所有21年新增客户，在新增月份及之后履约月份分别每月都算一次
select
	c.province_name,
	c.city_group_name,
	a.business_type_name,
	sum(a.month_cnt) as month_cnt
from
	(
	select 
		customer_no,
		business_type_name,
		count(distinct substr(sdt,1,6)) as month_cnt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210101' and '20211231'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		customer_no,
		business_type_name
	)a
	left join
		(
		select
			customer_no,customer_name,sales_id,work_no,sales_name,province_code,province_name,sales_region_code,sales_region_name,city_group_code,city_group_name
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt='current'
		)c on c.customer_no=a.customer_no
-- where b.first_month>='202101'
group by 
	c.province_name,
	c.city_group_name,
	a.business_type_name
;
			
		
	