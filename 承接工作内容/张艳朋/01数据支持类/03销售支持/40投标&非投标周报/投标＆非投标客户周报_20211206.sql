-- 全国总计			
select
	'全国总计' as b_type,
	a.business_type_name,
	count(distinct a.customer_no) as customer_cnt,
	sum(a.sales_value) as sales_value,
	--投标非投标客户数
	count(distinct if(b.business_type_name='投标',a.customer_no,null)) as tb_customer_cnt,
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.customer_no) as tb_customer_rate,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null)) as ftb_customer_cnt,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.customer_no) as ftb_customer_rate,
	--投标非投标履约金额
	sum(if(b.business_type_name='投标',a.sales_value,null)) as tb_sales_value,
	sum(if(b.business_type_name='投标',a.sales_value,null))/sum(a.sales_value) as tb_sales_value_rate,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null)) as ftb_sales_value,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/sum(a.sales_value) as ftb_sales_value_rate,
	--新履约客户投标非投标客户数
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_tb_customer_rate,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_ftb_customer_rate,
	--新履约客户投标非投标履约金额
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_tb_sales_value_rate,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_ftb_sales_value_rate		
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20211201' and '20211205'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and province_name !='BBC'
	)a
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on b.customer_no=a.customer_no	
	left join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and first_order_date between '20211201' and '20211205'
		) c on c.customer_no=a.customer_no
group by 
	a.business_type_name
	
union all

-- 全国合计	
select
	'全国总计' as b_type,
	'合计' as business_type_name,
	count(distinct a.customer_no) as customer_cnt,
	sum(a.sales_value) as sales_value,
	--投标非投标客户数
	count(distinct if(b.business_type_name='投标',a.customer_no,null)) as tb_customer_cnt,
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.customer_no) as tb_customer_rate,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null)) as ftb_customer_cnt,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.customer_no) as ftb_customer_rate,
	--投标非投标履约金额
	sum(if(b.business_type_name='投标',a.sales_value,null)) as tb_sales_value,
	sum(if(b.business_type_name='投标',a.sales_value,null))/sum(a.sales_value) as tb_sales_value_rate,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null)) as ftb_sales_value,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/sum(a.sales_value) as ftb_sales_value_rate,
	--新履约客户投标非投标客户数
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_tb_customer_rate,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_ftb_customer_rate,
	--新履约客户投标非投标履约金额
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_tb_sales_value_rate,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_ftb_sales_value_rate		
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20211201' and '20211205'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and province_name !='BBC'
	)a
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on b.customer_no=a.customer_no	
	left join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and first_order_date between '20211201' and '20211205'
		) c on c.customer_no=a.customer_no
;

--==============================================================================================================================================================================
-- 全国平均		
select
	'全国平均' as b_type,
	a.business_type_name,
	count(distinct a.customer_no)/count(distinct a.province_name) as avg_customer_cnt,
	sum(a.sales_value)/count(distinct a.province_name) as avg_sales_value,
	--投标非投标客户数
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.province_name) as avg_tb_customer_cnt,
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.customer_no) as tb_customer_rate,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.province_name) as avg_ftb_customer_cnt,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.customer_no) as ftb_customer_rate,
	--投标非投标履约金额
	sum(if(b.business_type_name='投标',a.sales_value,null))/count(distinct a.province_name) as avg_tb_sales_value,
	sum(if(b.business_type_name='投标',a.sales_value,null))/sum(a.sales_value) as tb_sales_value_rate,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/count(distinct a.province_name) as avg_ftb_sales_value,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/sum(a.sales_value) as ftb_sales_value_rate,
	--新履约客户投标非投标客户数
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct a.province_name) as avg_new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_tb_customer_rate,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct a.province_name) as avg_new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_ftb_customer_rate,
	--新履约客户投标非投标履约金额
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/count(distinct a.province_name) as avg_new_tb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_tb_sales_value_rate,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/count(distinct a.province_name) as avg_new_ftb_sales_value,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_ftb_sales_value_rate		
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20211201' and '20211205'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and province_name !='BBC'
		--and province_name ='福建省'
	)a
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on b.customer_no=a.customer_no	
	left join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and first_order_date between '20211201' and '20211205'
		) c on c.customer_no=a.customer_no
group by 
	a.business_type_name

union all

-- 全国平均合计		
select
	'全国平均' as b_type,
	'合计' as business_type_name,
	count(distinct a.customer_no)/count(distinct a.province_name) as avg_customer_cnt,
	sum(a.sales_value)/count(distinct a.province_name) as avg_sales_value,
	--投标非投标客户数
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.province_name) as avg_tb_customer_cnt,
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.customer_no) as tb_customer_rate,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.province_name) as avg_ftb_customer_cnt,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.customer_no) as ftb_customer_rate,
	--投标非投标履约金额
	sum(if(b.business_type_name='投标',a.sales_value,null))/count(distinct a.province_name) as avg_tb_sales_value,
	sum(if(b.business_type_name='投标',a.sales_value,null))/sum(a.sales_value) as tb_sales_value_rate,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/count(distinct a.province_name) as avg_ftb_sales_value,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/sum(a.sales_value) as ftb_sales_value_rate,
	--新履约客户投标非投标客户数
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct a.province_name) as avg_new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_tb_customer_rate,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct a.province_name) as avg_new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_ftb_customer_rate,
	--新履约客户投标非投标履约金额
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/count(distinct a.province_name) as avg_new_tb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_tb_sales_value_rate,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/count(distinct a.province_name) as avg_new_ftb_sales_value,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_ftb_sales_value_rate		
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20211201' and '20211205'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and province_name !='BBC'
		--and province_name ='福建省'
	)a
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on b.customer_no=a.customer_no	
	left join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and first_order_date between '20211201' and '20211205'
		) c on c.customer_no=a.customer_no
;

--==============================================================================================================================================================================
-- 省份		
select
	b.province_name,
	a.business_type_name,
	count(distinct a.customer_no) as customer_cnt,
	sum(a.sales_value) as sales_value,
	--投标非投标客户数
	count(distinct if(b.business_type_name='投标',a.customer_no,null)) as tb_customer_cnt,
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.customer_no) as tb_customer_rate,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null)) as ftb_customer_cnt,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.customer_no) as ftb_customer_rate,
	--投标非投标履约金额
	sum(if(b.business_type_name='投标',a.sales_value,null)) as tb_sales_value,
	sum(if(b.business_type_name='投标',a.sales_value,null))/sum(a.sales_value) as tb_sales_value_rate,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null)) as ftb_sales_value,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/sum(a.sales_value) as ftb_sales_value_rate,
	--新履约客户投标非投标客户数
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_tb_customer_rate,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_ftb_customer_rate,
	--新履约客户投标非投标履约金额
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_tb_sales_value_rate,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_ftb_sales_value_rate		
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20211201' and '20211205'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and province_name !='BBC'
		--and province_name ='福建省'
	)a
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on b.customer_no=a.customer_no	
	left join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and first_order_date between '20211201' and '20211205'
		) c on c.customer_no=a.customer_no
group by 
	b.province_name,
	a.business_type_name

union all

-- 省份合计		
select
	b.province_name,
	'合计' as business_type_name,
	count(distinct a.customer_no) as customer_cnt,
	sum(a.sales_value) as sales_value,
	--投标非投标客户数
	count(distinct if(b.business_type_name='投标',a.customer_no,null)) as tb_customer_cnt,
	count(distinct if(b.business_type_name='投标',a.customer_no,null))/count(distinct a.customer_no) as tb_customer_rate,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null)) as ftb_customer_cnt,
	count(distinct if(b.business_type_name !='投标' or b.business_type_name is null,a.customer_no,null))/count(distinct a.customer_no) as ftb_customer_rate,
	--投标非投标履约金额
	sum(if(b.business_type_name='投标',a.sales_value,null)) as tb_sales_value,
	sum(if(b.business_type_name='投标',a.sales_value,null))/sum(a.sales_value) as tb_sales_value_rate,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null)) as ftb_sales_value,
	sum(if(b.business_type_name !='投标' or b.business_type_name is null,a.sales_value,null))/sum(a.sales_value) as ftb_sales_value_rate,
	--新履约客户投标非投标客户数
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_tb_customer_rate,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null))/count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_ftb_customer_rate,
	--新履约客户投标非投标履约金额
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_tb_sales_value_rate,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null))/sum(if(c.customer_no is not null,a.sales_value,null)) as new_ftb_sales_value_rate		
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20211201' and '20211205'
		and channel_code in ('1','7','9')
		and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and province_name !='BBC'
		--and province_name ='福建省'
	)a
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on b.customer_no=a.customer_no	
	left join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and first_order_date between '20211201' and '20211205'
		) c on c.customer_no=a.customer_no
group by 
	b.province_name
;
