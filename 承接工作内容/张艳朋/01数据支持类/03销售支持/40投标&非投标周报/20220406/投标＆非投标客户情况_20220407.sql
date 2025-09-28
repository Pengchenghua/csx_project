-- 开始日期
set current_start_day = '20210101';

-- 结束日期
set current_end_day = '20211231';

-- 省区			
select
	a.province_name,
	--非投标 新履约客户数
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_customer_cnt,
	--非投标 新履约金额
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null,a.sales_value,null)) as new_sales_value
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		--and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--and province_name !='BBC'
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
			and first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		) c on c.customer_no=a.customer_no
group by 
	a.province_name
	
union all

-- 全国合计	
select
	'全国' as province_name,
	--非投标 新履约客户数
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_customer_cnt,
	--非投标 新履约金额
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null,a.sales_value,null)) as new_sales_value
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		--and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--and province_name !='BBC'
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
			and first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		) c on c.customer_no=a.customer_no
;


-- 大区			
select
	a.region_name,
	--非投标 新履约客户数
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_customer_cnt,
	--非投标 新履约金额
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null,a.sales_value,null)) as new_sales_value
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,region_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		--and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--and province_name !='BBC'
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
			and first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		) c on c.customer_no=a.customer_no
group by 
	a.region_name
;

--================================================================================================================================================================================
-- 开始日期
set current_start_day = '20210101';

-- 结束日期
set current_end_day = '20210331';

-- 省区			
select
	a.province_name,
	--非投标 新履约客户数
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_customer_cnt,
	--非投标 新履约金额
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null,a.sales_value,null)) as new_sales_value
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		--and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--and province_name !='BBC'
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
			and first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		) c on c.customer_no=a.customer_no
group by 
	a.province_name
	
union all

-- 全国合计	
select
	'全国' as province_name,
	--非投标 新履约客户数
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_customer_cnt,
	--非投标 新履约金额
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null,a.sales_value,null)) as new_sales_value
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		--and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--and province_name !='BBC'
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
			and first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		) c on c.customer_no=a.customer_no
;


-- 大区			
select
	a.region_name,
	--非投标 新履约客户数
	count(distinct if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.customer_no,null)) as new_ftb_customer_cnt,
	count(distinct if(c.customer_no is not null and b.business_type_name='投标',a.customer_no,null)) as new_tb_customer_cnt,
	count(distinct if(c.customer_no is not null,a.customer_no,null)) as new_customer_cnt,
	--非投标 新履约金额
	sum(if(c.customer_no is not null and (b.business_type_name !='投标' or b.business_type_name is null),a.sales_value,null)) as new_ftb_sales_value,
	sum(if(c.customer_no is not null and b.business_type_name='投标',a.sales_value,null)) as new_tb_sales_value,
	sum(if(c.customer_no is not null,a.sales_value,null)) as new_sales_value
from
	(
	select 
		id,sdt,province_name,city_group_name,goods_code,customer_no,business_type_name,region_name,
		sales_value/10000 as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9')
		--and business_type_code in ('1','2','4','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--and province_name !='BBC'
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
			and first_order_date between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		) c on c.customer_no=a.customer_no
group by 
	a.region_name
;