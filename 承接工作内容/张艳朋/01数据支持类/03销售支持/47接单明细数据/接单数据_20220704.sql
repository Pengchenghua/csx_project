insert overwrite directory '/tmp/zhangyanpeng/20220704_01' row format delimited fields terminated by '\t'

select
	b.province_code,
	b.province_name,
	b.city_group_code,
	b.city_group_name,
	a.customer_no,
	b.customer_name,
	a.child_customer_no,
	a.child_customer_name,
	a.order_no,
	a.recep_order_time,
	a.recep_user_number,
	a.recep_order_by,
	a.num,
	a.order_kind_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.order_kind
from	
	(
	select
		customer_no,customer_name,child_customer_no,child_customer_name,order_no,recep_order_time,recep_user_number,recep_order_by,
		case when order_kind='NORMAL' then '日配单'
			when order_kind='WELFARE' then '福利单'
			when order_kind='BIGAMOUNT_TRADE' then '大宗贸易'
			when order_kind='INNER' then '内购单'
			else '其他'
		end as order_kind_name,
		order_kind,
		count(goods_code) as num
	from
		csx_dw.dws_csms_r_d_yszx_order_m_new
	where
		(sdt='19990101' or sdt>='20220526')
		and to_date(recep_order_time) between '2022-05-26' and '2022-06-25'
	group by 
		customer_no,customer_name,child_customer_no,child_customer_name,order_no,recep_order_time,recep_user_number,recep_order_by,
		case when order_kind='NORMAL' then '日配单'
			when order_kind='WELFARE' then '福利单'
			when order_kind='BIGAMOUNT_TRADE' then '大宗贸易'
			when order_kind='INNER' then '内购单'
			else '其他'
		end,
		order_kind		
	) a 
	left join
		(
		select 
			customer_no,customer_name,attribute,attribute_desc,first_category_name,second_category_name,third_category_name,
			province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where
			sdt='20220703'
		) b on b.customer_no=a.customer_no	
