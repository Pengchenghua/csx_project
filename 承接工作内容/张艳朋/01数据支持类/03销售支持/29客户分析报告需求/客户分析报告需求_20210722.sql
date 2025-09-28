-- ================================================================================================================
-- 客户分析报告需求

-- insert overwrite directory '/tmp/zhangyanpeng/20210531_linshi_1' row format delimited fields terminated by '\t' 

select
	a.prov_name,
	a.city_name,
	b.name,
	a.name,
	c.customer_cnt,
	c.customer_cnt2,
	d.sign_4,
	d.sign_5,
	d.sign_6,
	d.avg_sign_5_7,
	d.avg_sign_sales_5_7,
	d.n_sign_month
from	
	(
	select 
		id,leader_id,user_number,name,user_position,channel,user_source_busi,prov_name,city_name,del_flag,status
	from 
		csx_dw.dws_basic_w_a_user
	where 
		sdt ='current'
		and user_position ='SALES'
		and status='0' -- 0:启用  1：禁用
		and del_flag='0' -- 删除标记0正常-1删除
		and prov_name not like '%平台%'
	) a
	left join
		(
		select 
			id,user_number,name,user_position
		from 
			csx_dw.dws_basic_w_a_user
		where 
			sdt = 'current'
		) b on b.id = a.leader_id
	left join
		(
		select 
			t1.sales_id,count(distinct t1.customer_no) as customer_cnt,count(distinct t2.customer_no) as customer_cnt2
		from
			(
			select
				sales_id,customer_no
			from
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
				and is_cooperative_customer='1' -- 是否合作客户(0.否，1.是)
			) as t1
			left join
				(
				select
					customer_no,first_order_date
				from
					csx_dw.dws_crm_w_a_customer_active
				where
					sdt='current'
					and first_order_date is not null
				) as t2 on t2.customer_no=t1.customer_no
		group by 
			t1.sales_id
		) c on c.sales_id=a.id
	left join 
		(
		select 
			t1.sales_id,
			count(distinct case when sign_month = '202104' then t1.customer_no else null end) as sign_4,
			count(distinct case when sign_month = '202105' then t1.customer_no else null end) as sign_5,
			count(distinct case when sign_month = '202106' then t1.customer_no else null end) as sign_6,
			count(distinct case when sign_month between '202105' and '202107' then t1.customer_no else null end)/3 as avg_sign_5_7,
			count(distinct case when sign_month between '202105' and '202107' and t2.customer_no is not null then t1.customer_no else null end)/3 as avg_sign_sales_5_7,
			months_between('2021-07-21',max(sign_date)) as n_sign_month
		from 
			(
			select
				sales_id,customer_no,regexp_replace(substr(sign_time,1,7),'-','') as sign_month,substr(sign_time,1,10) as sign_date
			from
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
			) as t1
			left join
				(
				select
					customer_no,first_order_date
				from
					csx_dw.dws_crm_w_a_customer_active
				where
					sdt='current'
					and first_order_date is not null
				) as t2 on t2.customer_no=t1.customer_no
		group by 
			t1.sales_id
		) d on d.sales_id = a.id
