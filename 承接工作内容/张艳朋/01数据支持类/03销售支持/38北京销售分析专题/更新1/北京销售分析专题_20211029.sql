--==============================================================================================================================================================================
-- 业务类型销售额

select
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate
from
	(
	select 
		customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20210930'
		and channel_code in('1','7','9')
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on a.customer_no=b.customer_no
group by
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth		
;	

--==============================================================================================================================================================================

-- 新签客户

select
	b.sales_region_name,
	b.province_name,
	a.smonth,
	count(a.customer_no) as customer_cnt,
	count(case when b.attribute_desc like '%日配%' then a.customer_no else null end) as normal_customer_cnt
from
	(
	select
		customer_no,regexp_replace(substr(first_sign_time,1,7),'-','') as smonth,estimate_contract_amount
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='current'
		and regexp_replace(substr(first_sign_time,1,7),'-','') between '202101' and '202109'
	) as a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'
		)b on a.customer_no=b.customer_no	
group by 
	b.sales_region_name,
	b.province_name,
	a.smonth
;
--==============================================================================================================================================================================

-- 新履约客户及履约金额

select
	b.sales_region_name,
	b.province_name,
	a.smonth,
	count(a.customer_no) as customer_cnt,
	sum(c.sales_value) as sales_value
from
	(
	select
		sales_id,substr(first_order_date,1,6) as smonth,customer_no
	from
		csx_dw.dws_crm_w_a_customer_active
	where 
		sdt = 'current' 
		and substr(first_order_date,1,6) between '202101' and '202109'
	) as a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		) b on a.customer_no=b.customer_no
	left join
		(
		select 
			customer_no,substr(sdt,1,6) as smonth,sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<='20210930'
			and channel_code in('1','7','9')
			--and business_type_code ='1'
		group by 
			customer_no,substr(sdt,1,6)
		) c on c.customer_no=a.customer_no and c.smonth=a.smonth
group by 
	b.sales_region_name,
	b.province_name,
	a.smonth
;	

--==============================================================================================================================================================================

-- 断约客户数

select
	b.sales_region_name,
	b.province_name,
	a.after_month,	
	count(distinct a.customer_no) as customer_cnt
from
	(
	select 
		customer_no,
		substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as after_month
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20190101' and '20210930'
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and sales_type !='fanli'
	group by 
		customer_no
	) as a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		) b on a.customer_no=b.customer_no	
where
	a.after_month between '202101' and '202109'
group by
	b.sales_region_name,
	b.province_name,
	a.after_month	
;	

-- ================================================================================================================
-- 商机数量 不含100%

select 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6) as smonth,
	count(distinct t1.business_number) as business_cnt,
	sum(t1.estimate_contract_amount) as estimate_contract_amount
from
	(
	select
		sdt,id,status,business_stage,work_no,sales_name,business_number,estimate_contract_amount
	from
		csx_dw.dws_crm_w_a_business_customer 
	where 
		(sdt='20210731'
		and status='1'
		and business_stage !=5)
		or
		(sdt='20210831'
		and status='1'
		and business_stage !=5)
		or
		(sdt='20210930'
		and status='1'
		and business_stage !=5)	
	) t1 
	join
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		)t2 on t2.customer_id=t1.id		
group by 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6)
;

-- ================================================================================================================
-- 商机数量 不含10%和100%

select 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6) as smonth,
	count(distinct t1.business_number) as business_cnt,
	sum(t1.estimate_contract_amount) as estimate_contract_amount
from
	(
	select
		sdt,id,status,business_stage,work_no,sales_name,business_number,estimate_contract_amount
	from
		csx_dw.dws_crm_w_a_business_customer 
	where 
		(sdt='20210731'
		and status='1'
		and business_stage !=5
		and business_stage !=1)
		or
		(sdt='20210831'
		and status='1'
		and business_stage !=5
		and business_stage !=1)
		or
		(sdt='20210930'
		and status='1'
		and business_stage !=5
		and business_stage !=1)	
	) t1 
	join
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		)t2 on t2.customer_id=t1.id		
group by 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6)
;

-- ================================================================================================================
-- 商机

select
	t2.sales_region_name,t2.province_name,t1.business_stage_type,
	count(distinct t1.business_number) as business_number_cnt,
	sum(t1.estimate_contract_amount) as estimate_contract_amount
from
	(
	select 
		id,business_number,estimate_contract_amount,attribute_desc,business_stage,
		case when business_stage=1 then '10%阶段'
			when business_stage=2 then '25%阶段'
			when business_stage=3 then '50%阶段'
			when business_stage=4 then '75%阶段'
			when business_stage=5 then '100%阶段'
			else '其他'
		end as business_stage_type
	from 
		csx_dw.dws_crm_w_a_business_customer
	where 
		sdt = '20211025'
	) t1 
	join
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		)t2 on t2.customer_id=t1.id	
group by 
	t2.sales_region_name,t2.province_name,t1.business_stage_type
;

--==============================================================================================================================================================================

-- 业务类型销售额

select
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) as customer_cnt
from
	(
	select 
		customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20210930'
		and channel_code in('1','7','9')
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on a.customer_no=b.customer_no
group by
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth		
;	


--==============================================================================================================================================================================

-- 9月履约的日配客户、了解日配客户是否开发福利业务

--insert overwrite directory '/tmp/zhangyanpeng/tc_fuwuguanjia' row format delimited fields terminated by '\t'

select
	b.province_name,
	a.customer_no,
	b.customer_name,
	c.months_cnt,
	d.sdt_cnt,
	d.sales_value,
	e.sdt_cnt,
	e.sales_value
from
	( -- 9月履约日配业务的客户
	select 
		customer_no
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210901' and sdt<='20210930'
		and channel_code in('1','7','9')
		and business_type_code ='1'
	group by 
		customer_no
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			-- and province_name not like '%平台%'
			and province_name in ('北京市','福建省','四川省','重庆市')
		)b on a.customer_no=b.customer_no
	left join
		( --履约日配业务月数
		select 
			customer_no,count(distinct substr(sdt,1,6)) as months_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code ='1'
		group by 
			customer_no
		)c on c.customer_no=a.customer_no
	left join
		( --Q3日配配送频次 销售额
		select 
			customer_no,
			count(distinct sdt) as sdt_cnt,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210701' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code ='1'
		group by 
			customer_no
		)d on d.customer_no=a.customer_no	
	left join
		( --Q3福利配送频次 销售额
		select 
			customer_no,
			count(distinct sdt) as sdt_cnt,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210701' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code ='2'
		group by 
			customer_no
		)e on e.customer_no=a.customer_no			
;

--==============================================================================================================================================================================

-- 投标客户
	
select
	province_name,
	substr(sdt,1,6) as smonth,
	case when business_type_name='' then '非投标' else business_type_name end as business_type_name,
	count(distinct customer_no) as customer_cnt
from
	csx_dw.dws_crm_w_a_customer
where 
	(sdt = '20210131'
	or sdt = '20210228'
	or sdt = '20210331'
	or sdt = '20210430'
	or sdt = '20210531'
	or sdt = '20210630'
	or sdt = '20210731'
	or sdt = '20210831'
	or sdt = '20210930')
	--and province_name='北京市'
	and province_name in ('北京市','福建省','四川省','重庆市')
group by 
	province_name,
	substr(sdt,1,6),
	business_type_name
;

--==============================================================================================================================================================================

-- 投标客户销售金额

select
	b.province_name,
	a.smonth,
	case when b.business_type_name='' then '非投标' else b.business_type_name end as business_type_name,
	sum(a.sales_value) sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) as customer_cnt
from
	(
	select 
		customer_no,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20210930'
		and channel_code in('1','7','9')
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount,business_type_name
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			-- and province_name='北京市'
			and province_name in ('北京市','福建省','四川省','重庆市')
		)b on a.customer_no=b.customer_no
group by
	b.province_name,
	a.smonth,
	b.business_type_name
;

--==============================================================================================================================================================================

-- 新签投标客户数，签约金额

select
	province_name,
	regexp_replace(substr(sign_time,1,7),'-','') as smonth,
	case when business_type_name='' then '非投标' else business_type_name end as business_type_name,
	count(distinct customer_no) as customer_cnt,
	sum(cast(estimate_contract_amount as int)) as estimate_contract_amount
from
	csx_dw.dws_crm_w_a_customer
where 
	sdt = 'current'
	-- and province_name='北京市'
	and province_name in ('北京市','福建省','四川省','重庆市')
	and regexp_replace(substr(sign_time,1,7),'-','') between '202101' and '202109'
group by 	
	province_name,regexp_replace(substr(sign_time,1,7),'-',''),business_type_name	
;

--==============================================================================================================================================================================

-- 业务类型销售额

select
	b.province_name,
	b.first_category_name,
	b.second_category_name,
	a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
	count(distinct a.customer_no) as customer_cnt
from
	(
	select 
		customer_no,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20210930'
		and channel_code in('1','7','9')
		--and business_type_code !='4'
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			-- and province_name='北京市'
			and province_name in ('北京市','福建省','四川省','重庆市')
		)b on a.customer_no=b.customer_no
group by
	b.province_name,
	b.first_category_name,
	b.second_category_name,
	a.smonth	
;	

--==============================================================================================================================================================================

-- 分行业新签客户数，签约金额

select
	province_name,regexp_replace(substr(sign_time,1,7),'-','') as smonth,first_category_name,second_category_name,
	count(distinct customer_no) as customer_cnt,
	sum(cast(estimate_contract_amount as int)) as estimate_contract_amount
from
	csx_dw.dws_crm_w_a_customer
where 
	sdt = 'current'
	-- and province_name='北京市'
	and province_name in ('北京市','福建省','四川省','重庆市')
	and regexp_replace(substr(sign_time,1,7),'-','') between '202101' and '202109'
group by 
	province_name,regexp_replace(substr(sign_time,1,7),'-',''),first_category_name,second_category_name	
;

--==============================================================================================================================================================================

-- 9月仍履约客户的月均销售额（可以取1-9月月均销售额）的客户数量区间分布

select
	b.province_name,
	b.first_category_name,
	b.second_category_name,
	c.avg_sales_value_type,
	count(distinct a.customer_no) as customer_cnt,
	sum(avg_sales_value) as avg_sales_value,
	sum(avg_profit) as avg_profit
from
	( -- 9月履约日配业务的客户
	select 
		customer_no
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210901' and sdt<='20210930'
		and channel_code in('1','7','9')
		and business_type_code ='1'
	group by 
		customer_no
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			-- and province_name='北京市'
			and province_name in ('北京市','福建省','四川省','重庆市')
		)b on a.customer_no=b.customer_no
	left join
		( --履约日配业务月数
		select
			customer_no,
			case when avg_sales_value<=10000 then '（0,1]'
				when avg_sales_value<=50000 then '（1,5]'
				when avg_sales_value<=100000 then '（5,10]'
				when avg_sales_value<=200000 then '（10,20]'
				when avg_sales_value<=500000 then '（20,50]'
				when avg_sales_value<=1000000 then '（50,100]'
				when avg_sales_value>1000000 then '100以上'
				else '其他'
			end as avg_sales_value_type,
			avg_sales_value,
			avg_profit
		from
			(
			select 
				customer_no,
				count(distinct substr(sdt,1,6)) as months_cnt,
				sum(sales_value) as sales_value,
				sum(sales_value)/count(distinct substr(sdt,1,6)) as avg_sales_value,
				sum(profit) as profit,
				sum(profit)/count(distinct substr(sdt,1,6)) as avg_profit
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20210101' and sdt<='20210930'
				and channel_code in('1','7','9')
				and business_type_code ='1'
			group by 
				customer_no
			) t1
		)c on c.customer_no=a.customer_no
group by 
	b.province_name,
	b.first_category_name,
	b.second_category_name,
	c.avg_sales_value_type
;


--==============================================================================================================================================================================

-- 1-9月履约客户

select
	b.province_name,
	a.customer_no,
	b.customer_name,
	b.sign_date,
	d.a,
	c.first_order_date,
	c.last_order_date,
	'' as aa,
	e.months_cnt
from
	( -- 1-9月履约客户
	select 
		customer_no
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20210930'
		and channel_code in('1','7','9')
		and business_type_code !='4'
	group by 
		customer_no
	) a 
	join 
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name='北京市'
		)b on a.customer_no=b.customer_no
	left join	
		(
		select
			customer_no,customer_name,first_order_date,last_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
		)c on c.customer_no=a.customer_no	
	left join
		(
		select
			id,a,rn
		from
			(
			select 
				id,concat(get_json_object(contract_cycle, '$.input'),get_json_object(contract_cycle, '$.radioName')) AS a,
				row_number() over(partition by id order by update_time desc) as rn
			from 
				csx_dw.dws_crm_w_a_business_customer
			where 
				sdt = 'current' 
				and status=1
				and contract_cycle <> ''
			) t1
		where
			rn=1
		)d on d.id=b.customer_id
	left join
		(
		select 
			customer_no,
			count(distinct substr(sdt,1,6)) as months_cnt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' and sdt<='20210930'
			and channel_code in('1','7','9')
		group by 
			customer_no
		)e on e.customer_no=a.customer_no	
;
	
--==============================================================================================================================================================================

-- 人效

	select 
		work_no,sales_name,
		sum(sales_value)/count(distinct substr(sdt,1,6)) as avg_sales_value,
		sum(profit)/count(distinct substr(sdt,1,6)) as avg_profit,
		sum(front_profit)/count(distinct substr(sdt,1,6)) as avg_front_profit,
		count(distinct customer_no) as customer_cnt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210701' and sdt<='20210930'
		and channel_code in('1','7','9')
		and province_name='北京市'
	group by 
		work_no,sales_name
;
--==============================================================================================================================================================================

-- 新签客户数

select
	work_no,sales_name,regexp_replace(substr(sign_time,1,7),'-','') as smonth,
	count(distinct customer_no) as customer_cnt
from
	csx_dw.dws_crm_w_a_customer
where 
	sdt = 'current'
	and province_name='北京市'
	and regexp_replace(substr(sign_time,1,7),'-','') between '202101' and '202109'
group by 
	work_no,sales_name,regexp_replace(substr(sign_time,1,7),'-','')
;
--==============================================================================================================================================================================
-- 人效综合

select
	a.province_name,
	f.sales_supervisor_name,
	a.sales_name,
	a.work_no,
	g.have_service,
	a.avg_sales_value,
	a.customer_cnt,
	b.customer_cnt,
	b.customer_cnt2,
	c.customer_cnt,
	c.customer_cnt2,
	d.business_number_cnt,
	d.estimate_contract_amount,
	e.visit_cnt,
	e.visit_cnt2,
	e.visit_cnt3,
	e.visit_cnt4
from	
	-- 销售额
	( 
	select 
		work_no,sales_name,province_name,
		sum(sales_value)/count(distinct substr(sdt,1,6)) as avg_sales_value,
		count(distinct case when sdt between '20210901' and '20210930' then customer_no else null end) as customer_cnt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210701' and sdt<='20210930'
		and channel_code in('1','7','9')
		and province_name='北京市'
	group by 
		work_no,sales_name,province_name
	) a
	-- 新签客户
	left join
		( 
		select
			work_no,
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','') ='202109' then customer_no else null end) as customer_cnt,
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then customer_no else null end) as customer_cnt2
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name='北京市'
		group by 
			work_no
		) b on b.work_no=a.work_no
	-- 新履约客户
	left join
		( 
		select
			t2.work_no,
			count(distinct case when t1.first_order_date between '20210901' and '20210930' then t1.customer_no else null end) as customer_cnt,
			count(distinct case when t1.first_order_date between '20210701' and '20210930' then t1.customer_no else null end) as customer_cnt2
		from 
			(
			select
				sales_id,first_order_date,customer_no
			from
				csx_dw.dws_crm_w_a_customer_active
			where 
				sdt = 'current' 
				and first_order_date between '20210701' and '20210930'
			) t1
			join
				(
				select
					customer_no,customer_name,work_no,sales_name,sales_region_name,sales_province_name,city_group_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					sdt='current'
					and province_name='北京市'
				)t2 on t2.customer_no=t1.customer_no
		group by
			t2.work_no
		) c on c.work_no=a.work_no
	-- 商机数量及金额
	left join
		( 
		select
			t2.work_no,
			count(distinct t1.business_number) as business_number_cnt,
			sum(t1.estimate_contract_amount) as estimate_contract_amount
		from
			(
			select 
				id,business_number,estimate_contract_amount,business_stage
			from 
				csx_dw.dws_crm_w_a_business_customer
			where 
				sdt = '20211102'
				and status=1
				and business_stage !=5 -- 不含100%
			) t1 
			join
				(
				select
					customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
					sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
				from
					csx_dw.dws_crm_w_a_customer
				where 
					sdt = 'current'
					and province_name='北京市'
				)t2 on t2.customer_id=t1.id	
		group by 
			t2.work_no
		) d on d.work_no=a.work_no
	-- 拜访
	left join
		( 
		select
			t1.visit_work_no,
			count(t1.id) as visit_cnt,
			count(case when t2.is_cooperative_customer =1 then t1.id else null end) as visit_cnt2,
			count(case when t2.is_cooperative_customer =0 then t1.id else null end) as visit_cnt3,
			count(case when t1.effective_flag =0 then t1.id else null end) as visit_cnt4
		from
			(
			select
				id,visit_person_id,visit_work_no,customer_id,visit_time,customer_no,sdt,
				case when length(crm_contact_person) < 2 or length(crm_contact_phone) < 8 or cast(substr(visit_time,12,2) as int) >= 21 then 0 else 1 end as effective_flag
			from
				csx_dw.dws_crm_r_d_customer_visit
			where
				sdt>='20210901' and sdt<='20210930'
			) as t1
			join
				(
				select
					customer_id,customer_no,customer_name,work_no,sales_name,first_category_name,second_category_name,third_category_name,
					sales_province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,
					case when customer_no='' then 0 else 1 end as is_cooperative_customer
				from
					csx_dw.dws_crm_w_a_customer_union
				where 
					sdt = 'current'
					and sales_province_name='北京市'
				) as t2 on t2.customer_id=t1.customer_id
		group by 
			t1.visit_work_no
		) e on e.visit_work_no=a.work_no
	-- 主管信息
	left join
		( 
		select
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		from
			(
			select
				id as sales_id,
				name as sales_name,
				user_number as work_no,
				user_position as position,
				-- 主管
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_supervisor_id,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_supervisor_name,
				first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_supervisor_work_no,
				-- 销售经理
				first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_manager_id,
				first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_manager_name,
				first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_manager_work_no,
				row_number() over(partition by id order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt='20211102'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		) f on f.work_no = a.work_no	
	-- 是否有服务管家	
	left join
		( 
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211102'
		group by 
			work_no
		) g on g.work_no=a.work_no		
;

-- ================================================================================================================
-- 商机-转化率

select
	t2.sales_region_name,t2.province_name,t1.business_stage_type,
	count(distinct t1.business_number) as business_number_cnt,
	sum(t1.estimate_contract_amount) as estimate_contract_amount
from
	(
	select 
		id,business_number,estimate_contract_amount,attribute_desc,business_stage,
		case when business_stage=1 then '10%阶段'
			when business_stage=2 then '25%阶段'
			when business_stage=3 then '50%阶段'
			when business_stage=4 then '75%阶段'
			when business_stage=5 then '100%阶段'
			else '其他'
		end as business_stage_type
	from 
		csx_dw.dws_crm_w_a_business_customer
	where 
		sdt = '20211102'
		and status=1
	) t1 
	join
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_category_name,second_category_name,third_category_name,
			sales_province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,
			case when customer_no='' then 0 else 1 end as is_cooperative_customer
		from
			csx_dw.dws_crm_w_a_customer_union
		where 
			sdt = 'current'
			-- and sales_province_name='北京市'
			and sales_province_name in ('北京市')
		) as t2 on t2.customer_id=t1.customer_id
group by 
	t2.sales_region_name,t2.province_name,t1.business_stage_type
;

--==============================================================================================================================================================================
-- 业务类型销售额

select
	b.province_name,
	a.business_type_name,
	sum(a.sales_value) sales_value
	-- sum(a.profit) as profit,
	-- sum(a.profit)/abs(sum(a.sales_value)) as profit_rate
from
	(
	select 
		customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210701' and sdt<='20210930'
		and channel_code in('1','7','9')
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name in ('北京市')
		)b on a.customer_no=b.customer_no
group by
	b.province_name,
	a.business_type_name		
;	

--==============================================================================================================================================================================
-- 行业销售额

select
	b.province_name,
	b.first_category_name,
	b.second_category_name,
	sum(a.sales_value) sales_value,
	count(distinct a.customer_no) as customer_cnt
from
	(
	select 
		customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210701' and sdt<='20210930'
		and channel_code in('1','7','9')
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name in ('北京市')
		)b on a.customer_no=b.customer_no
group by
	b.province_name,
	b.first_category_name,
	b.second_category_name		
;	

--==============================================================================================================================================================================
-- 行业商机

	select 
		sales_province_name,first_category_name,second_category_name,
		count(distinct business_number) as business_cnt,
		sum(cast(estimate_contract_amount as int)) as estimate_contract_amount
	from 
		csx_dw.dws_crm_w_a_business_customer
	where 
		sdt='20211102'
		and status=1
		and sales_province_name='北京市'
	group by
		sales_province_name,first_category_name,second_category_name	
;
--==============================================================================================================================================================================
-- 各个阶段商机

	select 
		province_name,attribute_name,
		-- 10%阶段
		count(case when business_stage=1 then biz_id else null end) as cnt_1,
		sum(case when business_stage=1 then estimate_contract_amount else null end) as amount_1,
		count(case when business_stage>=1 then biz_id else null end)/count(*) as rate_1,
		-- 25%阶段
		count(case when business_stage=2 then biz_id else null end) as cnt_2,
		sum(case when business_stage=2 then estimate_contract_amount else null end) as amount_2,
		count(case when business_stage>=2 then biz_id else null end)/count(*) as rate_2,
		-- 50%阶段
		count(case when business_stage=3 then biz_id else null end) as cnt_3,
		sum(case when business_stage=3 then estimate_contract_amount else null end) as amount_3,
		count(case when business_stage>=3 then biz_id else null end)/count(*) as rate_3,
		-- 75%阶段
		count(case when business_stage=4 then biz_id else null end) as cnt_4,
		sum(case when business_stage=4 then estimate_contract_amount else null end) as amount_4,
		count(case when business_stage>=4 then biz_id else null end)/count(*) as rate_4,
		-- 100%阶段
		count(case when business_stage=5 then biz_id else null end) as cnt_5,
		sum(case when business_stage=5 then estimate_contract_amount else null end) as amount_5,
		count(case when business_stage>=5 then biz_id else null end)/count(*) as rate_5
	from 
		csx_dw.ads_crm_r_m_business_customer
	where 
		month='202111'
		and status=1
		and province_name='北京市'
	group by
		province_name,attribute_name
;

--==============================================================================================================================================================================
-- 预计签约金额

	select 
		province_name,attribute_name,regexp_replace(substr(if(business_stage=5,sign_time,expect_sign_time),1,7),'-','') as smonth,
		sum(cast(estimate_contract_amount as int)) as estimate_contract_amount
	from 
		csx_dw.ads_crm_r_m_business_customer
	where 
		month='202111'
		and status=1
		and province_name='北京市'
	group by
		province_name,attribute_name,regexp_replace(substr(if(business_stage=5,sign_time,expect_sign_time),1,7),'-','')
;


--==============================================================================================================================================================================

-- 查数

select
	b.province_name,b.customer_no,b.customer_name,b.work_no,b.sales_name,b.first_supervisor_work_no,b.first_supervisor_name,b.first_category_name,b.second_category_name,
	b.third_category_name,b.attribute_desc,sign_date,estimate_contract_amount,business_type_name
from
	(
	select 
		customer_no
		--substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20210930'
		and channel_code in('1','7','9')
	group by 
		customer_no
	) a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount,business_type_name
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			-- and province_name='北京市'
			and province_name in ('北京市')
		)b on a.customer_no=b.customer_no
;