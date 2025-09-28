-- ================================================================================================================
-- 销售员新签及断约

set current_day ='20210731';

select
	a.prov_name, -- 省份
	a.city_name, -- 城市
	'' as sales_manger, -- 销售经理
	b.name, -- 销售主管
	a.name, -- 销售员
	a.user_number, --工号
	c.customer_cnt, -- 合作客户数
	g.visit_cnt, -- 拜访次数-合作客户
	g.visit_cnt2, -- 拜访次数-非合作客户
	h.have_service, -- 是否有服务管家
	d.sales_three_months_customer_cnt, -- 在履约客户数 近三个月有交易
	d.sales_value, -- 本月销售额	
	d.profit, -- 本月毛利额
	d.front_profit, -- 本月前端毛利额
	f.break_this_month, -- 本月断约客户数	
	c.customer_cnt_2, -- 本月新签客户数	
	d.sign_sale_this_months, -- 本月新签并履约客户
	e.overdue_amount -- 逾期金额
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
		( -- 客户数量、近半年新签客户数
		select
			sales_id,
			count(distinct case when is_cooperative_customer='1' then customer_no else null end) as customer_cnt, -- 合作客户数量
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')='202107' then customer_no else null end) as customer_cnt_2 -- 7月新签客户数
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		group by 
			sales_id
		) c on c.sales_id=a.id
	left join
		( -- 本月销售额、定价毛利额、定价毛利率、近半年新签并履约客户、近半年新签销售金额、近三月新签并履约客户、近三月新签销售金额、本月新签并履约客户
		select 
			sales_id,
			sum(case when sdt between '20210701' and ${hiveconf:current_day} then sales_value else 0 end) as sales_value, -- 本月销售额
			sum(case when sdt between '20210701' and ${hiveconf:current_day} then profit else 0 end) as profit, -- 本月定价毛利额
			sum(case when sdt between '20210701' and ${hiveconf:current_day} then front_profit else 0 end) as front_profit, -- 本月前端毛利
			--count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')>='202103' then customer_no else null end) as sign_sale_six_months, -- 近半年新签并履约客户
			--sum(case when regexp_replace(substr(sign_time,1,7),'-','')>='202103' then sales_value else 0 end) as sign_sales_value_six_months, -- 近半年新签销售金额
			--count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')>='202105' then customer_no else null end) as sign_sale_three_months, -- 近三月新签并履约客户
			--sum(case when regexp_replace(substr(sign_time,1,7),'-','')>='202105' then sales_value else 0 end) as sign_sales_value_three_months, -- 近三月新签销售金额		
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')='202107' then customer_no else null end) as sign_sale_this_months, -- 本月新签并履约客户
			count(distinct case when sdt between '20210501' and ${hiveconf:current_day} then customer_no else null end) as sales_three_months_customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		group by 
			sales_id
		) d on d.sales_id=a.id
	left join
		(
		select --应收逾期
			sales_id,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_day}
		group by 
			sales_id
		) e on e.sales_id=a.id
	left join
		(
		select
			sales_id,
			--count(distinct case when after_month between '202103' and '202107' then customer_no else null end) as break_six_months,
			--count(distinct case when after_month between '202105' and '202107' then customer_no else null end) as break_three_months,
			count(distinct case when after_month between '202107' and '202107' then customer_no else null end) as break_this_month
		from
			(
			select 
				sales_id,customer_no,
				substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as after_month
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20190101' and ${hiveconf:current_day}
				and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				and sales_type !='fanli'
			group by 
				sales_id,customer_no
			) tmp1
		group by 
			sales_id
		) f on f.sales_id=a.id
	left join
		(
		select
			t1.visit_person_id,
			--count(t1.id) as visit_cnt,
			count(case when t2.is_cooperative_customer='1' then t1.id else null end) as visit_cnt, -- 拜访次数 不按客户去重
			count(case when t2.is_cooperative_customer !='1' or t2.is_cooperative_customer is null then t1.id else null end) as visit_cnt2
		from
			(
			select
				id,visit_person_id,customer_id,visit_time
			from
				csx_ods.source_crm_w_a_customer_visit_record
			where
				sdt=${hiveconf:current_day}
				and regexp_replace(substr(visit_time,1,7),'-','') between '202107' and '202107'
			) as t1
			left join
				(
				select
					id,is_cooperative_customer
				from
					csx_dw.dws_crm_w_a_customer
				where
					sdt=${hiveconf:current_day}
				) as t2 on t2.id=t1.customer_id
		group by 
			t1.visit_person_id
		) g on g.visit_person_id=a.id
	left join
		(
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20210731'
		group by 
			work_no
		) h on h.work_no=a.user_number
;



-- ================================================================================================================
-- Q3截止目前断约情况

set current_day ='20210808';

insert overwrite directory '/tmp/zhangyanpeng/20210809_linshi_2' row format delimited fields terminated by '\t' 

select
	b.sales_province_name,
	b.sales_city_name,
	a.customer_no,
	b.customer_name,
	b.attribute_desc,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_name,
	'' as manager_name,
	b.sign_date,
	c.diff_month,
	b.estimate_contract_amount,
	c.normal_total_value,
	c.normal_first_order_date,
	c.normal_last_order_date
from
	(
	select
		customer_no,break_date
	from
		(
		select 
			customer_no,
			regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-','') as break_date
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
		group by 
			customer_no
		) tmp1
	where
		break_date between '20210701' and ${hiveconf:current_day}
	) a 
	left join
		( 
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_province_name,sales_city_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		) b on b.customer_no=a.customer_no
	left join
		(
		select
			customer_no,customer_name,normal_first_order_date,normal_last_order_date,normal_total_value,
			months_between(from_unixtime(unix_timestamp(normal_last_order_date,'yyyyMMdd')),from_unixtime(unix_timestamp(normal_first_order_date,'yyyyMMdd'))) as diff_month
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
		) c on c.customer_no=a.customer_no
;


-- ================================================================================================================
-- 全国签约金额TOP20履约情况

set current_day ='20210808';

insert overwrite directory '/tmp/zhangyanpeng/20210809_linshi_3' row format delimited fields terminated by '\t' 

select
	a.sales_province_name,
	a.sales_city_name,
	a.customer_no,
	a.customer_name,
	a.attribute_desc,
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	a.work_no,
	a.sales_name,
	a.first_supervisor_name,
	'' as manager_name,
	a.sign_date,
	c.diff_month,
	a.estimate_contract_amount,
	c.normal_total_value,
	c.normal_first_order_date,
	c.normal_last_order_date
from
	(
	select
		customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
		sales_province_name,sales_city_name,sign_date,estimate_contract_amount,rn
	from
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_province_name,sales_city_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount,
			row_number()over(order by cast(estimate_contract_amount as bigint) desc) as rn
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and sales_province_name not like '%平台%'
			and regexp_replace(substr(sign_time,1,10),'-','')>='20210101'
		) t1
	where
		rn<=100
	) a 
	left join
		(
		select
			customer_no,customer_name,normal_first_order_date,normal_last_order_date,normal_total_value,
			months_between(from_unixtime(unix_timestamp(normal_last_order_date,'yyyyMMdd')),from_unixtime(unix_timestamp(normal_first_order_date,'yyyyMMdd'))) as diff_month
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
		) c on c.customer_no=a.customer_no
;


-- ================================================================================================================
-- Q3截止目前断约情况

set current_day ='20210808';

insert overwrite directory '/tmp/zhangyanpeng/20210810_linshi_1' row format delimited fields terminated by '\t' 

select
	b.sales_province_name,
	b.sales_city_name,
	a.customer_no,
	b.customer_name,
	b.attribute_desc,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_name,
	'' as manager_name,
	b.sign_date,
	c.diff_month,
	b.estimate_contract_amount,
	c.normal_total_value,
	c.normal_first_order_date,
	c.normal_last_order_date
from
	(
	select
		customer_no,break_date
	from
		(
		select 
			customer_no,
			regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-','') as break_date
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and business_type_code in ('1','4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
		group by 
			customer_no
		) tmp1
	where
		break_date between '20210701' and ${hiveconf:current_day}
	) a 
	left join
		( 
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_province_name,sales_city_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		) b on b.customer_no=a.customer_no
	--left join
	--	(
	--	select
	--		customer_no,customer_name,normal_first_order_date,normal_last_order_date,normal_total_value,
	--		months_between(from_unixtime(unix_timestamp(normal_last_order_date,'yyyyMMdd')),from_unixtime(unix_timestamp(normal_first_order_date,'yyyyMMdd'))) as diff_month
	--	from 
	--		csx_dw.dws_crm_w_a_customer_active
	--	where 
	--		sdt = 'current' 
	--	) c on c.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,
			min(sdt) as normal_first_order_date,
			max(sdt) as normal_last_order_date,
			sum(sales_value) as normal_total_value,
			months_between(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'))) as diff_month
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and business_type_code in ('1','4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
;

			
		
