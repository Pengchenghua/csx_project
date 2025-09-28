-- ================================================================================================================
-- 销售员新签及断约

set current_day ='20210930';

insert overwrite directory '/tmp/zhangyanpeng/20211009_01' row format delimited fields terminated by '\t'

select
	a.prov_name, -- 省份
	a.city_name, -- 城市
	b.sales_supervisor_name, -- 销售主管
	a.name, -- 销售员
	a.user_number, --工号
	d.q3_customer_cnt,
	g.have_service,
	f.visit_cnt,
	f.visit_cnt2,
	f.rate_1,
	f.rate_2,
	c.customer_cnt_1,
	c.customer_cnt_2,
	e.cnt_3,
	h.begin_date
from	
	(
	select 
		id,leader_id,user_number,name,user_position,channel,user_source_busi,prov_name,city_name,del_flag,status
	from 
		csx_dw.dws_basic_w_a_user
	where 
		sdt =${hiveconf:current_day}
		--and user_position ='SALES'
		--and status='0' -- 0:启用  1：禁用
		--and del_flag='0' -- 删除标记0正常-1删除
		and prov_name not like '%平台%'
	) a
	-- 主管信息
	left join
		(
		select
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
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
				row_number() over(partition by id order by distance desc) as rank
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt = ${hiveconf:current_day}
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
		) b on b.work_no = a.user_number
	left join
		(
		select
			work_no,
			--count(distinct case when is_cooperative_customer='1' then customer_no else null end) as customer_cnt, -- 合作客户数量
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')='202109' then customer_no else null end) as customer_cnt_1, -- 9月新签客户数
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then customer_no else null end) as customer_cnt_2 -- Q3新签客户数
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:current_day}
		group by 
			work_no
		) c on c.work_no=a.user_number
	left join
		( 
		select 
			work_no,
			count(distinct case when sdt between '20210701' and '20210930' then customer_no else null end) as q3_customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		group by 
			work_no
		) d on d.work_no=a.user_number
	left join
		(
		select
			sales_id,
			count(distinct case when first_order_date between '20210701' and '20210930' then customer_no else null end) as cnt_3
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = ${hiveconf:current_day}
		group by 
			sales_id
		) e on e.sales_id=a.id
	left join
		(
		select
			t1.visit_person_work_no,
			--count(t1.id) as visit_cnt,
			count(case when t2.is_cooperative_customer='合作' then t1.customer_id else null end) as visit_cnt,
			count(case when t2.is_cooperative_customer ='未合作' or t2.is_cooperative_customer is null then t1.customer_id else null end) as visit_cnt2,
			count(case when effective_flag='1' then t1.customer_id else null end)/count(t1.customer_id) as rate_1,
			count(case when effective_flag='0' then t1.customer_id else null end)/count(t1.customer_id) as rate_2
		from
			(
			select
				visit_person_work_no,customer_id,customer_no,visit_time,effective_flag
				--case when effective_flag='0' then '异常' when effective_flag='1' then '有效' esle '其他' end as visit_type
			from
				csx_dw.report_crm_r_a_customer_visit_analysis
			where
				sdt=${hiveconf:current_day}
				and regexp_replace(substr(visit_time,1,7),'-','') between '202107' and '202109'
			) as t1
			left join
				(
				select
					customer_id,case when customer_no !='' then '合作' else '未合作' end as is_cooperative_customer
				from
					csx_dw.dws_crm_w_a_customer_union
				where
					sdt=${hiveconf:current_day}
				) as t2 on t2.customer_id=t1.customer_id
		group by 
			t1.visit_person_work_no
		) f on f.visit_person_work_no=a.user_number
	left join
		(
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt=${hiveconf:current_day}
		group by 
			work_no
		) g on g.work_no=a.user_number
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date,leader_code,leader_name
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = ${hiveconf:current_day}
		) h on h.employee_code=a.user_number			
		
		
