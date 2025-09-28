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
				sdt = ${hiveconf:current_day}
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		) b on b.work_no = a.user_number
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
		
		
		

-- ================================================================================================================
-- 销售员销售额及履约客户数

insert overwrite directory '/tmp/zhangyanpeng/20211021_01_01' row format delimited fields terminated by '\t' 

select
	a.sales_region_name,
	a.sales_province_name,
	a.city_group_name,
	b.sales_manager_name,
	b.sales_supervisor_name,
	a.sales_name,
	a.work_no,
	c.have_service, -- 是否有服务管家
	a.smonth,
	a.sales_value,
	a.customer_cnt
from
	( -- 销售员销售额及履约客户数
	select 
		t2.sales_region_name,t2.sales_province_name,t2.city_group_name,t1.work_no,t2.sales_name,
		substr(t1.sdt,1,6) as smonth,
		sum(t1.sales_value) as sales_value,
		count(distinct t1.customer_no) as customer_cnt
	from
		(
		select
			work_no,sdt,customer_no,sales_value
		from
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210101' and '20210930'
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		) t1
		left join
			(
			select
				customer_no,customer_name,work_no,sales_name,sales_region_name,sales_province_name,city_group_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current'
			)t2 on t2.customer_no=t1.customer_no
	group by 
		t2.sales_region_name,t2.sales_province_name,t2.city_group_name,t1.work_no,t2.sales_name,
		substr(t1.sdt,1,6)
	) a 
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
				sdt = '20211020'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		) b on b.work_no=a.work_no
	left join
		(
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211020'
		group by 
			work_no
		) c on c.work_no=a.work_no
;

-- ================================================================================================================
-- 销售员新签客户数及签约金额

insert overwrite directory '/tmp/zhangyanpeng/20211021_01_02' row format delimited fields terminated by '\t' 

select
	a.sales_region_name,
	a.sales_province_name,
	a.city_group_name,
	b.sales_manager_name,
	b.sales_supervisor_name,
	a.sales_name,
	a.work_no,
	c.have_service, -- 是否有服务管家
	a.smonth,
	a.customer_cnt,
	a.estimate_contract_amount
from
	( -- 销售员新签客户数及签约金额
	select
		work_no,sales_name,sales_region_name,sales_province_name,city_group_name,
		regexp_replace(substr(first_sign_time,1,7),'-','') as smonth,
		count(distinct customer_no) as customer_cnt,
		sum(estimate_contract_amount) as estimate_contract_amount
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='current'
		and regexp_replace(substr(first_sign_time,1,7),'-','') between '202101' and '202109'
	group by 
		work_no,sales_name,sales_region_name,sales_province_name,city_group_name,
		regexp_replace(substr(first_sign_time,1,7),'-','')
	) a 
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
				sdt = '20211020'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		) b on b.work_no=a.work_no
	left join
		(
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211020'
		group by 
			work_no
		) c on c.work_no=a.work_no
;	

-- ================================================================================================================
-- 销售员新履约客户数

insert overwrite directory '/tmp/zhangyanpeng/20211021_01_03' row format delimited fields terminated by '\t' 

select
	a.sales_region_name,
	a.sales_province_name,
	a.city_group_name,
	b.sales_manager_name,
	b.sales_supervisor_name,
	b.sales_name,
	b.work_no,
	c.have_service, -- 是否有服务管家
	a.smonth,
	a.customer_cnt
from
	( -- 销售员新履约客户数
	select
		t2.sales_region_name,t2.sales_province_name,t2.city_group_name,t1.sales_id,
		substr(t1.first_order_date,1,6) as smonth,
		count(distinct t1.customer_no) as customer_cnt
	from 
		(
		select
			sales_id,first_order_date,customer_no
		from
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
			and substr(first_order_date,1,6) between '202101' and '202109'
		) t1
		left join
			(
			select
				customer_no,customer_name,work_no,sales_name,sales_region_name,sales_province_name,city_group_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current'
			)t2 on t2.customer_no=t1.customer_no
	group by
		t2.sales_region_name,t2.sales_province_name,t2.city_group_name,t1.sales_id,
		substr(t1.first_order_date,1,6)
	) a 
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
				sdt = '20211020'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		) b on b.sales_id=a.sales_id
	left join
		(
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211020'
		group by 
			work_no
		) c on c.work_no=b.work_no
;			

-- ================================================================================================================
-- 销售员新履约客户数

insert overwrite directory '/tmp/zhangyanpeng/20211021_01_04' row format delimited fields terminated by '\t' 

select
	a.region_name,
	a.province_name,
	a.city_group_name,
	b.sales_manager_name,
	b.sales_supervisor_name,
	a.visit_person_name,
	a.visit_person_work_no,
	c.have_service, -- 是否有服务管家
	a.smonth,
	a.visit_type,
	a.is_cooperative_customer,
	a.visit_cnt
from
	( -- 销售员新履约客户数
	select
		t1.region_name,t1.province_name,t1.city_group_name,t1.visit_person_work_no,t1.visit_person_name,
		t1.smonth,t1.visit_type,t2.is_cooperative_customer,
		count(t1.customer_id) as visit_cnt
	from
		(
		select
			region_name,province_name,city_group_name,
			visit_person_work_no,visit_person_name,customer_id,customer_no,visit_time,effective_flag,
			case when effective_flag='0' then '异常' when effective_flag='1' then '有效' else '其他' end as visit_type,
			regexp_replace(substr(visit_time,1,7),'-','') as smonth
		from
			csx_dw.report_crm_r_a_customer_visit_analysis
		where
			sdt='20211020'
			and regexp_replace(substr(visit_time,1,7),'-','') between '202101' and '202109'
		) as t1
		left join
			(
			select
				customer_id,case when customer_no !='' then '合作' else '未合作' end as is_cooperative_customer
			from
				csx_dw.dws_crm_w_a_customer_union
			where
				sdt='20211020'
			) as t2 on t2.customer_id=t1.customer_id
	group by 
		t1.region_name,t1.province_name,t1.city_group_name,t1.visit_person_work_no,t1.visit_person_name,
		t1.smonth,t1.visit_type,t2.is_cooperative_customer
	) a 
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
				sdt = '20211020'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		) b on b.work_no=a.visit_person_work_no
	left join
		(
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211020'
		group by 
			work_no
		) c on c.work_no=a.visit_person_work_no
;	



-- ================================================================================================================
-- 销售员商机数量

insert overwrite directory '/tmp/zhangyanpeng/20211021_01_05' row format delimited fields terminated by '\t' 

select
	a.sales_region_name,
	a.sales_province_name,
	a.city_group_name,
	b.sales_manager_name,
	b.sales_supervisor_name,
	a.sales_name,
	a.work_no,
	c.have_service, -- 是否有服务管家
	a.smonth,
	a.business_cnt,
	a.estimate_contract_amount
from
	(
	select 
		t2.sales_region_name,t2.sales_province_name,t2.city_group_name,t2.work_no,t2.sales_name,
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
		left join
			(
			select
				customer_id,customer_no,customer_name,work_no,sales_name,sales_region_name,sales_province_name,city_group_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current'
			)t2 on t2.customer_id=t1.id		
	group by 
		t2.sales_region_name,t2.sales_province_name,t2.city_group_name,t2.work_no,t2.sales_name,
		substr(t1.sdt,1,6)		
	) a 
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
				sdt = '20211020'
			) tmp 
		where 
			tmp.rank = 1
			and sales_supervisor_work_no is not null
		group by 
			sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name,sales_manager_id,sales_manager_name,sales_manager_work_no
		) b on b.work_no=a.work_no
	left join
		(
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211020'
		group by 
			work_no
		) c on c.work_no=a.work_no
;
		

			
		
