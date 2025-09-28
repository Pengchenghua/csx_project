
--=============================================================================================================================================================================
--10月数据
select
	a.work_no,
	a.sales_name,
	a.customer_cnt,
	i.customer_cnt,
	g.have_service,
	g.service_customers_cnt,
	i.sales_value,
	i.profit,
	i.profit_rate,
	h.overdue_amount,
	a.customer_cnt2,
	j.customer_cnt,
	j.sales_value,
	i.customer_cnt_3,
	i.sales_value_3,
	i.customer_cnt_3,
	a.customer_cnt_4,
	k.customer_cnt_5,
	k.customer_cnt_6,
	k.customer_cnt_7,
	e.visit_cnt2,
	e.rate_1,
	e.visit_cnt3,
	e.rate_2,
	e.visit_cnt,
	e.rate_3,
	e.rate_4,
	d.business_number_cnt_1,--10%
	d.estimate_contract_amount_1,--10%
	d.business_number_cnt_2,--25%
	d.estimate_contract_amount_2,--25%
	d.business_number_cnt_3,--50%
	d.estimate_contract_amount_3,--50%
	d.business_number_cnt_4,--75%
	d.estimate_contract_amount_4 --75%	
from
	(
	select --销售员信息
		sales_id,work_no,sales_name,
		count(customer_no) as customer_cnt, --总客户数
		count(case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then customer_no else null end) as customer_cnt2, --Q3新签客户数
		count(case when regexp_replace(substr(sign_time,1,7),'-','') between '202110' and '202110' then customer_no else null end) as customer_cnt_4 --10月新签客户数
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = 'current'
		and work_no in ('80936787','1000000571107','80949208','80142191','80796125','81020490','80803819','81143435','80771782','81078947','81111859','81109882','81090020',
		'81111211','81133788','80936816','81130832','81004930','81060217','80857686','80878209','81082968','81099211','80887010','81126138','81111210','81087440','81131050',
		'81048704','80892167','80939468','81143556','80979681','81014012','80954581')
	group by 
		sales_id,work_no,sales_name
	) a 
	left join
		( 
		select 
			work_no,
			sum(sales_value) as sales_value,--10月履约额
			sum(profit) as profit,--10月定价毛利额
			sum(profit)/abs(sum(sales_value)) as profit_rate,--10月定价毛利率
			count(distinct customer_no) as customer_cnt, -- 10月履约客户数
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','') between '202110' and '202110' then customer_no else null end) as customer_cnt_3, --10月新签并履约客户数
			sum(case when regexp_replace(substr(sign_time,1,7),'-','') between '202110' and '202110' then sales_value else null end) as sales_value_3 --10月新签并履约金额
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20211001' and sdt<='20211031'
			and channel_code in('1','7','9')
			and business_type_code !='4'
		group by 
			work_no
		) i on i.work_no=a.work_no
	left join
		( 
		select 
			work_no,
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then customer_no else null end) as customer_cnt, -- Q3新签并履约客户
			sum(case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then sales_value else 0 end) as sales_value -- Q3新签销售额
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210701' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code !='4'
		group by 
			work_no
		) j on j.work_no=a.work_no
	-- 商机数量及金额
	left join
		( 
		select
			t2.work_no,
			count(distinct case when t1.business_stage=1 then t1.business_number else null end) as business_number_cnt_1,--10%
			sum(case when t1.business_stage=1 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_1,--10%
			count(distinct case when t1.business_stage=2 then t1.business_number else null end) as business_number_cnt_2,--25%
			sum(case when t1.business_stage=2 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_2,--25%
			count(distinct case when t1.business_stage=3 then t1.business_number else null end) as business_number_cnt_3,--50%
			sum(case when t1.business_stage=3 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_3,--50%
			count(distinct case when t1.business_stage=4 then t1.business_number else null end) as business_number_cnt_4,--75%
			sum(case when t1.business_stage=4 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_4 --75%
		from
			(
			select 
				customer_id,business_number,estimate_contract_amount,business_stage
			from 
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = '202110'
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
				)t2 on t2.customer_id=t1.customer_id	
		group by 
			t2.work_no
		) d on d.work_no=a.work_no
	-- 拜访
	left join
		( 
		select
			t1.visit_work_no,
			count(t1.id) as visit_cnt,--合计
			count(case when t2.is_cooperative_customer =1 then t1.id else null end) as visit_cnt2,--合作客户
			count(case when t2.is_cooperative_customer =1 then t1.id else null end)/count(t1.id) as rate_1,--合作客户占比
			count(case when t2.is_cooperative_customer =0 then t1.id else null end) as visit_cnt3,--非合作客户
			count(case when t2.is_cooperative_customer =0 then t1.id else null end)/count(t1.id) as rate_2,--非合作客户占比
			count(case when t1.effective_flag =1 then t1.id else null end)/count(t1.id) as rate_3,--有效拜访占比
			count(case when t1.effective_flag =0 then t1.id else null end)/count(t1.id) as rate_4--异常拜访占比
		from
			(
			select
				id,visit_person_id,visit_work_no,customer_id,visit_time,customer_no,sdt,
				case when length(crm_contact_person) < 2 or length(crm_contact_phone) < 8 or cast(substr(visit_time,12,2) as int) >= 21 then 0 else 1 end as effective_flag
			from
				csx_dw.dws_crm_r_d_customer_visit
			where
				sdt>='20211001' and sdt<='20211031'
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
				) as t2 on t2.customer_id=t1.customer_id
		group by 
			t1.visit_work_no
		) e on e.visit_work_no=a.work_no
	-- 是否有服务管家	
	left join
		( 
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service,
			count(case when service_user_work_no !='' then service_user_work_no else null end) as service_customers_cnt
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211031'
		group by 
			work_no
		) g on g.work_no=a.work_no	
	left join
		(
		select --应收预期
			work_no,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt='20211031'
		group by 
			work_no
		)h on h.work_no=a.work_no
	left join
		(
		select
			b.work_no,
			count(distinct case when after_month between '202101' and '202106' then a.customer_no else null end) as customer_cnt_5, --Q1-Q2断约客户
			count(distinct case when after_month between '202107' and '202109' then a.customer_no else null end) as customer_cnt_6, --Q3断约客户
			count(distinct case when after_month between '202110' and '202110' then a.customer_no else null end) as customer_cnt_7 --10月断约客户
		from
			(
			select 
				customer_no,
				substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as after_month
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20190101' and '20211115'
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
				) b on a.customer_no=b.customer_no	
		where
			a.after_month between '202101' and '202111'
		group by
			b.work_no
		) k on k.work_no=a.work_no	
;


--=============================================================================================================================================================================
--11月数据
select
	a.work_no,
	a.sales_name,
	a.customer_cnt,
	i.customer_cnt,
	g.have_service,
	g.service_customers_cnt,
	i.sales_value,
	i.profit,
	i.profit_rate,
	h.overdue_amount,
	a.customer_cnt2,
	j.customer_cnt,
	j.sales_value,
	i.customer_cnt_3,
	i.sales_value_3,
	i.customer_cnt_3,
	a.customer_cnt_4,
	k.customer_cnt_5,
	k.customer_cnt_6,
	k.customer_cnt_7,
	e.visit_cnt2,
	e.rate_1,
	e.visit_cnt3,
	e.rate_2,
	e.visit_cnt,
	e.rate_3,
	e.rate_4,
	d.business_number_cnt_1,--10%
	d.estimate_contract_amount_1,--10%
	d.business_number_cnt_2,--25%
	d.estimate_contract_amount_2,--25%
	d.business_number_cnt_3,--50%
	d.estimate_contract_amount_3,--50%
	d.business_number_cnt_4,--75%
	d.estimate_contract_amount_4 --75%	
from
	(
	select --销售员信息
		sales_id,work_no,sales_name,
		count(customer_no) as customer_cnt, --总客户数
		count(case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then customer_no else null end) as customer_cnt2, --Q3新签客户数
		count(case when regexp_replace(substr(sign_time,1,7),'-','') between '202111' and '202111' then customer_no else null end) as customer_cnt_4 --11月新签客户数
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = 'current'
		and work_no in ('80936787','1000000571107','80949208','80142191','80796125','81020490','80803819','81143435','80771782','81078947','81111859','81109882','81090020',
		'81111211','81133788','80936816','81130832','81004930','81060217','80857686','80878209','81082968','81099211','80887010','81126138','81111210','81087440','81131050',
		'81048704','80892167','80939468','81143556','80979681','81014012','80954581')
	group by 
		sales_id,work_no,sales_name
	) a 
	left join
		( 
		select 
			work_no,
			sum(sales_value) as sales_value,--11月履约额
			sum(profit) as profit,--11月定价毛利额
			sum(profit)/abs(sum(sales_value)) as profit_rate,--11月定价毛利率
			count(distinct customer_no) as customer_cnt, -- 11月履约客户数
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','') between '202110' and '202110' then customer_no else null end) as customer_cnt_3, --11月新签并履约客户数
			sum(case when regexp_replace(substr(sign_time,1,7),'-','') between '202110' and '202110' then sales_value else null end) as sales_value_3 --11月新签并履约金额
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20211101' and sdt<='20211130'
			and channel_code in('1','7','9')
			and business_type_code !='4'
		group by 
			work_no
		) i on i.work_no=a.work_no
	left join
		( 
		select 
			work_no,
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then customer_no else null end) as customer_cnt, -- Q3新签并履约客户
			sum(case when regexp_replace(substr(sign_time,1,7),'-','') between '202107' and '202109' then sales_value else 0 end) as sales_value -- Q3新签销售额
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210701' and sdt<='20210930'
			and channel_code in('1','7','9')
			and business_type_code !='4'
		group by 
			work_no
		) j on j.work_no=a.work_no
	-- 商机数量及金额
	left join
		( 
		select
			t2.work_no,
			count(distinct case when t1.business_stage=1 then t1.business_number else null end) as business_number_cnt_1,--10%
			sum(case when t1.business_stage=1 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_1,--10%
			count(distinct case when t1.business_stage=2 then t1.business_number else null end) as business_number_cnt_2,--25%
			sum(case when t1.business_stage=2 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_2,--25%
			count(distinct case when t1.business_stage=3 then t1.business_number else null end) as business_number_cnt_3,--50%
			sum(case when t1.business_stage=3 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_3,--50%
			count(distinct case when t1.business_stage=4 then t1.business_number else null end) as business_number_cnt_4,--75%
			sum(case when t1.business_stage=4 then t1.estimate_contract_amount else 0 end) as estimate_contract_amount_4 --75%
		from
			(
			select 
				customer_id,business_number,estimate_contract_amount,business_stage
			from 
				csx_dw.ads_crm_r_m_business_customer
			where 
				month = '202111'
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
				)t2 on t2.customer_id=t1.customer_id	
		group by 
			t2.work_no
		) d on d.work_no=a.work_no
	-- 拜访
	left join
		( 
		select
			t1.visit_work_no,
			count(t1.id) as visit_cnt,--合计
			count(case when t2.is_cooperative_customer =1 then t1.id else null end) as visit_cnt2,--合作客户
			count(case when t2.is_cooperative_customer =1 then t1.id else null end)/count(t1.id) as rate_1,--合作客户占比
			count(case when t2.is_cooperative_customer =0 then t1.id else null end) as visit_cnt3,--非合作客户
			count(case when t2.is_cooperative_customer =0 then t1.id else null end)/count(t1.id) as rate_2,--非合作客户占比
			count(case when t1.effective_flag =1 then t1.id else null end)/count(t1.id) as rate_3,--有效拜访占比
			count(case when t1.effective_flag =0 then t1.id else null end)/count(t1.id) as rate_4--异常拜访占比
		from
			(
			select
				id,visit_person_id,visit_work_no,customer_id,visit_time,customer_no,sdt,
				case when length(crm_contact_person) < 2 or length(crm_contact_phone) < 8 or cast(substr(visit_time,12,2) as int) >= 21 then 0 else 1 end as effective_flag
			from
				csx_dw.dws_crm_r_d_customer_visit
			where
				sdt>='20211101' and sdt<='20211130'
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
				) as t2 on t2.customer_id=t1.customer_id
		group by 
			t1.visit_work_no
		) e on e.visit_work_no=a.work_no
	-- 是否有服务管家	
	left join
		( 
		select 
			work_no,
			case when sum(case when service_user_work_no is null or service_user_work_no ='' then 0 else 1 end)>0 then '是' else '否' end as have_service,
			count(case when service_user_work_no !='' then service_user_work_no else null end) as service_customers_cnt
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info 
		where 
			sdt='20211115'
		group by 
			work_no
		) g on g.work_no=a.work_no	
	left join
		(
		select --应收预期
			work_no,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt='20211115'
		group by 
			work_no
		)h on h.work_no=a.work_no
	left join
		(
		select
			b.work_no,
			count(distinct case when after_month between '202101' and '202106' then a.customer_no else null end) as customer_cnt_5, --Q1-Q2断约客户
			count(distinct case when after_month between '202107' and '202109' then a.customer_no else null end) as customer_cnt_6, --Q3断约客户
			count(distinct case when after_month between '202111' and '202111' then a.customer_no else null end) as customer_cnt_7 --11月断约客户
		from
			(
			select 
				customer_no,
				substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as after_month
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20190101' and '20211115'
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
				) b on a.customer_no=b.customer_no	
		where
			a.after_month between '202101' and '202111'
		group by
			b.work_no
		) k on k.work_no=a.work_no	
;

