
--hive 表同步 注意分区为上月（执行日期3日），smt_c为EXCEL文件月
-- 标讯中标转化 csx_ods_csx_data_config_bid_list_inversion_df;
-- 标讯中标收集客户商机清单 csx_ods_csx_data_config_bid_list_business_customer_df


--标讯对应客户
drop table csx_analyse_tmp.tmp_biaoxun_customer;
create table csx_analyse_tmp.tmp_biaoxun_customer
as
select distinct a.id,a.customer_code,b.first_sign_date,e.first_sale_date,e.first_order_month
from 
(
	select idd as id,customer_code
	from csx_ods.csx_ods_csx_data_config_bid_list_business_customer_df
	where smt_c='202311'
	and flag='客户'

--标讯对应商机里的客户
union all
select distinct a.id,b.customer_code
from 
	(
	select idd as id,business_number
	from csx_ods.csx_ods_csx_data_config_bid_list_business_customer_df
	where smt_c='202311'
	and flag='商机'
	) a 
join 
	(
	select 
		business_number,		 --	商机号
		customer_id,		 --	客户ID	
		customer_code
	from csx_dim.csx_dim_crm_business_info
	where sdt='current'
	)b on a.business_number=b.business_number
)a
--客户信息 首次签约时间
left join 
(
	select customer_id,customer_code,
	regexp_replace(substr(first_sign_time, 1, 10), '-', '') first_sign_date		 --	签约时间
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on b.customer_code=a.customer_code
--客户首次销售日期，区分老客新标
left join
(
	select customer_code,first_sale_date,
	substr(first_sale_date,1,6) first_order_month
	from csx_dws.csx_dws_crm_customer_active_di
	where sdt = 'current'
)e on e.customer_code=a.customer_code
;

--标讯对应客户 区分新老客
drop table csx_analyse_tmp.tmp_biaoxun_customer_new_old;
create table csx_analyse_tmp.tmp_biaoxun_customer_new_old
as
select b.no as id,a.customer_code,a.first_sale_date,a.first_sign_date,
	regexp_replace(substr(b.win_bid_date, 1, 10), '-', '') as win_bid_date,
	-- case when a.first_sale_date<regexp_replace(substr(b.win_bid_date, 1, 10), '-', '')
	-- 		or a.first_sign_date<regexp_replace(substr(b.win_bid_date, 1, 10), '-', '')
	-- 	then '老客户' 
	-- end cust_new_old
	coalesce(bid_customer_type,'新客户') as cust_new_old
from csx_analyse_tmp.tmp_biaoxun_customer a 
left join 
(
select *
from csx_ods.csx_ods_csx_data_config_bid_list_inversion_df 
where smt_c='202311'
)b on a.id=b.no;


--商机信息
drop table csx_analyse_tmp.tmp_business_information;
create table csx_analyse_tmp.tmp_business_information
as
select a.*,
	c.customer_code,
	--coalesce(c.customer_code,f.customer_code) customer_code,
	e.first_sale_date,
	f.id as id_cust,
	g.id as id_business,
	coalesce(f.id,g.id) as id
from
	(
	select 
		customer_id,		 --	客户ID
		business_number,		 --	商机号
		customer_name,		 --	客户名称
		--first_category_code,		 --	一级客户分类编码
		first_category_name,		 --	一级客户分类名称
		--second_category_code,		 --	二级客户分类编码
		second_category_name,		 --	二级客户分类名称
		--third_category_code,		 --	三级客户分类编码
		third_category_name,		 --	三级客户分类名称
		business_attribute_code,		 --	商机属性编码
		business_attribute_name,		 --	机属性
		performance_region_name,		 --	销售大区名称(业绩划分)
		performance_province_name,		 --	销售归属省区名称
		performance_city_name,		 --	城市组名称(业绩划分)
		owner_user_number work_no,		 --	销售员工号
		owner_user_name sales_name,		 --	销售员名称
		--first_supervisor_work_no,first_supervisor_name,
		--third_supervisor_work_no,third_supervisor_name,			
		business_stage business_stage_code,		 --	阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
		--status,		 --	是否有效 0.无效 1.有效
		case business_stage 
		when 1 then '10%商机'
		when 2 then '25%商机'
		when 3 then '50%商机'
		when 4 then '75%商机'
		when 5 then '100%商机' end business_stage,
		case status 
		when 0 then '停止跟进'
		when 1 then '正常跟进' end status,
		estimate_once_amount,		 --	预估一次性配送金额
		estimate_month_amount,		 --	预估月度配送金额
		estimate_contract_amount,		 --	预计合同签约金额
		gross_profit_rate,		 --	预计毛利率
		regexp_replace(substr(expect_sign_time, 1, 10), '-', '') as expect_sign_date,		 --	预计签约时间
		expect_execute_time,
		contract_cycle,		 --	合同周期		--	预计履约时间
		--regexp_replace(regexp_replace(regexp_replace(regexp_replace(contract_cycle,
		--		'{"input":"', ''), 
		--		'","radio":"1","radioName":"年"}', '年'),
		--		'","radio":"2","radioName":"月"}', '月'), 
		--		'","radio":"3","radioName":"日"}', '日') contract_cycle,		 --	合同周期
		regexp_replace(substr(business_sign_time, 1, 10), '-', '') sign_time,		 --	签约时间
		regexp_replace(substr(create_time, 1, 10), '-', '') create_time,		 --	创建时间
		sdt		 
	from csx_dim.csx_dim_crm_business_info
	where sdt='current'
	--and regexp_replace(substr(create_time, 1, 10), '-', '')>='20211201'
	--and (regexp_replace(substr(business_sign_time, 1, 10), '-', '')>='20211201' or business_sign_time is null)
	--and status='1'  --是否有效 0.无效 1.有效 (status=0,'停止跟进')
	)a			
left join 
	(
	select customer_id,customer_code
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current' 
	and customer_type_code=4
	)c on a.customer_id=c.customer_id	
left join
	(
	select customer_code,first_sale_date
	from csx_dws.csx_dws_crm_customer_active_di
	where sdt = 'current'
	)e on e.customer_code=c.customer_code
left join csx_analyse_tmp.tmp_biaoxun_customer_new_old f on f.customer_code=c.customer_code
left join 
	(select idd as id,business_number
	from csx_ods.csx_ods_csx_data_config_bid_list_business_customer_df
	where smt_c='202311'
	and flag='商机') g on g.business_number=a.business_number
where f.id is not null or g.id is not null
;


--top标讯+商机+销售
drop table csx_analyse_tmp.tmp_bid_business_sale;
create  table csx_analyse_tmp.tmp_bid_business_sale
as
select 
a.id,	--	序号
a.bid_date_new,	 -- 投标日期
a.business_director,	 -- 商机指导人
a.business_principal,	 -- 商机负责人
a.bid_principal,	 -- 投标负责人
a.region_name,	 -- 大区
a.province_name,	 -- 省区
a.city_name,	 -- 城市
a.second_category_name,	 -- 二级行业
a.bid_name,	 -- 项目名称
a.bid_number,	 -- 项目编号
a.original_customer,	 -- 采购单位
a.bid_amount,	 -- 标的金额（万）
a.bid_package_max,	 -- 最大可中包数/总包数
a.bid_amount_max,	 -- 最大可中标金额
a.supply_deadline,	 -- 服务时长（月）
a.business_attribute,	 -- 业务类型
a.early_work,	 -- 前期工作
a.bid_customer_type,	 -- 新老客户标识
a.cooperation_type,	 -- XM
a.bid_company,	 -- 投标主体
a.bid_result,	 -- 项目结果
a.win_bid_date_new,	 -- 结果公布日期
a.win_bid_amount,	 -- 中标金额（万）
a.bid_result_company,	 -- 项目结果
a.bid_result_analysis,	 -- 开标情况
a.bid_result_remark,	 -- 备注
a.customer_number,	 -- 客户号
a.business_number_1,	 -- 商机号
a.remark,	 -- 备注

	a.customer_id,		 --	客户ID
	a.business_number,		 --	商机号
	coalesce(a.customer_code,f.customer_code) customer_code,
	a.customer_name,		 --	客户名称
	a.business_attribute_code,		 --	商机属性编码
	a.business_attribute_name,		 --	商机属性
	a.work_no,		 --	销售员工号
	a.sales_name,		 --	销售员名称
	a.business_stage_code,
	a.business_stage,
	a.status,
	a.estimate_once_amount,		 --	预估一次性配送金额
	a.estimate_month_amount,		 --	预估月度配送金额
	a.estimate_contract_amount,		 --	预计合同签约金额
	a.gross_profit_rate,		 --	预计毛利率
	a.expect_sign_date,		 --	预计签约时间
	a.expect_execute_time,		 --	预计履约时间
	a.contract_cycle,		 --	合同周期
	a.sign_time,		 --	签约时间
	a.create_time,		 --	创建时间
	a.id_cust,
	a.id_business,
	f.first_sale_date,
	f.cust_new_old,
	c.smonth,
	sum(case when a.win_bid_date_new <= c.sdt then c.sale_amt end) sale_amt,
	sum(case when a.win_bid_date_new <= c.sdt then c.rp_sale_amt end) rp_sale_amt,   --日配含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.fl_sale_amt end) fl_sale_amt,   --福利含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.bbc_sale_amt end) bbc_sale_amt,   --BBC含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.bbc_sale_amt end) csfws_sale_amt,   --城市服务商含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.bbc_sale_amt end) ngdz_sale_amt,   --内购大宗含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.profit end) profit,
	sum(case when a.win_bid_date_new <= c.sdt then c.count_day end) count_day
from
(
select 
a.no as id,	 -- 序号
regexp_replace(substr(cast(a.bid_date as string), 1, 10), '-', '') as bid_date_new,	 -- 投标日期
a.business_director,	 -- 商机指导人
a.business_principal,	 -- 商机负责人
a.bid_principal,	 -- 投标负责人
a.region_name,	 -- 大区
a.province_name,	 -- 省区
a.city_name,	 -- 城市
a.second_category_name,	 -- 二级行业
a.bid_name,	 -- 项目名称
a.bid_number,	 -- 项目编号
a.original_customer,	 -- 采购单位
a.bid_amount,	 -- 标的金额（万）
a.bid_package_max,	 -- 最大可中包数/总包数
a.bid_amount_max,	 -- 最大可中标金额
a.supply_deadline,	 -- 服务时长（月）
a.business_attribute,	 -- 业务类型
a.early_work,	 -- 前期工作
a.bid_customer_type,	 -- 新老客户标识
a.cooperation_type,	 -- XM
a.bid_company,	 -- 投标主体
a.bid_result,	 -- 项目结果
regexp_replace(substr(cast(a.win_bid_date as string), 1, 10), '-', '') as win_bid_date_new,	 -- 结果公布日期
a.win_bid_amount,	 -- 中标金额（万）
a.bid_result_company,	 -- 项目结果
a.bid_result_analysis,	 -- 开标情况
a.bid_result_remark,	 -- 备注
regexp_replace(a.customer_number,'\\n|\\r|\\t','') as customer_number,	 -- 客户号
regexp_replace(a.business_number,'\\n|\\r|\\t','') as business_number_1,	 -- 商机号
a.remark,	 -- 备注
	b.customer_id,		 --	客户ID
	b.business_number,		 --	商机号
	b.customer_code,
	b.customer_name,		 --	客户名称
	b.business_attribute_code,		 --	商机属性编码
	b.business_attribute_name,		 --	商机属性
	b.work_no,		 --	销售员工号
	b.sales_name,		 --	销售员名称
	b.business_stage_code,
	b.business_stage,
	b.status,
	b.estimate_once_amount,		 --	预估一次性配送金额
	b.estimate_month_amount,		 --	预估月度配送金额
	b.estimate_contract_amount,		 --	预计合同签约金额
	b.gross_profit_rate,		 --	预计毛利率
	b.expect_sign_date,		 --	预计签约时间
	b.expect_execute_time,		 --	预计履约时间
	b.contract_cycle,		 --	合同周期
	b.sign_time,		 --	签约时间
	b.create_time,		 --	创建时间
	b.id_cust,
	b.id_business
	--b.id,
	--b.first_sale_date,
--`(result_date)?+.+`
--regexp_replace(to_date(from_unixtime(unix_timestamp(result_date,'yyyy/MM/dd'))), '-', '') result_date
from 
(
select *
from csx_ods.csx_ods_csx_data_config_bid_list_inversion_df 
where smt_c='202311'
) a
left join csx_analyse_tmp.tmp_business_information b on a.no=b.id
)a 
left join csx_analyse_tmp.tmp_biaoxun_customer_new_old f on f.id=a.id and f.customer_code=a.customer_code
left join
(
select customer_code,sdt,substr(sdt,1,6) smonth,
	sum(sale_amt) sale_amt,   --含税销售额
	--业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	sum(case when business_type_code='1' then sale_amt end) rp_sale_amt,   --日配含税销售额
	sum(case when business_type_code='2' then sale_amt end) fl_sale_amt,   --福利含税销售额
	sum(case when business_type_code='6' then sale_amt end) bbc_sale_amt,   --BBC含税销售额
	sum(case when business_type_code='4' then sale_amt end) csfws_sale_amt,   --城市服务商含税销售额
	sum(case when business_type_code in('3','4') then sale_amt end) ngdz_sale_amt,   --内购大宗含税销售额
    sum(profit) profit,   --含税毛利
	count(distinct sdt) count_day
from csx_dws.csx_dws_sale_detail_di
where sdt>='20230101'
and channel_code in('1','7','8','9')
group by customer_code,sdt,substr(sdt,1,6) 
)c on c.customer_code=f.customer_code
group by
a.id,	--	序号
a.bid_date_new,	 -- 投标日期
a.business_director,	 -- 商机指导人
a.business_principal,	 -- 商机负责人
a.bid_principal,	 -- 投标负责人
a.region_name,	 -- 大区
a.province_name,	 -- 省区
a.city_name,	 -- 城市
a.second_category_name,	 -- 二级行业
a.bid_name,	 -- 项目名称
a.bid_number,	 -- 项目编号
a.original_customer,	 -- 采购单位
a.bid_amount,	 -- 标的金额（万）
a.bid_package_max,	 -- 最大可中包数/总包数
a.bid_amount_max,	 -- 最大可中标金额
a.supply_deadline,	 -- 服务时长（月）
a.business_attribute,	 -- 业务类型
a.early_work,	 -- 前期工作
a.bid_customer_type,	 -- 新老客户标识
a.cooperation_type,	 -- XM
a.bid_company,	 -- 投标主体
a.bid_result,	 -- 项目结果
a.win_bid_date_new,	 -- 结果公布日期
a.win_bid_amount,	 -- 中标金额（万）
a.bid_result_company,	 -- 项目结果
a.bid_result_analysis,	 -- 开标情况
a.bid_result_remark,	 -- 备注
a.customer_number,	 -- 客户号
a.business_number_1,	 -- 商机号
a.remark,	 -- 备注
	a.customer_id,		 --	客户ID
	a.business_number,		 --	商机号
	coalesce(a.customer_code,f.customer_code),
	a.customer_name,		 --	客户名称
	a.business_attribute_code,		 --	商机属性编码
	a.business_attribute_name,		 --	商机属性
	a.work_no,		 --	销售员工号
	a.sales_name,		 --	销售员名称
	a.business_stage_code,
	a.business_stage,
	a.status,
	a.estimate_once_amount,		 --	预估一次性配送金额
	a.estimate_month_amount,		 --	预估月度配送金额
	a.estimate_contract_amount,		 --	预计合同签约金额
	a.gross_profit_rate,		 --	预计毛利率
	a.expect_sign_date,		 --	预计签约时间
	a.expect_execute_time,		 --	预计履约时间
	a.contract_cycle,		 --	合同周期
	a.sign_time,		 --	签约时间
	a.create_time,		 --	创建时间
	a.id_cust,
	a.id_business,
	f.first_sale_date,
	f.cust_new_old,
	c.smonth
;

--select count(1)
--from csx_analyse_tmp.tmp_bid_business_sale;

  
--top标讯+销售
drop table csx_analyse_tmp.tmp_bid_sale;
create  table csx_analyse_tmp.tmp_bid_sale
as
select 
a.id,	 -- 序号
a.bid_date_new,	 -- 投标日期
a.business_director,	 -- 商机指导人
a.business_principal,	 -- 商机负责人
a.bid_principal,	 -- 投标负责人
a.region_name,	 -- 大区
a.province_name,	 -- 省区
a.city_name,	 -- 城市
a.second_category_name,	 -- 二级行业
a.bid_name,	 -- 项目名称
a.bid_number,	 -- 项目编号
a.original_customer,	 -- 采购单位
a.bid_amount,	 -- 标的金额（万）
a.bid_package_max,	 -- 最大可中包数/总包数
a.bid_amount_max,	 -- 最大可中标金额
a.supply_deadline,	 -- 服务时长（月）
a.business_attribute,	 -- 业务类型
a.early_work,	 -- 前期工作
a.bid_customer_type,	 -- 新老客户标识
a.cooperation_type,	 -- XM
a.bid_company,	 -- 投标主体
a.bid_result,	 -- 项目结果
a.win_bid_date_new,	 -- 结果公布日期
a.win_bid_amount,	 -- 中标金额（万）
a.bid_result_company,	 -- 项目结果
a.bid_result_analysis,	 -- 开标情况
a.bid_result_remark,	 -- 备注
a.customer_number,	 -- 客户号
a.business_number_1,	 -- 商机号
a.remark,	 -- 备注
	e.estimate_contract_amount,		 --	预计合同签约金额
	if(d.id_cust_old>0,'老客户','新客户') id_cust_old,   --标讯新老客标签
	b1.business_stage business_stage_max,	
	b.count_business,
	d.count_customer,
	d.count_cust_old,
    f.business_number_list,
    g.customer_code_list,
    g.customer_name_list,	
	c.smonth,
	--c.sale_amt,c.profit,c.count_day
	count(distinct case when a.win_bid_date_new <= c.sdt then c.sdt end) count_days,
	sum(case when a.win_bid_date_new <= c.sdt then c.sale_amt end) sale_amt,
	sum(case when a.win_bid_date_new <= c.sdt then c.rp_sale_amt end) rp_sale_amt,   --日配含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.fl_sale_amt end) fl_sale_amt,   --福利含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.bbc_sale_amt end) bbc_sale_amt,   --BBC含税销售额	
	sum(case when a.win_bid_date_new <= c.sdt then c.csfws_sale_amt end) csfws_sale_amt,   --城市服务商含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.ngdz_sale_amt end) ngdz_sale_amt,   --内购大宗含税销售额
			
	sum(case when a.win_bid_date_new <= c.sdt then c.sale_amt_old end) sale_amt_old,
	sum(case when a.win_bid_date_new <= c.sdt then c.rp_sale_amt_old end) rp_sale_amt_old,   --老客日配含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.fl_sale_amt_old end) fl_sale_amt_old,   --老客福利含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.bbc_sale_amt_old end) bbc_sale_amt_old,   --老客BBC含税销售额	
	sum(case when a.win_bid_date_new <= c.sdt then c.csfws_sale_amt_old end) csfws_sale_amt_old,   --老客城市服务商含税销售额
	sum(case when a.win_bid_date_new <= c.sdt then c.ngdz_sale_amt_old end) ngdz_sale_amt_old,   --老客内购大宗含税销售额	
	sum(case when a.win_bid_date_new <= c.sdt then c.profit end) profit
from 
	(
	select 
	a.no as id,	 -- 序号
	regexp_replace(substr(cast(a.bid_date as string), 1, 10), '-', '') as bid_date_new,	 -- 投标日期
	a.business_director,	 -- 商机指导人
	a.business_principal,	 -- 商机负责人
	a.bid_principal,	 -- 投标负责人
	a.region_name,	 -- 大区
	a.province_name,	 -- 省区
	a.city_name,	 -- 城市
	a.second_category_name,	 -- 二级行业
	a.bid_name,	 -- 项目名称
	a.bid_number,	 -- 项目编号
	a.original_customer,	 -- 采购单位
	a.bid_amount,	 -- 标的金额（万）
	a.bid_package_max,	 -- 最大可中包数/总包数
	a.bid_amount_max,	 -- 最大可中标金额
	a.supply_deadline,	 -- 服务时长（月）
	a.business_attribute,	 -- 业务类型
	a.early_work,	 -- 前期工作
	a.bid_customer_type,	 -- 新老客户标识
	a.cooperation_type,	 -- XM
	a.bid_company,	 -- 投标主体
	a.bid_result,	 -- 项目结果
	regexp_replace(substr(cast(a.win_bid_date as string), 1, 10), '-', '') as win_bid_date_new,	 -- 结果公布日期
	a.win_bid_amount,	 -- 中标金额（万）
	a.bid_result_company,	 -- 项目结果
	a.bid_result_analysis,	 -- 开标情况
	a.bid_result_remark,	 -- 备注
	regexp_replace(a.customer_number,'\\n|\\r|\\t','') as customer_number,	 -- 客户号
	regexp_replace(a.business_number,'\\n|\\r|\\t','') as business_number_1,	 -- 商机号
	a.remark	 -- 备注
	from 
(
select *
from csx_ods.csx_ods_csx_data_config_bid_list_inversion_df 
where smt_c='202311'
)	a
	) a
--投标对应客户的销售
left join 
(
	select
		id,sdt,smonth,
			sum(sale_amt) sale_amt,   --含税销售额
			--业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			sum(rp_sale_amt) rp_sale_amt,   --日配含税销售额
			sum(fl_sale_amt) fl_sale_amt,   --福利含税销售额
			sum(bbc_sale_amt) bbc_sale_amt,   --BBC含税销售额		
			sum(csfws_sale_amt) csfws_sale_amt,   --城市服务商含税销售额
			sum(ngdz_sale_amt) ngdz_sale_amt,   --内购大宗含税销售额			
			sum(case when cust_new_old='老客户' then sale_amt end) sale_amt_old,
			sum(case when cust_new_old='老客户' then rp_sale_amt end) rp_sale_amt_old,   --老客日配含税销售额
			sum(case when cust_new_old='老客户' then fl_sale_amt end) fl_sale_amt_old,   --老客福利含税销售额
			sum(case when cust_new_old='老客户' then bbc_sale_amt end) bbc_sale_amt_old,   --老客BBC含税销售额	
			sum(case when cust_new_old='老客户' then csfws_sale_amt end) csfws_sale_amt_old,   --老客城市服务商含税销售额
			sum(case when cust_new_old='老客户' then ngdz_sale_amt end) ngdz_sale_amt_old,   --老客内购大宗含税销售额			
			sum(profit) profit   --含税毛利	
	from
	(
		select distinct
			b.id,b.first_sale_date,b.cust_new_old,
			c.customer_code,c.sdt,c.smonth,c.sale_amt,c.rp_sale_amt,c.fl_sale_amt,c.bbc_sale_amt,
			c.csfws_sale_amt,c.ngdz_sale_amt,c.profit,c.count_day
		from csx_analyse_tmp.tmp_biaoxun_customer_new_old b
		left join
		(
			select customer_code,sdt,substr(sdt,1,6) smonth,
				sum(sale_amt) sale_amt,   --含税销售额
				--业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				sum(case when business_type_code='1' then sale_amt end) rp_sale_amt,   --日配含税销售额
				sum(case when business_type_code='2' then sale_amt end) fl_sale_amt,   --福利含税销售额
				sum(case when business_type_code='6' then sale_amt end) bbc_sale_amt,   --BBC含税销售额	
				sum(case when business_type_code='4' then sale_amt end) csfws_sale_amt,   --城市服务商含税销售额
				sum(case when business_type_code in('3','5') then sale_amt end) ngdz_sale_amt,   --内购大宗含税销售额				
				sum(profit) profit,   --含税毛利
				count(distinct sdt) count_day
			from csx_dws.csx_dws_sale_detail_di
			where sdt>='20230101'
			and channel_code in('1','7','8','9')
			group by customer_code,sdt,substr(sdt,1,6) 
		)c on c.customer_code=b.customer_code
	)a
	group by id,sdt,smonth
)c on a.id=c.id
--投标对应的商机数 最快商机进度
left join 
(
select id,
count(business_number) count_business,
max(business_stage_code) business_stage_max
from csx_analyse_tmp.tmp_business_information
group by id
)b on b.id=a.id
left join 
(
select distinct business_stage_code,business_stage
from csx_analyse_tmp.tmp_business_information
)b1 on b1.business_stage_code=b.business_stage_max
--投标对应的客户数
left join 
(
select id,count(customer_code) count_customer,
count(case when cust_new_old='老客户' then customer_code end) count_cust_old,
count(case when cust_new_old='老客户' then id end) id_cust_old
from csx_analyse_tmp.tmp_biaoxun_customer_new_old
--统计客户个数时只统计客户编号的，排除文本记录的
--where customer_code rlike '[0-9]{6}'
group by id
)d on d.id=a.id
--投标对应的商机签约金额
left join 
(
select id,sum(estimate_contract_amount) estimate_contract_amount		 --	预计合同签约金额
from csx_analyse_tmp.tmp_business_information
where id_business is not null
group by id
)e on e.id=a.id
--标讯对应商机列表
left join 
(
  select 
    id,
      concat_ws('：',collect_list(business_number)) as business_number_list
  from (select distinct id, business_number from csx_analyse_tmp.tmp_bid_business_sale)a
  group by id
)f on f.id=a.id 
--标讯对客户编号、客户名称列表
left join 
(
  select 
    id,
      concat_ws('：',collect_list(customer_code)) as customer_code_list,
      concat_ws('：',collect_list(customer_name)) as customer_name_list
  from (select distinct id, customer_code,customer_name from csx_analyse_tmp.tmp_bid_business_sale)a
  group by id
)g on g.id=a.id 
group by
a.id,	--	序号
a.bid_date_new,	 -- 投标日期
a.business_director,	 -- 商机指导人
a.business_principal,	 -- 商机负责人
a.bid_principal,	 -- 投标负责人
a.region_name,	 -- 大区
a.province_name,	 -- 省区
a.city_name,	 -- 城市
a.second_category_name,	 -- 二级行业
a.bid_name,	 -- 项目名称
a.bid_number,	 -- 项目编号
a.original_customer,	 -- 采购单位
a.bid_amount,	 -- 标的金额（万）
a.bid_package_max,	 -- 最大可中包数/总包数
a.bid_amount_max,	 -- 最大可中标金额
a.supply_deadline,	 -- 服务时长（月）
a.business_attribute,	 -- 业务类型
a.early_work,	 -- 前期工作
a.bid_customer_type,	 -- 新老客户标识
a.cooperation_type,	 -- XM
a.bid_company,	 -- 投标主体
a.bid_result,	 -- 项目结果
a.win_bid_date_new,	 -- 结果公布日期
a.win_bid_amount,	 -- 中标金额（万）
a.bid_result_company,	 -- 项目结果
a.bid_result_analysis,	 -- 开标情况
a.bid_result_remark,	 -- 备注
a.customer_number,	 -- 客户号
a.business_number_1,	 -- 商机号
a.remark,	 -- 备注
	e.estimate_contract_amount,		 --	预计合同签约金额
	if(d.id_cust_old>0,'老客户','新客户'),   --标讯新老客标签
	b1.business_stage,	
	b.count_business,
	d.count_customer,
	d.count_cust_old,
    f.business_number_list,
    g.customer_code_list,
    g.customer_name_list,	
	c.smonth	
;



--以下客户2022年后无销售
--select customer_code,first_sale_date,last_order_date
--from csx_dws.csx_dws_crm_customer_active_di
--where sdt = 'current'
--and customer_code in('108810','111130','111291','105790','115769');


--top标讯+商机+销售
select 
id,
bid_date_new,
business_director,
business_principal,
bid_principal,
region_name,
province_name,
city_name,
second_category_name,
regexp_replace(regexp_replace(bid_name,'\n',''),'\r','') as bid_name,
regexp_replace(regexp_replace(bid_number,'\n',''),'\r','') as bid_number,
regexp_replace(regexp_replace(original_customer,'\n',''),'\r','') as original_customer,
bid_amount,
bid_package_max,
bid_amount_max,
supply_deadline,
business_attribute,
early_work,
bid_customer_type,
cooperation_type,
bid_company,
bid_result,
win_bid_date_new,
win_bid_amount,
regexp_replace(regexp_replace(bid_result_company,'\n',''),'\r','') as bid_result_company,
bid_result_analysis,
regexp_replace(regexp_replace(bid_result_remark,'\n',''),'\r','') as bid_result_remark,
regexp_replace(regexp_replace(customer_number,'\n',''),'\r','') as customer_number,
business_number_1,
remark,
customer_id,
business_number,
customer_code,
customer_name,
business_attribute_code,
business_attribute_name,
work_no,
sales_name,
business_stage_code,
business_stage,
status,
estimate_once_amount,
estimate_month_amount,
estimate_contract_amount,
gross_profit_rate,
expect_sign_date,
expect_execute_time,
contract_cycle,
sign_time,
create_time,
id_cust,
id_business,
first_sale_date,
cust_new_old,
smonth,
sale_amt,
rp_sale_amt,
fl_sale_amt,
bbc_sale_amt,
csfws_sale_amt,
ngdz_sale_amt,
profit,
count_day
from csx_analyse_tmp.tmp_bid_business_sale
order by id;




--top标讯+销售
select 
id,
bid_date_new,
business_director,
business_principal,
bid_principal,
region_name,
province_name,
city_name,
second_category_name,
regexp_replace(regexp_replace(bid_name,'\n',''),'\r','') as bid_name,
regexp_replace(regexp_replace(bid_number,'\n',''),'\r','') as bid_number,
regexp_replace(regexp_replace(original_customer,'\n',''),'\r','') as original_customer,
bid_amount,
bid_package_max,
bid_amount_max,
supply_deadline,
business_attribute,
early_work,
bid_customer_type,
cooperation_type,
bid_company,
bid_result,
win_bid_date_new,
win_bid_amount,
regexp_replace(regexp_replace(bid_result_company,'\n',''),'\r','') as bid_result_company,
bid_result_analysis,
regexp_replace(regexp_replace(bid_result_remark,'\n',''),'\r','') as bid_result_remark,
regexp_replace(regexp_replace(customer_number,'\n',''),'\r','') as customer_number,
business_number_1,
remark,
estimate_contract_amount,
id_cust_old,
business_stage_max,
count_business,
count_customer,
count_cust_old,
business_number_list,
customer_code_list,
customer_name_list,
smonth,
count_days,
sale_amt,
rp_sale_amt,
fl_sale_amt,
bbc_sale_amt,
csfws_sale_amt,
ngdz_sale_amt,
sale_amt_old,
rp_sale_amt_old,
fl_sale_amt_old,
bbc_sale_amt_old,
csfws_sale_amt_old,
ngdz_sale_amt_old,
profit
from csx_analyse_tmp.tmp_bid_sale
order by id;


select id,count(1)
from csx_analyse_tmp.tmp_bid_business_sale
group by id;

-- 投标数
select regexp_replace(substr(bid_date,1,7),'-','') smonth,
-- 标讯状态为投标中（剔除弃标审批中的）、中标、未中标、未公示、流标（投标后）、废标、项目取消（投标后）、弃标（投标后）
coalesce(count(1),0) bid_counts,
sum(bid_amount) bid_amount,
sum(case when bid_customer_type_name='老客户' then bid_amount end) bid_amount_old,
sum(case when bid_customer_type_name='新客户' then bid_amount end) bid_amount_new
-- coalesce(sum(case when bid_status_name_new='投标中' then 1 end),0) bid_tbz,  -- 投标中
-- coalesce(sum(case when bid_status_code='5' then 1 end),0) bid_zb,  -- 中标
-- coalesce(sum(case when bid_status_code='6' then 1 end),0) bid_wzb,  -- 未中标
-- coalesce(sum(case when bid_status_name_new='未公示' then 1 end),0) bid_wgs,  -- 未公示
-- coalesce(sum(case when bid_status_code='8' then 1 end),0) bid_lb,  -- 流标
-- coalesce(sum(case when bid_status_code='14' then 1 end),0) bid_xmqx,  -- 项目取消
-- coalesce(sum(case when bid_status_code='11' then 1 end),0) bid_qb,  -- 弃标
-- round(coalesce(sum(bid_amount),0),1)  bid_amount,
-- round(coalesce(sum(win_bid_amount),0),1) win_bid_amount
from csx_analyse.csx_analyse_crm_bid_info_df
where sdt= '20231116'
and bid_status_code in ('3','4','5','6','8','11','14','15')
group by regexp_replace(substr(bid_date,1,7),'-','')
order by smonth;



