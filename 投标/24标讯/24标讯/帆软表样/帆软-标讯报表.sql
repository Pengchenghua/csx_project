

--1、统计表
select
	a.sales_province,a.business_attribute,a.bid_customer_type,
	b.count_bid_mtd,		-- 投标数
	b.count_public_bid_mtd,		-- 公示数
	b.count_win_bid_mtd,		-- 中标数
	b.bid_amount_mtd,		-- 标的金额
	b.bid_amount_max_mtd,		-- 最大中标金额
	b.win_bid_amount_mtd,		-- 中标金额
	c.jt_count_bid_day,		-- 明日即投-投标数
	c.jt_bid_amount_day,		-- 明日即投-标的金额
	c.jt_bid_amount_max_day,		-- 明日即投-最大中标金额
	c.jt_count_bid_week,		-- 本周即投-投标数
	c.jt_bid_amount_week,		-- 本周即投-标的金额
	c.jt_bid_amount_max_week,		-- 本周即投-最大中标金额
	c.jt_count_bid_week_rp300,		-- 本周即投-日配300万以上投标数
	c.jt_bid_amount_week_rp300,		-- 本周即投-日配300万以上标的金额
	d.jt_count_bid,		-- 即投-投标数
	d.jt_bid_amount,		-- 即投-标的金额
	d.jt_bid_amount_max,		-- 即投-最大中标金额
	d.jt_count_bid_rp300,		-- 即投-日配300万以上投标数
	d.jt_bid_amount_rp300,		-- 即投-日配300万以上标的金额
	e.ts_count_bid,		-- 推送-投标数
	e.ts_bid_amount,		-- 推送-标的金额
	e.ts_bid_amount_max		-- 推送-最大中标金额
from
	(
	select 
		distinct sales_province,business_attribute,bid_customer_type
	from csx_b2b_crm.bid_info
	)a
-- 月至今
left join
	(
	select
		sales_province,		-- 销售省份
		business_attribute,		-- 业务类型 1：日配客户 2：福利客户 5：BBC
		bid_customer_type,		-- 客户类型 1新客户 2老客户
		count(bid_number) count_bid_mtd,		-- 投标数
		count(case when bid_status in('5','6') then bid_number end) count_public_bid_mtd,		-- 公示数
		count(case when bid_status='5' then bid_number end) count_win_bid_mtd,		-- 中标数
		sum(bid_amount) bid_amount_mtd,		-- 标的金额
		sum(bid_amount_max) bid_amount_max_mtd,		-- 最大中标金额
		sum(win_bid_amount) win_bid_amount_mtd		-- 中标金额
	from csx_b2b_crm.bid_info
	where bid_date>='2022-10-01'
	and bid_date<='2022-10-20'
	and bid_status not in('1','9','10','11','12','13','14')
	group by sales_province,business_attribute,bid_customer_type
	)b on a.sales_province=b.sales_province and a.business_attribute=b.business_attribute and a.bid_customer_type=b.bid_customer_type
-- 即投：本周即投、明日即投
left join
	(
	select
		sales_province,		-- 销售省份
		business_attribute,		-- 业务类型 1：日配客户 2：福利客户 5：BBC
		bid_customer_type,		-- 客户类型 1新客户 2老客户
		count(case when bid_date>='2022-10-21' then bid_number end) jt_count_bid_day,		-- 明日即投-投标数
		sum(case when bid_date>='2022-10-21' then bid_amount end) jt_bid_amount_day,		-- 明日即投-标的金额
		sum(case when bid_date>='2022-10-21' then bid_amount_max end) jt_bid_amount_max_day,		-- 明日即投-最大中标金额
		
		count(bid_number) jt_count_bid_week,		-- 本周即投-投标数
		sum(bid_amount) jt_bid_amount_week,		-- 本周即投-标的金额
		sum(bid_amount_max) jt_bid_amount_max_week,		-- 本周即投-最大中标金额
		count(case when business_attribute='1' and bid_amount>=300 then bid_number end) jt_count_bid_week_rp300,		-- 本周即投-日配300万以上投标数
		sum(case when business_attribute='1' and bid_amount>=300 then bid_amount end) jt_bid_amount_week_rp300		-- 本周即投-日配300万以上标的金额
	from csx_b2b_crm.bid_info
	where bid_date>='2022-10-20'
	and bid_date<='2022-10-25'
	and bid_status not in('1','9','10','11','12','13','14')
	group by sales_province,business_attribute,bid_customer_type
	)c on a.sales_province=c.sales_province and a.business_attribute=c.business_attribute and a.bid_customer_type=c.bid_customer_type
-- 即投：所有即投
left join
	(
	select
		sales_province,		-- 销售省份
		business_attribute,		-- 业务类型 1：日配客户 2：福利客户 5：BBC
		bid_customer_type,		-- 客户类型 1新客户 2老客户
		count(bid_number) jt_count_bid,		-- 即投-投标数
		sum(bid_amount) jt_bid_amount,		-- 即投-标的金额
		sum(bid_amount_max) jt_bid_amount_max,		-- 即投-最大中标金额
		count(case when business_attribute='1' and bid_amount>=300 then bid_number end) jt_count_bid_rp300,		-- 即投-日配300万以上投标数
		sum(case when business_attribute='1' and bid_amount>=300 then bid_amount end) jt_bid_amount_rp300		-- 即投-日配300万以上标的金额
	from csx_b2b_crm.bid_info
	where bid_date>='2022-10-20'
	and bid_status not in('1','9','10','11','12','13','14')
	and bid_status in('2','3')
	and approval_type in('2')
	and approval_status in('1','2')
	group by sales_province,business_attribute,bid_customer_type
	)d on a.sales_province=d.sales_province and a.business_attribute=d.business_attribute and a.bid_customer_type=d.bid_customer_type
-- 爬虫标讯(每日推送)
left join
	(
	select
		sales_province,		-- 销售省份
		business_attribute,		-- 业务类型 1：日配客户 2：福利客户 5：BBC
		bid_customer_type,		-- 客户类型 1新客户 2老客户
		count(bid_number) ts_count_bid,		-- 推送-投标数
		sum(bid_amount) ts_bid_amount,		-- 推送-标的金额
		sum(bid_amount_max) ts_bid_amount_max		-- 推送-最大中标金额
	from csx_b2b_crm.bid_info
	where bid_date>='2022-10-01'
	and bid_date<'2022-11-01'
	and bid_status not in('9','10','11','12','13','14')
	and bid_source in('1','3')
	group by sales_province,business_attribute,bid_customer_type
	)e on a.sales_province=e.sales_province and a.business_attribute=e.business_attribute and a.bid_customer_type=e.bid_customer_type
;

--2、明细表
select
bid_name,		-- 项目名称
bid_number,		-- 项目编号
bid_customer_name,		-- 项目客户名称
business_number,		-- 商机编号
bid_status,		-- 标讯状态 1.未报名 2.已报名 3.投标中 4.未公示 5.中标 6.未中标 7.流标（投标中） 8.流标（投标后） 9.弃标（投标前） 10.弃标（投标中） 11.弃标（投标后） 12.项目取消（投标前） 13.项目取消（投标中） 14.项目取消（投标后）
bid_ascription,		-- 标讯归属 0.删除 1.标讯池 2.历史标讯 3.标讯池&历史标讯 4.弃标标讯
approval_type,		-- 审批类型 1：关联商机 2：确认报名 3：主动弃标
approval_status,		-- 审批状态 1：审批中 2：审批完成 3：审批拒绝
sales_user_id,		-- 销售Id
sales_user_name,		-- 销售名称
guid_sales_user_id,		-- 销售指导人Id
guid_sales_user_name,		-- 销售指导人名称
bid_user_id,		-- 投标负责人Id
bid_user_name,		-- 投标负责人名称
guid_bid_user_id,		-- 投标指导人Id
guid_bid_user_name,		-- 投标指导人名称
sales_province,		-- 销售省份
sales_city,		-- 销售城市
project_customer_province,		-- 客户省份
project_customer_city,		-- 客户城市
first_category_code,		-- 一级客户分类
second_category_code,		-- 二级客户分类
third_category_code,		-- 三级客户分类
category,		-- 品类
notice_type,		-- 公告类型 1招标公告 2意向公告
business_attribute,		-- 业务类型 1：日配客户 2：福利客户 5：BBC
bid_amount,		-- 标的金额（万元）
bid_package,		-- 项目包数
bid_send_package,		-- 投标包数
bid_package_max,		-- 项目最大中标包数
win_bid_count,		-- 中标家数
bid_amount_max,		-- 项目最大中标金额（万元）
supply_deadline,		-- 供应期限（月）
enroll_date_end,		-- 报名截止日期
bid_date,		-- 投标日期
bid_source,		-- 投标来源 1.每日推送 2.销售提供 3.每日推送&销售提供
cooperation_type,		-- 合作形式 1.自营 2.城市服务商
early_work,		-- 前期工作 1.盲投 2.销售先行 3.销售后行
history_attribute,		-- 历史合作类型,分割 1：日配客户 2：福利客户 5：BBC
bid_customer_type,		-- 客户类型 1新客户 2老客户
bid_company,		-- 投标主体
bid_result,		-- 项目结果
win_bid_date,		-- 公示结果、中标日期
win_bid_amount,		-- 中标金额（万元）
bid_result_analysis,		-- 结果分析
bid_result_remark		-- 项目结果备注
from csx_b2b_crm.bid_info
where bid_date>='2022-10-01'
;


--标讯状态 1.未报名 2.已报名 3.投标中 4.未公示 5.中标 6.未中标 7.流标（投标中） 8.流标（投标后） 9.弃标（投标前） 10.弃标（投标中） 11.弃标（投标后） 12.项目取消（投标前） 13.项目取消（投标中） 14.项目取消（投标后）
--投标数=项目总数-弃标-项目取消，
--中标率=中标数/（投标数-未公示-流标）或者中标率=中标数/（中标数+未中标+废标）




