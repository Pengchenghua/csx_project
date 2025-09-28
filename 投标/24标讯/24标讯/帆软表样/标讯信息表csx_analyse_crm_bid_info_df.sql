
set hive.tez.container.size=8128;
insert overwrite table csx_analyse.csx_analyse_crm_bid_info_df partition(sdt)
select
	a.id,		-- 主键ID
	a.bid_name,		-- 项目名称
	a.bid_name_alias,		-- 项目名称别名
	a.bid_number,		-- 项目编号
	a.bid_name_number,		-- 项目名称加编号
	a.bid_customer_name,		-- 项目客户名称
	a.business_number,		-- 商机编号
	a.bid_status as bid_status_code,		-- 标讯状态 1.未报名 2.已报名 3.投标中 4.未公示 5.中标 6.未中标 7.流标（投标中） 8.流标（投标后） 9.弃标（投标前） 10.弃标（投标中） 11.弃标（投标后） 12.项目取消（投标前） 13.项目取消（投标中） 14.项目取消（投标后）
	case a.bid_status
	when 1 then '未报名'
	when 2 then '已报名'
	when 3 then '投标中'
	when 4 then '未公示'
	when 5 then '中标'
	when 6 then '未中标'
	when 7 then '流标（投标中）'
	when 8 then '流标（投标后）'
	when 9 then '弃标（投标前）'
	when 10 then '弃标（投标中）'
	when 11 then '弃标（投标后）'
	when 12 then '项目取消（投标前）'
	when 13 then '项目取消（投标中）'
	when 14 then '项目取消（投标后）'
	else '其他'
	end bid_status_name,
	a.bid_change_reason,		-- 变更原因
	a.bid_ascription,		-- 标讯归属 0.删除 1.标讯池 2.历史标讯 3.标讯池&历史标讯 4.弃标标讯
	a.approval_type,		-- 审批类型 1：关联商机 2：确认报名 3：主动弃标
	a.approval_status,		-- 审批状态 1：审批中 2：审批完成 3：审批拒绝
	a.sales_user_id,		-- 销售Id
	a.sales_user_name,		-- 销售名称
	a.guid_sales_user_id,		-- 销售指导人Id
	a.guid_sales_user_name,		-- 销售指导人名称
	a.bid_user_id,		-- 投标负责人Id
	a.bid_user_name,		-- 投标负责人名称
	a.guid_bid_user_id,		-- 投标指导人Id
	a.guid_bid_user_name,		-- 投标指导人名称
    coalesce(b.performance_region_code,b2.performance_region_code, '') as performance_region_code,
    coalesce(b.performance_region_name,b2.performance_region_name, '') as performance_region_name,
    coalesce(b.performance_province_code,b2.performance_province_code, '') as performance_province_code,
    coalesce(b.performance_province_name,b2.performance_province_name, '') as performance_province_name,
    coalesce(b.performance_city_code, '') as performance_city_code,
    coalesce(b.performance_city_name, '') as performance_city_name,	
    -- '' as performance_city_code,
    -- '' as performance_city_name,	
    coalesce(a.sales_province, '') as owner_province_code,		-- 销售省份
    coalesce(d.province, '') as owner_province_name,
    coalesce(if(a.sales_province in ('16','33','34','35','36'), '100000', a.sales_city), '') as owner_city_code,		-- 销售城市
    coalesce(e.region_name, '') as owner_city_name,	
    coalesce(a.project_customer_province, '') as customer_province_code,		-- 客户省份
    coalesce(d2.province, '') as customer_province_name,
    coalesce(if(a.project_customer_province in ('16','33','34','35','36'), '100000', a.project_customer_city), '') as customer_city_code,
    coalesce(e2.region_name, '') as customer_city_name,			-- 客户城市
	a.first_category_code,		-- 一级客户分类
	trim(split(f.full_name, '/')[0]) as first_category_name,
	a.second_category_code,		-- 二级客户分类
	trim(split(f.full_name, '/')[1]) as second_category_name,
	a.third_category_code,		-- 三级客户分类
	concat_ws('/', trim(split(f.full_name, '/')[2]), trim(split(f.full_name, '/')[3]), trim(split(f.full_name, '/')[4])) as third_category_name,
	a.category,		-- 品类
	a.notice_type,		-- 公告类型 1招标公告 2意向公告
	a.contact_person,		-- 客户联系人名称
	a.contact_phone,		-- 客户联系人电话
	a.business_attribute as business_attribute_code,		-- 业务类型 1：日配客户 2：福利客户 3：大宗贸易 4：M端 5：BBC 6：内购
	coalesce(g.name, '') as business_attribute_name,
	a.bid_agent_name,		-- 代理机构名称
	a.agent_person,		-- 代理联系人名称
	a.agent_phone,		-- 代理联系人电话
	a.bid_amount,		-- 标的金额（万元）
	a.bid_package,		-- 项目包数
	a.bid_send_package,		-- 投标包数
	a.bid_package_max,		-- 项目最大中标包数
	a.win_bid_count,		-- 中标家数
	a.bid_amount_max,		-- 项目最大中标金额（万元）
	a.supply_deadline,		-- 供应期限（月）
	a.enroll_date_end,		-- 报名截止日期
	a.get_bid_date,		-- 获得标讯日期
	a.bid_date,		-- 投标日期
	a.bid_source as bid_source_code,		-- 投标来源 1.每日推送 2.销售提供 3.每日推送&销售提供
	case a.bid_source
	when 1 then '每日推送'
	when 2 then '销售提供'
	when 3 then '每日推送&销售提供'
	else '其他'
	end bid_source_name,	
	
	a.cooperation_type as cooperation_type_code,		-- 合作形式 1.自营 2.项目供应商
	case a.cooperation_type
	when 1 then '自营'
	when 2 then '项目供应商'
	else '其他'
	end cooperation_type_name,
	
	a.early_work as early_work_code,		-- 前期工作 1.盲投 2.销售先行 3.销售后行
	case a.early_work
	when 1 then '盲投'
	when 2 then '销售先行'
	when 3 then '销售后行'
	else '其他'
	end early_work_name,
	
	a.history_attribute as history_attribute_code,		-- 历史合作类型,分割 1：日配客户 2：福利客户 5：BBC
	case a.history_attribute
	when 1 then '日配'
	when 2 then '福利'
	when 3 then 'BBC'
	else '其他'
	end history_attribute_name,

	a.bid_customer_type as bid_customer_type_code,		-- 新老客户 1新客户 2老客户
	case a.bid_customer_type
	when 1 then '新客户'
	when 2 then '老客户'
	else '其他'
	end bid_customer_type_name,	
	a.bid_goods,		-- 标的物
	a.bid_link,		-- 标讯链接
	a.bid_company,		-- 投标主体
	a.bid_customer_info,		-- 客户及项目信息
	a.cost_profit,		-- 成本及毛利核算
	a.bid_segment,		-- 招投标环节
	a.compete_situation,		-- 竞争对手情况
	a.other_price,		-- 开标各方报价
	a.bid_result,		-- 项目结果
	a.win_bid_date,		-- 公示结果、中标日期
	a.win_bid_amount,		-- 中标金额（万元）
	a.down_rate,		-- 下浮或折扣率
	a.up_rate,		-- 上浮或折扣率
	a.bid_result_analysis,		-- 结果分析
	a.bid_result_remark,		-- 项目结果备注
	a.settlement_rule,		-- 结算规则
	a.price_rule,		-- 报价规则
	a.bid_files,		-- 招标文件
	a.contract_files,		-- 合同文件
	a.create_time,		-- 创建时间
	a.create_by,		-- 创建人
	a.update_time,		-- 更新时间
	a.update_by,		-- 更新人
	h.new_classify_name,
	i.user_number as sales_user_number,
	j.user_number as guid_sales_user_number,
	k.user_number as bid_user_number,
	l.user_number as guid_bid_user_number,
	m.company_name,
	a.sdt		-- 日期分区{\"FORMAT\":\"yyyymmdd\"}

from
(
select *
from csx_ods.csx_ods_csx_crm_prod_bid_info_df
where sdt='${sdt_yes}'
)a 
left join
( -- 获取数据中心定义业绩归属省区与城市信息
  select distinct
    city_code,
    province_id,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name
  from csx_dim.csx_dim_sales_area_belong_mapping
  where sdt = 'current'
) b on a.sales_province = b.province_id 
		and if(a.sales_province in ('16','33','34','35','36'), '100000', a.sales_city) = b.city_code
left join
( -- 获取数据中心定义业绩归属省区与城市信息
  select distinct
    -- city_code,
    province_id,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name
    -- performance_city_code,
    -- performance_city_name
  from csx_dim.csx_dim_sales_area_belong_mapping
  where sdt = 'current'
  and province_id not in('2','10') -- 江苏 上海
  union all
  select 2 as province_id, '1' as performance_region_code, '华东大区' as performance_region_name, '2' as performance_province_code, '上海市' as performance_province_name
  union all
  select 10 as province_id, '1' as performance_region_code, '华东大区' as performance_region_name, '10' as performance_province_code, '江苏省' as performance_province_name
) b2 on a.sales_province = b2.province_id 
		-- and if(a.sales_province in ('16','33','34','35','36'), '100000', a.sales_city) = b2.city_code
  left join
  ( -- 获取省区
    select * from csx_ods.csx_ods_csx_crm_prod_sys_province_df
    where sdt = '${sdt_yes}'
  ) d on a.sales_province = d.province_code
  left join
  ( -- 获取城市，直辖市可以取到区
    select region_code, region_name
    from csx_ods.csx_ods_csx_crm_prod_sys_region_df
    where sdt = '${sdt_yes}' and (region_level in (0, 2) or parent_code in ('110100','120100','310100','500100'))
  ) e on if(a.sales_province in ('16','33','34','35','36'), '100000', a.sales_city) = e.region_code
  left join
  ( -- 获取客户省区
    select * from csx_ods.csx_ods_csx_crm_prod_sys_province_df
    where sdt = '${sdt_yes}'
  ) d2 on a.project_customer_province = d2.province_code
  left join
  ( -- 获取客户城市，直辖市可以取到区
    select region_code, region_name
    from csx_ods.csx_ods_csx_crm_prod_sys_region_df
    where sdt = '${sdt_yes}' and (region_level in (0, 2) or parent_code in ('110100','120100','310100','500100'))
  ) e2 on if(a.project_customer_province in ('16','33','34','35','36'), '100000', a.project_customer_city) = e2.region_code   
  left join
  ( -- 获取客户分类
    select * from csx_ods.csx_ods_csx_crm_prod_sys_customer_category_df
    where sdt ='${sdt_yes}'
  ) f on concat_ws('/', a.first_category_code, a.second_category_code) = f.path
  left join
  ( -- 获取客户属性
    select code, name from csx_ods.csx_ods_csx_crm_prod_sys_dict_df
    where sdt = '${sdt_yes}' and parent_code = 'customer_attr'
  ) g on cast(a.business_attribute as string) = g.code  
  left join csx_analyse.csx_analyse_fr_new_customer_classify_mf h on a.second_category_code = h.second_category_code
  left join 
  (select   user_id,user_number,user_name,user_position,city_name,province_name
        from csx_dim.csx_dim_uc_user
        where sdt='${sdt_yes}'
        -- and status = 0 
        and delete_flag = '0'
  ) i on a.sales_user_id = i.user_id
   left join 
  (select   user_id,user_number,user_name,user_position,city_name,province_name
        from csx_dim.csx_dim_uc_user
        where sdt='${sdt_yes}'
        -- and status = 0 
        and delete_flag = '0'
  ) j on a.guid_sales_user_id = j.user_id 
  left join 
  (select   user_id,user_number,user_name,user_position,city_name,province_name
        from csx_dim.csx_dim_uc_user
        where sdt='${sdt_yes}'
        -- and status = 0 
        and delete_flag = '0'
  ) k on a.bid_user_id = k.user_id 
  left join 
  (select   user_id,user_number,user_name,user_position,city_name,province_name
        from csx_dim.csx_dim_uc_user
        where sdt='${sdt_yes}'
        -- and status = 0 
        and delete_flag = '0'
  ) l on a.guid_bid_user_id = l.user_id 
  left join 
  (select   company_code,company_name
        from csx_dim.csx_dim_basic_company
        where sdt='${sdt_yes}'
  ) m on a.bid_company = m.company_code   
;


--标讯状态 1.未报名 2.已报名 3.投标中 4.未公示 5.中标 6.未中标 7.流标（投标中） 8.流标（投标后） 9.弃标（投标前） 10.弃标（投标中） 11.弃标（投标后） 12.项目取消（投标前） 13.项目取消（投标中） 14.项目取消（投标后）
--投标数=项目总数-弃标-项目取消，
--中标率=中标数/（投标数-未公示-流标）或者中标率=中标数/（中标数+未中标+废标）


/*
---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------
CREATE TABLE `csx_analyse.csx_analyse_crm_bid_info_df`(
`id`	string	COMMENT	'主键ID',
`bid_name`	string	COMMENT	'项目名称',
`bid_name_alias`	string	COMMENT	'项目名称别名',
`bid_number`	string	COMMENT	'项目编号',
`bid_name_number`	string	COMMENT	'项目名称加编号',
`bid_customer_name`	string	COMMENT	'项目客户名称',
`business_number`	string	COMMENT	'商机编号',
`bid_status_code`	int	COMMENT	'标讯状态编码',
`bid_status_name`	string	COMMENT	'标讯状态',
`bid_change_reason`	string	COMMENT	'变更原因',
`bid_ascription`	int	COMMENT	'标讯归属 0.删除 1.标讯池 2.历史标讯 3.标讯池&历史标讯 4.弃标标讯',
`approval_type`	int	COMMENT	'审批类型 1：关联商机 2：确认报名 3：主动弃标',
`approval_status`	int	COMMENT	'审批状态 1：审批中 2：审批完成 3：审批拒绝',
`sales_user_id`	string	COMMENT	'销售Id',
`sales_user_name`	string	COMMENT	'销售名称',
`guid_sales_user_id`	string	COMMENT	'销售指导人Id',
`guid_sales_user_name`	string	COMMENT	'销售指导人名称',
`bid_user_id`	string	COMMENT	'投标负责人Id',
`bid_user_name`	string	COMMENT	'投标负责人名称',
`guid_bid_user_id`	string	COMMENT	'投标指导人Id',
`guid_bid_user_name`	string	COMMENT	'投标指导人名称',
`performance_region_code`	string	COMMENT	'业绩归属大区编码',
`performance_region_name`	string	COMMENT	'业绩归属大区名称',
`performance_province_code`	string	COMMENT	'绩效归属省区编码',
`performance_province_name`	string	COMMENT	'绩效归属省区名称',
`performance_city_code`	string	COMMENT	'绩效归属城市编码',
`performance_city_name`	string	COMMENT	'绩效归属城市名称',
`owner_province_code`	string	COMMENT	'销售省份编码',
`owner_province_name`	string	COMMENT	'销售省份',
`owner_city_code`	string	COMMENT	'销售城市编码',
`owner_city_name`	string	COMMENT	'销售城市',
`customer_province_code`	string	COMMENT	'客户省份编码',
`customer_province_name`	string	COMMENT	'客户省份',
`customer_city_code`	string	COMMENT	'客户城市编码',
`customer_city_name`	string	COMMENT	'客户城市',
`first_category_code`	string	COMMENT	'一级客户分类编码',
`first_category_name`	string	COMMENT	'一级客户分类名称',
`second_category_code`	string	COMMENT	'二级客户分类编码',
`second_category_name`	string	COMMENT	'二级客户分类名称',
`third_category_code`	string	COMMENT	'三级客户分类编码',
`third_category_name`	string	COMMENT	'三级客户分类名称',
`category`	string	COMMENT	'品类',
`notice_type`	int	COMMENT	'公告类型 1招标公告 2意向公告',
`contact_person`	string	COMMENT	'客户联系人名称',
`contact_phone`	string	COMMENT	'客户联系人电话',
`business_attribute_code`	int	COMMENT	'商机属性编码(1：日配 2：福利 3：大宗贸易 4：m端 5：bbc 6：内购)',
`business_attribute_name`	string	COMMENT	'商机属性名称',
`bid_agent_name`	string	COMMENT	'代理机构名称',
`agent_person`	string	COMMENT	'代理联系人名称',
`agent_phone`	string	COMMENT	'代理联系人电话',
`bid_amount`	decimal(26,6)	COMMENT	'标的金额（万元）',
`bid_package`	int	COMMENT	'项目包数',
`bid_send_package`	int	COMMENT	'投标包数',
`bid_package_max`	int	COMMENT	'项目最大中标包数',
`win_bid_count`	int	COMMENT	'中标家数',
`bid_amount_max`	decimal(26,6)	COMMENT	'项目最大中标金额（万元）',
`supply_deadline`	int	COMMENT	'供应期限（月）',
`enroll_date_end`	string	COMMENT	'报名截止日期',
`get_bid_date`	string	COMMENT	'获得标讯日期',
`bid_date`	string	COMMENT	'投标日期',
`bid_source_code`	int	COMMENT	'投标来源编码',
`bid_source_name`	string	COMMENT	'投标来源',
`cooperation_type_code`	int	COMMENT	'合作形式编码 ',
`cooperation_type_name`	string	COMMENT	'合作形式 ',
`early_work_code`	int	COMMENT	'前期工作编码',
`early_work_name`	string	COMMENT	'前期工作',
`history_attribute_code`	string	COMMENT	'历史合作类型编码',
`history_attribute_name`	string	COMMENT	'历史合作类型',
`bid_customer_type_code`	int	COMMENT	'新老客户编码 ',
`bid_customer_type_name`	string	COMMENT	'新老客户 ',
`bid_goods`	string	COMMENT	'标的物',
`bid_link`	string	COMMENT	'标讯链接',
`bid_company`	string	COMMENT	'投标主体',
`bid_customer_info`	string	COMMENT	'客户及项目信息',
`cost_profit`	decimal(26,6)	COMMENT	'成本及毛利核算',
`bid_segment`	string	COMMENT	'招投标环节',
`compete_situation`	string	COMMENT	'竞争对手情况',
`other_price`	string	COMMENT	'开标各方报价',
`bid_result`	string	COMMENT	'项目结果',
`win_bid_date`	string	COMMENT	'公示结果、中标日期',
`win_bid_amount`	decimal(26,6)	COMMENT	'中标金额（万元）',
`down_rate`	decimal(26,6)	COMMENT	'下浮或折扣率',
`up_rate`	decimal(26,6)	COMMENT	'上浮或折扣率',
`bid_result_analysis`	string	COMMENT	'结果分析',
`bid_result_remark`	string	COMMENT	'项目结果备注',
`settlement_rule`	string	COMMENT	'结算规则',
`price_rule`	string	COMMENT	'报价规则',
`bid_files`	string	COMMENT	'招标文件',
`contract_files`	string	COMMENT	'合同文件',
`create_time`	timestamp	COMMENT	'创建时间',
`create_by`	string	COMMENT	'创建人',
`update_time`	timestamp	COMMENT	'更新时间',
`update_by`	string	COMMENT	'更新人'
) COMMENT '投标信息表'
PARTITIONED BY (`sdt` string COMMENT '日期分区')



