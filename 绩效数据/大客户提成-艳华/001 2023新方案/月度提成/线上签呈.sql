-- 线上签呈数据
select 
c.performance_region_name,     --  销售大区名称(业绩划分)
c.performance_province_name,     --  销售归属省区名称
c.performance_city_name,     --  城市组名称(业绩划分)	 

a.id,  -- 主键ID
-- a.create_user_id,  -- 创建人ID
a.petition_type,  -- 签呈类型 1,2
b.name as petition_type_name,
a.petition_type_supplement,  -- 签呈类型补充
-- a.customer_data,  -- 客户信息
a.effective_period,  -- 生效周期 yyyy-mm - yyyy-mm
a.cycle_start,  -- 时间周期开始yyyy-mm-dd
a.cycle_end,  -- 时间周期结束yyyy-mm-dd
a.remark,  -- 内容说明
a.file_json,  -- 附件文件
-- a.approval_id,  -- 审批ID
a.approval_status,  -- 审批状态 0:待发起 1：审批中 2：审批完成 3：审批拒绝
a.status,  -- 状态 0.无效 1.有效
a.create_time,  -- 创建时间
a.create_by,  -- 创建人
a.update_time,  -- 更新时间
a.update_by,  -- 更新人

a.customer_code,  -- 客户编码
c.customer_name,  -- 客户名称
a.attribute,  -- 已选业务类型 1：日配 2：福利 3：大宗贸易 4：M端 5：BBC 6：内购
a.attributeName,  -- 业务类型中文
a.currentCustomerAttribute,  -- 全部业务类型
a.amount,  -- 金额
a.rate,  -- 比例
a.rateType  -- 比例类型
from 
(	
	select
		id,  -- 主键ID
		petition_type,  -- 签呈类型 1,2
		petition_type_supplement,  -- 签呈类型补充
		-- a.customer_data,  -- 客户信息
		regexp_replace(effective_period, '-', '') as effective_period,  -- 生效周期 yyyy-mm - yyyy-mm
		cycle_start,  -- 时间周期开始yyyy-mm-dd
		cycle_end,  -- 时间周期结束yyyy-mm-dd
		remark,  -- 内容说明
		file_json,  -- 附件文件
		-- approval_id,  -- 审批ID
		approval_status,  -- 审批状态 0:待发起 1：审批中 2：审批完成 3：审批拒绝
		status,  -- 状态 0.无效 1.有效
		create_time,  -- 创建时间
		create_by,  -- 创建人
		update_time,  -- 更新时间
		update_by,  -- 更新人			
		
		split(petition_type,'\\,')[1] as petition_type_1,
		get_json_object(customer_data_3,'$.customerNumber') as customer_code,  -- 客户编码
		get_json_object(customer_data_3,'$.customerName') as customer_name,        -- 客户名称
		get_json_object(customer_data_3,'$.attribute') as attribute,  -- 已选业务类型 1：日配 2：福利 3：大宗贸易 4：M端 5：BBC 6：内购
		get_json_object(customer_data_3,'$.attributeName') as attributeName,  -- 业务类型中文
		get_json_object(customer_data_3,'$.currentCustomerAttribute') as currentCustomerAttribute,  -- 全部业务类型
		get_json_object(customer_data_3,'$.amount') as amount,  -- 金额
		get_json_object(customer_data_3,'$.rate') as rate,  -- 比例
		get_json_object(customer_data_3,'$.rateType') as rateType  -- 比例类型
	from
	(
		select
				*,
				concat('{',customer_data_2,'}') as customer_data_3
		from
		(
			select *,
					-- id,
					-- create_user_id,
					-- customer_data,
					-- 第一步，去除customer_data最外层的[]
					-- X第二步， 此处不涉及由于 customer_data 是用 逗号 分割的，会和数据中其他逗号混淆，为了避免分割错误，将其转为其他字符，这里尽量使用数据中不会出现的符号，我这里是将，转为了||				
					regexp_replace(customer_data,'\\[\\{|\\}\\]','') as customer_data_1
			from csx_ods.csx_ods_csx_crm_prod_petition_info_df  
			where split(petition_type,'\\,')[0]=1
			and status=1
			and approval_status not in(0,3)
		-- 第三步，lateral view函数将“customer_data_1”字段炸开进行扩展 将 customer_data_1'|| },{'使用split函数分割转成多行        
		)b lateral view explode(split(customer_data_1,'\\}, \\{')) shiftList As customer_data_2
	) a

-- 合并批量导入的扣减毛利额
union all
select 
		id,  -- 主键ID
		'1' as petition_type,  -- 签呈类型 1,2
		'' as petition_type_supplement,  -- 签呈类型补充
		-- a.customer_data,  -- 客户信息
		substr(regexp_replace(date_sub(current_date, 1), '-', ''),1,6) as effective_period,  -- 生效周期 yyyy-mm - yyyy-mm
		'' as cycle_start,  -- 时间周期开始yyyy-mm-dd
		'' as cycle_end,  -- 时间周期结束yyyy-mm-dd
		'' as remark,  -- 内容说明
		'' as file_json,  -- 附件文件
		-- approval_id,  -- 审批ID
		2 as approval_status,  -- 审批状态 0:待发起 1：审批中 2：审批完成 3：审批拒绝
		status,  -- 状态 0.无效 1.有效
		create_time,  -- 创建时间
		create_by,  -- 创建人
		update_time,  -- 更新时间
		update_by,  -- 更新人			
		
		'8' as petition_type_1,
		customer_number as customer_code,  -- 客户编码
		customer_name,        -- 客户名称
		cast(business_attribute as string) as attribute,  -- 已选业务类型 1：日配 2：福利 3：大宗贸易 4：M端 5：BBC 6：内购
		case 
		when business_attribute=1 then '日配'
		when business_attribute=2 then '福利'
		when business_attribute=3 then '大宗贸易'
		when business_attribute=4 then 'M端'
		when business_attribute=5 then 'BBC'
		when business_attribute=6 then '内购'
		end as attributeName,  -- 业务类型中文
		'' as currentCustomerAttribute,  -- 全部业务类型
		rebate_amount as amount,  -- 金额
		null as rate,  -- 比例
		null as rateType  -- 比例类型
from csx_ods.csx_ods_csx_crm_prod_petition_rebate_config_df  -- 签呈客户返利配置
where status=1
)a 
left join 
(
select code,name,parent_code
from csx_ods.csx_ods_csx_crm_prod_sys_dict_df  -- 系统字典表
where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
-- and parent_code='petition_type'
-- and parent_code like 'petition%'
and parent_code='petition_one_sub_type'
)b on a.petition_type_1=b.code
left join  
   (
	 select
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name,     --  城市组名称(业绩划分)	 
		customer_code,
		customer_name
	 from  csx_dim.csx_dim_crm_customer_info 
	 where sdt='current'	       
	)c on c.customer_code=a.customer_code






