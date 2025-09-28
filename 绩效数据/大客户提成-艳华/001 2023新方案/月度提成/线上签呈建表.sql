--hive 提成-线上签呈数据
drop table if exists csx_analyse.csx_analyse_tc_customer_petition_info_mi;
create table csx_analyse.csx_analyse_tc_customer_petition_info_mi(
`biz_id` string	COMMENT	'业务主键',
`id_2`	string	COMMENT	'业务库id',
`petition_type` string COMMENT '签呈类型编码',
`petition_type_name1` string COMMENT '签呈类型1级',
`petition_type_name2` string COMMENT '签呈类型2级',
`petition_type_supplement` string COMMENT '签呈类型补充',
`performance_region_code` string COMMENT '大区编码',
`performance_region_name` string COMMENT '大区名称',
`performance_province_code` string COMMENT '省区编码',
`performance_province_name` string COMMENT '省区名称',
`performance_city_code` string COMMENT '城市编码',
`performance_city_name` string COMMENT '城市名称',
`sales_id` STRING  COMMENT '销售员id',
`work_no` STRING  COMMENT '销售员工号',
`sales_name` STRING  COMMENT '销售员',
`rp_service_user_id` STRING  COMMENT '日配_服务管家id',
`rp_service_user_work_no` STRING  COMMENT '日配_服务管家工号',
`rp_service_user_name` STRING  COMMENT '日配_服务管家',
`fl_service_user_id` STRING  COMMENT '福利_服务管家id',
`fl_service_user_work_no` STRING  COMMENT '福利_服务管家工号',
`fl_service_user_name` STRING  COMMENT '福利_服务管家',
`bbc_service_user_id` STRING  COMMENT 'bbc_服务管家id',
`bbc_service_user_work_no` STRING  COMMENT 'bbc_服务管家工号',
`bbc_service_user_name` STRING  COMMENT 'bbc_服务管家',
`customer_code` string COMMENT '客户编码',
`customer_name` string COMMENT '客户名称',
`attribute_code` string COMMENT '业务类型编码',
`attribute_name` string COMMENT '业务类型',
`attribute_code_all` string COMMENT '客户全业务类型编码',
`amount` decimal(20,6) COMMENT '调整金额',
`rate` decimal(10,2) COMMENT '调整比例',
`ratetype` string COMMENT '调整比例类型',
`effective_period` string COMMENT '时间期限',
`cycle_start` string COMMENT '时间周期开始yyyy-mm-dd',
`cycle_end` string COMMENT '时间周期结束yyyy-mm-dd',
`remark` string COMMENT '备注',
`file_json` string COMMENT '附件文件',
`approval_status` string COMMENT '审批状态 1：审批中 2：审批完成 3：审批拒绝',
`status` string COMMENT '0:启用  1：禁用',
`create_time` string COMMENT '创建时间',
`create_by` string COMMENT '创建人',
`update_time` string COMMENT '更新时间',
`update_by` string COMMENT '更新人',
`update_time_b` string COMMENT '更新时间_表任务',
`sdt` string COMMENT '更新日期'
) COMMENT '提成-线上签呈数据'
PARTITIONED BY (smt string COMMENT '日期分区')
;


-- 线上签呈数据 
insert overwrite table csx_analyse.csx_analyse_tc_customer_petition_info_mi partition(smt)
select 
	concat_ws('-',substr(regexp_replace(add_months('${sdt_yes_date}',0),'-',''), 1, 6),a.id,a.petition_type,a.customer_code,a.attribute,substr(a.amount,1,10)) as biz_id,
	a.id as id_2,  -- 主键ID
	-- a.create_user_id,  -- 创建人ID
	a.petition_type,  -- 签呈类型 1,2
	b1.name as petition_type_name1,
	b2.name as petition_type_name2,
	a.petition_type_supplement,  -- 签呈类型补充
	c.performance_region_code,
	c.performance_region_name,     --  销售大区名称(业绩划分)
	c.performance_province_code,
	c.performance_province_name,     --  销售归属省区名称
	c.performance_city_code,
	c.performance_city_name,     --  城市组名称(业绩划分)	
	a.customer_code,  -- 客户编码
	c.customer_name,  -- 客户名称
	a.attribute as attribute_code,  -- 已选业务类型 1：日配 2：福利 3：大宗贸易 4：M端 5：BBC 6：内购
	a.attributeName as attribute_name,  -- 业务类型中文
	a.currentCustomerAttribute as attribute_code_all,  -- 全部业务类型
	a.amount,  -- 金额
	a.rate,  -- 比例
	a.ratetype,  -- 比例类型
	-- a.customer_data,  -- 客户信息
	a.effective_period,  -- 生效周期 yyyy-mm - yyyy-mm
	a.cycle_start,  -- 时间周期开始yyyy-mm-dd
	a.cycle_end,  -- 时间周期结束yyyy-mm-dd
	a.remark,  -- 内容说明
	a.file_json,  -- 附件文件
	-- a.approval_id,  -- 审批ID
	case 
	when a.approval_status='0' then '待发起'
	when a.approval_status='1' then '审批中'
	when a.approval_status='2' then '审批完成'
	when a.approval_status='3' then '审批拒绝'
	else a.approval_status end as approval_status,  -- 审批状态 0:待发起 1：审批中 2：审批完成 3：审批拒绝
	if(a.status='0','无效','有效') as status,  -- 状态 0.无效 1.有效
	a.create_time,  -- 创建时间
	a.create_by,  -- 创建人
	a.update_time,  -- 更新时间
	a.update_by,  -- 更新人
	from_utc_timestamp(current_timestamp(),'GMT') update_time_b,
	regexp_replace(substr(a.update_time,1,10),'-','') as sdt,
	d.sales_id_new as sales_id,
	d.work_no_new as work_no,
	d.sales_name_new as sales_name,
	d.rp_service_user_id_new as rp_service_user_id,
	d.rp_service_user_work_no_new as rp_service_user_work_no,		
	d.rp_service_user_name_new as rp_service_user_name,

	d.fl_service_user_id_new as fl_service_user_id,
	d.fl_service_user_work_no_new as fl_service_user_work_no,
	d.fl_service_user_name_new as fl_service_user_name,

	d.bbc_service_user_id_new as bbc_service_user_id,	
	d.bbc_service_user_work_no_new as bbc_service_user_work_no,
	d.bbc_service_user_name_new as bbc_service_user_name,	
	regexp_replace(substr(a.update_time,1,7),'-','') as smt -- 统计日期
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
		split(petition_type,'\\,')[0] as petition_type_1,
		split(petition_type,'\\,')[1] as petition_type_2,
		get_json_object(customer_data_3,'$.customerNumber') as customer_code,  -- 客户编码
		get_json_object(customer_data_3,'$.customerName') as customer_name,        -- 客户名称
		get_json_object(customer_data_3,'$.attribute') as attribute,  -- 已选业务类型 1：日配 2：福利 3：大宗贸易 4：M端 5：BBC 6：内购
		get_json_object(customer_data_3,'$.attributeName') as attributeName,  -- 业务类型中文
		get_json_object(customer_data_3,'$.currentCustomerAttribute') as currentCustomerAttribute,  -- 全部业务类型
		get_json_object(customer_data_3,'$.amount') as amount,  -- 金额
		get_json_object(customer_data_3,'$.rate') as rate,  -- 比例
		get_json_object(customer_data_3,'$.rateType') as ratetype  -- 比例类型
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
			where status=1
			-- and split(petition_type,'\\,')[0]=1  -- 1 大客户 2 业务代理
			and approval_status not in(0,3)
			and regexp_replace(substr(update_time,1,7),'-','')=substr(regexp_replace(add_months('${sdt_yes_date}',0),'-',''), 1, 6) -- 当月提交的签呈
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
			-- substr(regexp_replace(date_sub(current_date, 1), '-', ''),1,6) as effective_period,  -- 生效周期 yyyy-mm - yyyy-mm
			regexp_replace(substr(create_time,1,7),'-','') as effective_period,  -- 生效周期 yyyy-mm - yyyy-mm
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
			
			'1' as petition_type_1,
			'8' as petition_type_2,
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
	and regexp_replace(substr(update_time,1,7),'-','')=substr(regexp_replace(add_months('${sdt_yes_date}',-0),'-',''), 1, 6)
)a 
-- 签呈大类别
left join 
(
	select code,name,parent_code
	from csx_ods.csx_ods_csx_crm_prod_sys_dict_df  -- 系统字典表
	where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
	and parent_code='petition_type'
	-- and parent_code like 'petition%'
	-- and parent_code='petition_one_sub_type'
)b1 on a.petition_type_1=b1.code
-- 签呈子类别
left join 
(
	select code,name,parent_code
	from csx_ods.csx_ods_csx_crm_prod_sys_dict_df  -- 系统字典表
	where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
	-- and parent_code='petition_type'
	and parent_code like 'petition%'
	-- and parent_code='petition_one_sub_type'
)b2 on a.petition_type_2=b2.code
left join  
(
	select
		performance_region_code,
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_code,
		performance_province_name,     --  销售归属省区名称
		performance_city_code,
		performance_city_name,     --  城市组名称(业绩划分)	 
		customer_code,
		customer_name
	from  csx_dim.csx_dim_crm_customer_info 
	where sdt='current'	       
)c on c.customer_code=a.customer_code
left join  
(
select *
from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)d on d.customer_no=a.customer_code
;




petition_type_name1


select *
from csx_analyse.csx_analyse_tc_customer_petition_info_mi
where 
	1=1 
	${if(len(EDATE)==0,"","AND smt in( '"+EDATE+"')")}
	${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	${if(len(city)==0,"","AND performance_city_name in( '"+city+"') ")}
	${if(len(customer_no)==0,"","and customer_code in ('"+replace(customer_no,",",
	"','")+"') ")}
	${if(len(petition_type_name1)==0,"","and petition_type_name1 in ('"+petition_type_name1+"') ")}
	${if(len(sales_name)==0,"","and sales_name in ('"+replace(sales_name,",","','")+"') ")}
	${if(len(rp_service_name)==0,"","and rp_service_user_name in ('"+replace(rp_service_name,",","','")+"') ")}
	${if(len(fl_service_name)==0,"","and fl_service_user_name in ('"+replace(fl_service_name,",","','")+"') ")}
	${if(len(bbc_service_name)==0,"","and bbc_service_user_name in ('"+replace(bbc_service_name,",","','")+"') ")}
order by sdt desc


http://fr.csxdata.cn/webroot/decision/view/report?viewlet=CRM%252F%25E7%25BA%25BF%25E4%25B8%258A%25E7%25AD%25BE%25E5%2591%2588.cpt&ref_t=design&ref_c=e52ef43a-75d2-422d-b4d6-4f3cd33cfe74



