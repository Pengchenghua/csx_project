
insert overwrite table csx_analyse.csx_analyse_report_oms_bulletin_board_order_di
select
  b.performance_region_code AS region_code,
  b.performance_region_name AS region_name,
  b.performance_province_code as province_code,
  b.performance_province_name as province_name,
  b.performance_city_code as city_group_code,
  b.performance_city_name as city_group_name,
  a.customer_no,
  coalesce(b.customer_name, a.customer_name, '') as customer_name,
  a.child_customer_code,
  a.child_customer_name,
  a.created_user_id,
  coalesce(c.user_name, 'sys') as created_user_name,
  a.recep_user_number,
  a.recep_order_by,
  d.cost_center_code,
  d.cost_center_name,
  d.employee_org_code,
  d.employee_org_name,
  a.recep_order_date,
  a.order_category_desc,
  a.order_category_name,
  a.order_date,
  count(distinct a.order_code) as order_cnt,
  count(a.goods_code) as goods_cnt
from
	(
	select
		customer_code as customer_no, -- 客户编码
		customer_name, -- 客户名称
		sub_customer_code as child_customer_code, -- 子客户编码
		sub_customer_name as child_customer_name, -- 子客户名称
		created_user_id,
		recep_order_user_number as recep_user_number, -- 接单员工号
		recep_order_by, -- 接单员名称
		regexp_replace(to_date(recep_order_time),'-','') as recep_order_date, -- 接单日期
		order_business_type as order_category_desc, -- 订单类型(NORMAL-日配单,WELFARE-福利单,BIGAMOUNT_TRADE-大宗贸易,INNER-内购单)
		case when order_business_type = 'NORMAL' then '日配单' when order_business_type = 'WELFARE' then '福利单'
		  when order_business_type = 'BIGAMOUNT_TRADE' then '大宗贸易' when order_business_type = 'INNER' then '内购单' end as order_category_name, -- 订单类型名称
		order_date,
		order_code,
		goods_code
	from 
		csx_dwd.csx_dwd_csms_yszx_order_detail_di
	where 
		-- sdt >= '${60days_ago}'  
		sdt >= '20220101'  
		-- and regexp_replace(to_date(recep_order_time),'-','') >= '${30days_ago}'
		and regexp_replace(to_date(recep_order_time),'-','') >= '20220101'
		and order_status not in ('CREATED', 'PAID', 'CONFIRMED') 
		and item_status <> 0
		and order_business_type in ('NORMAL','WELFARE')
	) a 
	join
		(
		select
			customer_code, customer_name, performance_region_code, performance_region_name, performance_province_code,
			performance_province_name, performance_city_code, performance_city_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current' and customer_code <> ''
		) b on a.customer_no = b.customer_code
	left join
		(
		select
			user_id,user_name
		from 
			csx_dim.csx_dim_uc_user_extend
		where 
			sdt = 'current' 
			and distance = 0
		) c on a.created_user_id = c.user_id
	left join
		(
		select
			employee_code,cost_center_code,cost_center_name,employee_org_code,employee_org_name
		from
			csx_dim.csx_dim_basic_employee
		where
			sdt='current'
		group by
			employee_code,cost_center_code,cost_center_name,employee_org_code,employee_org_name
		) d on d.employee_code=a.recep_user_number
group by 
	b.performance_region_code,
	b.performance_region_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_city_code,
	b.performance_city_name,
	a.customer_no,
	coalesce(b.customer_name, a.customer_name, ''),
	a.child_customer_code,
	a.child_customer_name,
	a.created_user_id,
	coalesce(c.user_name, 'sys') ,
	a.recep_user_number,
	a.recep_order_by,
	d.cost_center_code,
	d.cost_center_name,
	d.employee_org_code,
	d.employee_org_name,
	a.recep_order_date,
	a.order_category_desc,
	a.order_category_name,
	a.order_date
	
;
/*

create table csx_analyse.csx_analyse_report_oms_bulletin_board_order_di(
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`child_customer_code`            string              COMMENT    '子客户编码',
`child_customer_name`            string              COMMENT    '子客户名称',
`created_user_id`                bigint              COMMENT    '下单用户id',
`created_user_name`              string              COMMENT    '下单用户名称',
`recep_user_number`              string              COMMENT    '接单人工号',
`recep_order_by`                 string              COMMENT    '接单人',
`cost_center_code`               string              COMMENT    '接单人成本中心编码',
`cost_center_name`               string              COMMENT    '接单人成本中心名称',
`employee_org_code`              string              COMMENT    '接单人员组织编码',
`employee_org_name`              string              COMMENT    '接单人员组织名称',
`recep_order_date`               string              COMMENT    '接单日期',
`order_category_desc`            string              COMMENT    '订单类型',
`order_category_name`            string              COMMENT    '订单类型名称',
`order_date`                     string              COMMENT    '下单日期',
`order_cnt`                      bigint              COMMENT    '订单数',
`goods_cnt`                      bigint              COMMENT    'SKU数'

) COMMENT '共享中心客诉看板-接单'
STORED AS PARQUET;

*/	

