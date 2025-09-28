

-- 人员信息
select * from csx_dim.csx_dim_uc_user where user_number='81275247'
-- 增加创建时间更新时间字段

alter table csx_data_config.tc_service_level add   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间';
alter table csx_data_config.tc_customer_person_rate_special_rules add   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

-- 服务管家导出处理
SELECT x.* FROM csx_data_config.tc_service_level x
WHERE tc_sdt ='20240731'

;

-- 管家等级导入
INSERT
	into
	csx_data_config.tc_service_level
(region_name,
	province_name,
	city_group_name,
	service_user_work_no,
	service_user_name,
	begin_date,
	position_detail,
	service_user_position,
	s_level,
	salary_level,
	level_sale_rate,
	level_profit_rate,
	tc_sdt)
select
	region_name,
	province_name,
	city_group_name,
	service_user_work_no,
	service_user_name,
	begin_date,
	position_detail,
	service_user_position,
	s_level,
	salary_level,
	level_sale_rate,
	level_profit_rate,
	'20250531' tc_sdt
from
	csx_data_config.tc_service_level x
WHERE
	tc_sdt = '20250430';

-- 调整人员比例-删除过期签呈
select a.*,b.category_first,effective_period
 from 
(SELECT x.* FROM csx_data_config.tc_customer_person_rate_special_rules x
WHERE smt_date ='202411')a 
left join 
(SELECT x.* FROM csx_data_config.tc_customer_special_rules_2023 x
WHERE smt_date ='202411'
and category_first='大客户提成-调整对应人员比例') b on a.customer_no=b.customer_code 
;



-- 签呈导入处理 单客特殊规则维护表 将上个月数据失效的删除后导入当前月份
tc_customer_special_rules_2023
 

DELETE  from csx_data_config.tc_customer_special_rules_2023  where effective_period like '%202408'
-- 验证数据		
select *
FROM
	csx_data_config.tc_customer_special_rules_2023 x	
	where
	smt_date = '202407'
	

;


-- 人员比例根据有需求
-- 查询上月导入
delete from csx_data_config.tc_customer_person_rate_special_rules   where smt_date ='202408' 
	and  customer_no not in ('105150','106721','105182','119062','106423','105181','119990','107404','105156','105177','116886','108127','107885','102202','114017',
		'104469','115987','115982','103199','106481','102686','PF1205','115857','102751','120465','114033','104563','104324','104549','104564','104478','104562',
		'104612','104592','109377','126979','126984','127016','215440','228441','235949','243764','250843','250146','250145')
;
select * from csx_data_config.tc_customer_person_rate_special_rules   where smt_date ='202408'
;

 -- 查询上月导入
INSERT into csx_data_config.tc_customer_person_rate_special_rules  
(biz_id,
	customer_id,
	customer_no,
	customer_name,
	channel_code,
	channel_name,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	sales_id,
	work_no,
	sales_name,
	rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,
	bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	rp_sales_sale_fp_rate,
	rp_sales_profit_fp_rate,
	fl_sales_sale_fp_rate,
	fl_sales_profit_fp_rate,
	bbc_sales_sale_fp_rate,
	bbc_sales_profit_fp_rate,
	rp_service_user_sale_fp_rate,
	rp_service_user_profit_fp_rate,
	fl_service_user_sale_fp_rate,
	fl_service_user_profit_fp_rate,
	bbc_service_user_sale_fp_rate,
	bbc_service_user_profit_fp_rate,
	smt_date)
SELECT
	concat('202508','_',customer_no )biz_id,
	customer_id,
	customer_no,
	customer_name,
	channel_code,
	channel_name,
	region_code,
	region_name,
	a.province_code,
	a.province_name,
	city_group_code,
	city_group_name,
	sales_id,
	work_no,
	sales_name,
	rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,
	bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	rp_sales_sale_fp_rate,
	rp_sales_profit_fp_rate,
	fl_sales_sale_fp_rate,
	fl_sales_profit_fp_rate,
	bbc_sales_sale_fp_rate,
	bbc_sales_profit_fp_rate,
	-- 浙江未挂销售由管家直接服务的客户，申请按原分配系数执行，不做下调
	rp_service_user_sale_fp_rate,
	rp_service_user_profit_fp_rate,
	fl_service_user_sale_fp_rate,
	fl_service_user_profit_fp_rate,
	bbc_service_user_sale_fp_rate,
	bbc_service_user_profit_fp_rate,
	'202508' smt_date 
-- 	b.category_first,
-- 	effective_period 
from 
(SELECT x.* FROM csx_data_config.tc_customer_person_rate_special_rules x
WHERE smt_date ='202507' 
	
)a 
join 
(SELECT DISTINCT x.customer_code,x.category_first,x.effective_period 
FROM csx_data_config.tc_customer_special_rules_2023 x
	WHERE smt_date ='202507'
and category_first='大客户提成-调整对应人员比例' 
and (effective_period != '当月'
		and effective_period not like '%202507'
	)) b on a.customer_no=b.customer_code 
;


	