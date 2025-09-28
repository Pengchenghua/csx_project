--hive 大客户提成-核销订单的待核销金额
drop table if exists csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi;
create table csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi(
`biz_id`	string	COMMENT	'业务主键',
`channel_code` string COMMENT '渠道编码',
`channel_name` string COMMENT '渠道名称',
`region_code` string COMMENT '大区编码',
`region_name` string COMMENT '大区名称',
`province_code` string COMMENT '省区编码',
`province_name` string COMMENT '省区名称',
`city_group_code` string COMMENT '城市编码',
`city_group_name` string COMMENT '城市名称',
`sales_id` string COMMENT '销售员id',
`work_no` string COMMENT '销售员工号',
`sales_name` string COMMENT '销售员',
`rp_service_user_id` string COMMENT '日配客服经理id',
`rp_service_user_work_no` string COMMENT '日配客服经理工号',
`rp_service_user_name` string COMMENT '日配客服经理',
`fl_service_user_id` string COMMENT '福利客服经理id',
`fl_service_user_work_no` string COMMENT '福利客服经理工号',
`fl_service_user_name` string COMMENT '福利客服经理',
`bbc_service_user_id` string COMMENT 'bbc客服经理id',
`bbc_service_user_work_no` string COMMENT 'bbc客服经理工号',
`bbc_service_user_name` string COMMENT 'bbc客服经理',
`bill_type` string COMMENT '单据类型',
`sdt` string COMMENT '订单数据源分区日期',
`source_bill_no` string COMMENT '来源单号',
`customer_code` string COMMENT '客户编码',
`customer_name` string COMMENT '客户名称',
`credit_code` string COMMENT '信控号',
`happen_date` string COMMENT '发生时间',
`company_code` string COMMENT '签约公司编码',
`source_sys` string COMMENT '来源系统',
`reconciliation_period` string COMMENT '对账周期',
`bill_date` string COMMENT '结算日',
`overdue_date` string COMMENT '逾期开始日期',
`paid_date` string COMMENT '核销日期',
`order_amt` decimal(20,6) COMMENT '源单据对账金额',
`unpay_amt` decimal(20,6) COMMENT '历史核销剩余金额',
`pay_amt_old` decimal(20,6) COMMENT '核销金额_原始',
`pay_amt` decimal(20,6) COMMENT '核销金额',
`business_type_code` string COMMENT '业务类型编码',
`business_type_name` string COMMENT '业务类型名称',
`status` string COMMENT '是否有效 0.无效 1.有效',
`sale_amt` decimal(20,6) COMMENT '销售额',
`profit` decimal(20,6) COMMENT '毛利额',
`sale_amt_jiushui` decimal(20,6) COMMENT '需剔除酒水销售额',
`profit_jiushui` decimal(20,6) COMMENT '需剔除酒水毛利额',
`update_time`	timestamp	COMMENT    '更新时间',
`smt_ct`	string	COMMENT	'日期分区复制'
) COMMENT '大客户提成-核销订单的待核销金额'
PARTITIONED BY (smt string COMMENT '日期分区')
;



--hive 大客户提成-人员毛利目标达成情况
drop table if exists csx_analyse.csx_analyse_tc_person_profit_target_rate;
create table csx_analyse.csx_analyse_tc_person_profit_target_rate(
`biz_id`	string	COMMENT	'业务主键',
`region_code` string COMMENT '大区编码',
`region_name` string COMMENT '大区名称',
`province_code` string COMMENT '省区编码',
`province_name` string COMMENT '省区名称',
`city_group_code` string COMMENT '城市编码',
`city_group_name` string COMMENT '城市名称',
`user_position` string COMMENT '岗位类别',
`sales_id` string COMMENT '销售员id',
`work_no` string COMMENT '销售员工号',
`sales_name` string COMMENT '销售员',
`begin_date` string COMMENT '入职日期',
`begin_less_1year_flag` string COMMENT '入职是否小于1年',
`sale_amt` decimal(20,6) COMMENT '销售额',
`profit` decimal(20,6) COMMENT '毛利额',
`profit_basic` decimal(20,6) COMMENT '毛利目标',
`profit_target_rate` decimal(20,6) COMMENT '毛利目标达成系数',
`update_time`	timestamp	COMMENT    '更新时间',
`smt_ct`	string	COMMENT	'日期分区复制'
) COMMENT '大客户提成-人员毛利目标达成情况'
PARTITIONED BY (smt string COMMENT '日期分区')
;



--hive 大客户提成-回款订单明细
drop table if exists csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail;
create table csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail(
`biz_id`	string	COMMENT	'业务主键',
`bill_type`	string	COMMENT	'单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单',
`source_bill_no`	string	COMMENT	'来源单号',
`customer_code`	string	COMMENT	'客户编码',
`customer_name`	string	COMMENT	'客户名称',
`credit_code`	string	COMMENT	'信控编号',
`happen_date`	string	COMMENT	'发生日期',
`company_code`	string	COMMENT	'公司代码',
`account_period_code`	string	COMMENT	'账期编码',
`account_period_name`	string	COMMENT	'账期名称',
`account_period_value`	string	COMMENT	'账期值',
`source_sys`	string	COMMENT	'来源系统 mall b端销售 bbc bbc端 begin期初',
`reconciliation_period`	decimal(20,0)	COMMENT	'对账周期',
`bill_date`	string	COMMENT	'结算日期',
`overdue_date`	string	COMMENT	'逾期日期',
`paid_date`	string	COMMENT	'回款核销日期',
`dff`	decimal(20,0)	COMMENT	'回款间隔天数',
`dff_rate`	decimal(20,6)	COMMENT	'回款时间系数',
`order_amt`	decimal(20,6)	COMMENT	'源单据对账金额',
`pay_amt`	decimal(20,6)	COMMENT	'回款核销金额',
`business_type_code`	string	COMMENT	'业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)',
`business_type_name`	string	COMMENT	'业务类型名称',
`status`	string	COMMENT	'是否有效',
`sale_amt`	decimal(20,6)	COMMENT	'含税销售额',
`profit`	decimal(20,6)	COMMENT	'含税定价毛利额',
`sale_amt_jiushui`	decimal(20,6)	COMMENT	'销售额_酒水等',
`profit_jiushui`	decimal(20,6)	COMMENT	'毛利额_酒水等',
`region_code`	string	COMMENT	'大区编码',
`region_name`	string	COMMENT	'大区',
`province_code`	string	COMMENT	'省区编码',
`province_name`	string	COMMENT	'省区',
`city_group_code`	string	COMMENT	'城市编码',
`city_group_name`	string	COMMENT	'城市',
`sales_id`	string	COMMENT	'销售员id',
`work_no`	string	COMMENT	'销售员工号',
`sales_name`	string	COMMENT	'销售员',
`rp_service_user_id`	string	COMMENT	'日配客服经理id',
`rp_service_user_work_no`	string	COMMENT	'日配客服经理工号',
`rp_service_user_name`	string	COMMENT	'日配客服经理',
`fl_service_user_id`	string	COMMENT	'福利客服经理id',
`fl_service_user_work_no`	string	COMMENT	'福利客服经理工号',
`fl_service_user_name`	string	COMMENT	'福利客服经理',
`bbc_service_user_id`	string	COMMENT	'bbc客服经理id',
`bbc_service_user_work_no`	string	COMMENT	'bbc客服经理工号',
`bbc_service_user_name`	string	COMMENT	'bbc客服经理',
`rp_sales_fp_rate`	decimal(20,6)	COMMENT	'日配提成系数-销售员',
`fl_sales_sale_fp_rate`	decimal(20,6)	COMMENT	'福利提成系数-销售员',
`bbc_sales_sale_fp_rate`	decimal(20,6)	COMMENT	'BBC提成系数-销售员',
`rp_service_user_fp_rate`	decimal(20,6)	COMMENT	'日配提成系数-日配客服经理',
`fl_service_user_fp_rate`	decimal(20,6)	COMMENT	'福利提成系数-福利客服经理',
`bbc_service_user_fp_rate`	decimal(20,6)	COMMENT	'BBC提成系数-BBC客服经理',
`update_time`	timestamp	COMMENT    '更新时间',
`smt_ct`	string	COMMENT	'日期分区复制'
) COMMENT '大客户提成-回款订单明细'
PARTITIONED BY (smt string COMMENT '日期分区')
;


--hive 大客户提成-客户结算月回款时间系数明细
drop table if exists csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail;
create table csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail(
`biz_id`	string	COMMENT	'业务主键',
`region_code`	string	COMMENT	'大区编码',
`region_name`	string	COMMENT	'大区',
`province_code`	string	COMMENT	'省区编码',
`province_name`	string	COMMENT	'省区',
`city_group_code`	string	COMMENT	'城市编码',
`city_group_name`	string	COMMENT	'城市',
`customer_code`	string	COMMENT	'客户编码',
`customer_name`	string	COMMENT	'客户名称',
`sales_id`	string	COMMENT	'销售员id',
`work_no`	string	COMMENT	'销售员工号',
`sales_name`	string	COMMENT	'销售员',
`rp_service_user_id`	string	COMMENT	'日配客服经理id',
`rp_service_user_work_no`	string	COMMENT	'日配客服经理工号',
`rp_service_user_name`	string	COMMENT	'日配客服经理',
`fl_service_user_id`	string	COMMENT	'福利客服经理id',
`fl_service_user_work_no`	string	COMMENT	'福利客服经理工号',
`fl_service_user_name`	string	COMMENT	'福利客服经理',
`bbc_service_user_id`	string	COMMENT	'bbc客服经理id',
`bbc_service_user_work_no`	string	COMMENT	'bbc客服经理工号',
`bbc_service_user_name`	string	COMMENT	'bbc客服经理',
`bill_month`	string	COMMENT	'结算月',
`dff_rate`	string	COMMENT	'回款时间系数',
`pay_amt`	decimal(20,6)	COMMENT	'回款金额',
`rp_pay_amt`	decimal(20,6)	COMMENT	'日配回款金额',
`bbc_pay_amt`	decimal(20,6)	COMMENT	'bbc回款金额',
`bbc_ly_pay_amt`	decimal(20,6)	COMMENT	'bbc联营回款金额',
`bbc_zy_pay_amt`	decimal(20,6)	COMMENT	'bbc自营回款金额',
`fl_pay_amt`	decimal(20,6)	COMMENT	'福利回款金额',
`sale_amt`	decimal(20,6)	COMMENT	'销售额',
`rp_sale_amt`	decimal(20,6)	COMMENT	'日配销售额',
`bbc_sale_amt`	decimal(20,6)	COMMENT	'bbc销售额',
`bbc_ly_sale_amt`	decimal(20,6)	COMMENT	'bbc联营销售额',
`bbc_zy_sale_amt`	decimal(20,6)	COMMENT	'bbc自营销售额',
`fl_sale_amt`	decimal(20,6)	COMMENT	'福利销售额',
`profit`	decimal(20,6)	COMMENT	'毛利额',
`rp_profit`	decimal(20,6)	COMMENT	'日配毛利额',
`bbc_profit`	decimal(20,6)	COMMENT	'bbc毛利额',
`bbc_ly_profit`	decimal(20,6)	COMMENT	'bbc联营毛利额',
`bbc_zy_profit`	decimal(20,6)	COMMENT	'bbc自营毛利额',
`fl_profit`	decimal(20,6)	COMMENT	'福利毛利额',
`profit_rate`	decimal(20,6)	COMMENT	'毛利率',
`rp_profit_rate`	decimal(20,6)	COMMENT	'日配毛利率',
`bbc_profit_rate`	decimal(20,6)	COMMENT	'bbc毛利率',
`bbc_ly_profit_rate`	decimal(20,6)	COMMENT	'bbc联营毛利率',
`bbc_zy_profit_rate`	decimal(20,6)	COMMENT	'bbc自营毛利率',
`fl_profit_rate`	decimal(20,6)	COMMENT	'福利毛利率',
`cust_rp_profit_rate_tc`	decimal(20,6)	COMMENT	'日配毛利率提成比例',
`cust_bbc_zy_profit_rate_tc`	decimal(20,6)	COMMENT	'bbc联营毛利率提成比例',
`cust_bbc_ly_profit_rate_tc`	decimal(20,6)	COMMENT	'bbc自营毛利率提成比例',
`cust_fl_profit_rate_tc`	decimal(20,6)	COMMENT	'福利毛利率提成比例',
`rp_sales_fp_rate`	decimal(20,6)	COMMENT	'日配提成系数-销售员',
`fl_sales_sale_fp_rate`	decimal(20,6)	COMMENT	'福利提成系数-销售员',
`bbc_sales_sale_fp_rate`	decimal(20,6)	COMMENT	'BBC提成系数-销售员',
`rp_service_user_fp_rate`	decimal(20,6)	COMMENT	'日配提成系数-日配客服经理',
`fl_service_user_fp_rate`	decimal(20,6)	COMMENT	'福利提成系数-福利客服经理',
`bbc_service_user_fp_rate`	decimal(20,6)	COMMENT	'BBC提成系数-BBC客服经理',
`sales_profit_basic`	decimal(20,6)	COMMENT	'销售员毛利额_基数',
`sales_profit_finish`	decimal(20,6)	COMMENT	'销售员毛利额_达成',
`sales_target_rate`	decimal(20,6)	COMMENT	'销售员毛利额_达成率',
`sales_target_rate_tc`	decimal(20,6)	COMMENT	'销售员毛利额_达成系数',
`rp_service_profit_basic`	decimal(20,6)	COMMENT	'日配客服经理毛利额_基数',
`rp_service_profit_finish`	decimal(20,6)	COMMENT	'日配客服经理毛利额_达成',
`rp_service_target_rate`	decimal(20,6)	COMMENT	'日配客服经理毛利额_达成率',
`rp_service_target_rate_tc`	decimal(20,6)	COMMENT	'日配客服经理毛利额_达成系数',
`fl_service_profit_basic`	decimal(20,6)	COMMENT	'福利客服经理毛利额_基数',
`fl_service_profit_finish`	decimal(20,6)	COMMENT	'福利客服经理毛利额_达成',
`fl_service_target_rate`	decimal(20,6)	COMMENT	'福利客服经理毛利额_达成率',
`fl_service_target_rate_tc`	decimal(20,6)	COMMENT	'福利客服经理毛利额_达成系数',
`bbc_service_profit_basic`	decimal(20,6)	COMMENT	'BBC客服经理毛利额_基数',
`bbc_service_profit_finish`	decimal(20,6)	COMMENT	'BBC客服经理毛利额_达成',
`bbc_service_target_rate`	decimal(20,6)	COMMENT	'BBC客服经理毛利额_达成率',
`bbc_service_target_rate_tc`	decimal(20,6)	COMMENT	'BBC客服经理毛利额_达成系数',
`tc_sales`	decimal(20,6)	COMMENT	'销售员提成',
`tc_rp_service`	decimal(20,6)	COMMENT	'日配客服经理提成',
`tc_fl_service`	decimal(20,6)	COMMENT	'福利客服经理提成',
`tc_bbc_service`	decimal(20,6)	COMMENT	'BBC客服经理提成',
`update_time`	timestamp	COMMENT    '更新时间',
`smt_ct`	string	COMMENT	'日期分区复制'
) COMMENT '大客户提成-客户结算月回款时间系数明细'
PARTITIONED BY (smt string COMMENT '日期分区')
;


--hive 大客户提成-客户人员毛利完成值
drop table if exists csx_analyse.csx_analyse_fr_tc_customer_person_profit_real_mi;
create table csx_analyse.csx_analyse_fr_tc_customer_person_profit_real_mi(
`biz_id`	string	COMMENT	'业务主键',
`smonth`	string	COMMENT	'年月',
`performance_region_code`	string	COMMENT	'客户大区编码',
`performance_region_name`	string	COMMENT	'客户大区名称',
`performance_province_code`	string	COMMENT	'客户省份编码',
`performance_province_name`	string	COMMENT	'客户省份名称',
`performance_city_code`	string	COMMENT	'客户城市组编码',
`performance_city_name`	string	COMMENT	'客户城市组名称',
`customer_code`	string	COMMENT	'客户编码',
`customer_name`	string	COMMENT	'客户名称',
`sales_user_id`	string	COMMENT	'销售员id',
`sales_user_number`	string	COMMENT	'销售员工号',
`sales_user_name`	string	COMMENT	'销售员名称',
`rp_service_user_id`	string	COMMENT	'日配服务管家id',
`rp_service_user_work_no`	string	COMMENT	'日配服务管家工号',
`rp_service_user_name`	string	COMMENT	'日配服务管家名称',
`fl_service_user_id`	string	COMMENT	'福利服务管家id',
`fl_service_user_work_no`	string	COMMENT	'福利服务管家工号',
`fl_service_user_name`	string	COMMENT	'福利服务管家名称',
`bbc_service_user_id`	string	COMMENT	'BBC服务管家id',
`bbc_service_user_work_no`	string	COMMENT	'BBC服务管家工号',
`bbc_service_user_name`	string	COMMENT	'BBC服务管家名称',
`sale_amt`	decimal(20,6)	COMMENT	'销售额',
`rp_sale_amt`	decimal(20,6)	COMMENT	'日配销售额',
`bbc_sale_amt`	decimal(20,6)	COMMENT	'BBC销售额',
`bbc_sale_amt_zy`	decimal(20,6)	COMMENT	'BBC自营销售额',
`bbc_sale_amt_ly`	decimal(20,6)	COMMENT	'BBC联营销售额',
`fl_sale_amt`	decimal(20,6)	COMMENT	'福利销售额',
`profit`	decimal(20,6)	COMMENT	'定价毛利额',
`rp_profit`	decimal(20,6)	COMMENT	'日配定价毛利额',
`bbc_profit`	decimal(20,6)	COMMENT	'BBC定价毛利额',
`bbc_profit_zy`	decimal(20,6)	COMMENT	'BBC自营定价毛利额',
`bbc_profit_ly`	decimal(20,6)	COMMENT	'BBC联营定价毛利额',
`fl_profit`	decimal(20,6)	COMMENT	'福利定价毛利额',
`update_time`	timestamp	COMMENT    '更新时间',
`smt_ct`	string	COMMENT	'日期分区复制'
) COMMENT '大客户提成-客户人员毛利完成值'
PARTITIONED BY (smt string COMMENT '日期分区')
;


-- 确认需对哪些客服经理补充等级比例
select 
	a.performance_province_name,
	b.flag,
	b.user_work_no,
	b.user_name,
	d.s_level,
	d.level_sale_rate,
	d.level_profit_rate,
	sum(sale_amt) sale_amt,
	sum(profit) profit
from
	(
		select 
			performance_province_code,performance_province_name,customer_code,
			substr(sdt,1,6) smonth,sum(sale_amt) sale_amt,sum(profit) profit
		from csx_dws.csx_dws_sale_detail_di
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
			and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and channel_code in('1','7')
		group by performance_province_code,performance_province_name,customer_code,substr(sdt,1,6)
	)a	
left join 
(
	select distinct 
		flag,
		customer_no,
		user_work_no,
		user_name,
		user_id		
	from 
	(
		select 
			'服务管家' flag,
			customer_no,
			rp_service_user_work_no_new as user_work_no,
			rp_service_user_name_new as user_name,
			rp_service_user_id_new as user_id
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and rp_service_user_id_new is not null
		union all
		select 
			'服务管家' flag,
			customer_no,
			fl_service_user_work_no_new as user_work_no,
			fl_service_user_name_new as user_name,
			fl_service_user_id_new as user_id
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and fl_service_user_id_new is not null
		union all	
		select 
			'服务管家' flag,
			customer_no,
			bbc_service_user_work_no_new as user_work_no,
			bbc_service_user_name_new as user_name,
			bbc_service_user_id_new as user_id
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and bbc_service_user_id_new is not null
	)a
) b on b.customer_no=a.customer_code
left join 
(
	select *
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-2),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
	-- where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	-- and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- 不算提成 此类人员需再次确认
	and salary_level<>'不算提成'
)d on b.user_work_no=d.service_user_work_no	
where d.s_level is null
and b.user_name is not null
group by a.performance_province_name,b.flag,b.user_work_no,b.user_name,
d.s_level,d.level_sale_rate,d.level_profit_rate;	



-- 客户销售员管家对应关系 ★★★★先维护好管家等级表 X月客户销售员对照表
select 
a.customer_no,
a.region_name,
a.province_name,
a.city_group_name,
a.channel_name,
a.customer_no,
a.customer_name,
a.sales_id_new,
a.work_no_new,
a.sales_name_new,
a.rp_service_user_id_new,
a.rp_service_user_work_no_new,
a.rp_service_user_name_new,
a.fl_service_user_id_new,
a.fl_service_user_work_no_new,
a.fl_service_user_name_new,
a.bbc_service_user_id_new,
a.bbc_service_user_work_no_new,
a.bbc_service_user_name_new,
-- 202405签呈 每月 浙江全部福利BBC业务调整提成比例，销售员按100%，管家0%
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when length(a.rp_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as rp_sales_sale_rate,
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.fl_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as fl_sales_sale_rate,
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.bbc_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as bbc_sales_sale_rate,
b1.level_sale_rate as rp_service_user_fp_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b2.level_sale_rate end as fl_service_user_fp_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b3.level_sale_rate end as bbc_service_user_fp_rate    
from 
(
select *
from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
where sdt='20240630'
)a 
-- 关联管家等级	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b1 on a.rp_service_user_work_no_new=b1.service_user_work_no	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b2 on a.fl_service_user_work_no_new=b2.service_user_work_no
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b3 on a.bbc_service_user_work_no_new=b3.service_user_work_no
;


-- 调整对应人员比例
select 
concat_ws('-',substr(a.sdt,1,6),a.customer_no) as biz_id,
a.customer_id,
a.customer_no,
a.customer_name,
a.channel_code,
a.channel_name,
a.region_code,
a.region_name,
a.province_code,
a.province_name,
a.city_group_code,
a.city_group_name,
a.sales_id_new,
a.work_no_new,
a.sales_name_new,
a.rp_service_user_id_new,
a.rp_service_user_work_no_new,
a.rp_service_user_name_new,
a.fl_service_user_id_new,
a.fl_service_user_work_no_new,
a.fl_service_user_name_new,
a.bbc_service_user_id_new,
a.bbc_service_user_work_no_new,
a.bbc_service_user_name_new,

case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when length(a.rp_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as rp_sales_sale_rate,
null rp_sales_profit_rate,
	
case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.fl_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as fl_sales_sale_rate,
null fl_sales_profit_rate,	

case 
	when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
	when a.province_name='浙江省' and sdt>='20240501' and a.sales_id_new <>'' then 1
	when length(a.bbc_service_user_id_new)<>0 and length(a.sales_id_new)>0 then 0.6
	when length(a.sales_id_new)>0 then 1	
	end as bbc_sales_sale_rate,
null bbc_sales_profit_rate,	

b1.level_sale_rate as rp_service_user_sale_rate,
null rp_service_user_profit_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b2.level_sale_rate end as fl_service_user_sale_rate,
null fl_service_user_profit_rate,
case when a.province_name='浙江省' and sdt>='20240501' then 0 else b3.level_sale_rate end as bbc_service_user_sale_rate,
null bbc_service_user_profit_rate,
substr(a.sdt,1,6) smt_date
from 
(
select *
from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df 
where sdt='20240531'
and customer_no in(
'127553','127543','127592','127728','127729','127737','127739','127745','127751','127756','127698','127750','127755','127743','104885',
'230784','237034','247439','103044','231213','159635','105032','236254','246603','167200','244859','131174','117980','247357','248992',
'128689','182758','117822','130693','122487','248988'
)
)a 
-- 关联管家等级	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b1 on a.rp_service_user_work_no_new=b1.service_user_work_no	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b2 on a.fl_service_user_work_no_new=b2.service_user_work_no
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b3 on a.bbc_service_user_work_no_new=b3.service_user_work_no
order by a.province_name
;







/*

-- 大数据导出
select 
smt as `年月`,
-- channel_name as `渠道名称`,
region_name as `大区名称`,
province_name as `省区名称`,
city_group_name as `城市名称`,
bill_type as `单据类型`,
source_bill_no as `来源单号`,
customer_code as `客户编码`,
customer_name as `客户名称`,
credit_code as `信控号`,
happen_date as `发生时间`,
company_code as `签约公司编码`,
source_sys as `来源系统`,
reconciliation_period as `对账周期`,
bill_date as `结算日`,
overdue_date as `逾期开始日期`,
paid_date as `核销日期`,
cast(order_amt as decimal(26,2)) as `源单据金额`,
cast(unpay_amt as decimal(26,2)) as `历史核销剩余金额`,
cast(pay_amt_old as decimal(26,2)) as `核销金额_原始`,
cast(pay_amt as decimal(26,2)) as `核销金额`,
business_type_name as `业务类型名称`,
cast(sale_amt_jiushui as decimal(26,2))   as `需剔除酒水销售额`
from csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi	
where smt='${EDATE}'
and abs(unpay_amt)<abs(pay_amt_old)
	${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	${if(len(city)==0,"","AND city_group_name in( '"+city+"') ")}

-- 大客户提成-人员毛利目标达成情况
select
smt,
region_name,
province_name,
city_group_name,
user_position,
work_no,
sales_name,
begin_date,
-- begin_less_1year_flag,
sale_amt,
profit,
profit_basic,
profit_target_rate
from csx_analyse.csx_analyse_tc_person_profit_target_rate
where smt='202311'
and (sale_amt is not null or profit_basic is not null);



-- 客户
select 
smt
,region_name
,province_name
,city_group_name
,customer_code
,customer_name
,work_no
,sales_name
,rp_service_user_work_no
,rp_service_user_name
,fl_service_user_work_no
,fl_service_user_name
,bbc_service_user_work_no
,bbc_service_user_name
-- ,bill_month
,happen_month -- 销售月
,bill_date  -- 结算日期
,paid_date  -- 核销日期（打款日期）	
,dff_rate
,pay_amt
,rp_pay_amt
,bbc_pay_amt
,bbc_ly_pay_amt
,bbc_zy_pay_amt
,fl_pay_amt
-- ,sale_amt
-- ,rp_sale_amt
-- ,bbc_sale_amt
-- ,bbc_ly_sale_amt
-- ,bbc_zy_sale_amt
-- ,fl_sale_amt
-- ,profit
-- ,rp_profit
-- ,bbc_profit
-- ,bbc_ly_profit
-- ,bbc_zy_profit
-- ,fl_profit
,profit_rate
,rp_profit_rate
,bbc_profit_rate
,bbc_ly_profit_rate
,bbc_zy_profit_rate
,fl_profit_rate
,cust_rp_profit_rate_tc
,cust_bbc_ly_profit_rate_tc
,cust_bbc_zy_profit_rate_tc
,cust_fl_profit_rate_tc
,rp_sales_fp_rate
,fl_sales_sale_fp_rate
,bbc_sales_sale_fp_rate
,rp_service_user_fp_rate
,fl_service_user_fp_rate
,bbc_service_user_fp_rate
,new_cust_rate
,sales_profit_basic
,sales_profit_finish
,sales_target_rate
,sales_target_rate_tc
,rp_service_profit_basic
,rp_service_profit_finish
,rp_service_target_rate
,rp_service_target_rate_tc
,fl_service_profit_basic
,fl_service_profit_finish
,fl_service_target_rate
,fl_service_target_rate_tc
,bbc_service_profit_basic
,bbc_service_profit_finish
,bbc_service_target_rate
,bbc_service_target_rate_tc
,tc_sales
,tc_rp_service
,tc_fl_service
,tc_bbc_service
-- 本月销售额毛利额
,by_sale_amt
,by_rp_sale_amt
,by_bbc_sale_amt_zy
,by_bbc_sale_amt_ly
,by_fl_sale_amt
,by_profit
,by_rp_profit
,by_bbc_profit_zy
,by_bbc_profit_ly
,by_fl_profit
-- 服务费
,service_falg
,service_fee
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202308';



	
-- 销售员汇总
select 
smt,
region_name,
province_name,
city_group_name,
work_no,
sales_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc,
sum(pay_amt) pay_amt,
sum(rp_pay_amt) rp_pay_amt,
sum(bbc_pay_amt) bbc_pay_amt,
sum(bbc_ly_pay_amt) bbc_ly_pay_amt,
sum(bbc_zy_pay_amt) bbc_zy_pay_amt,
sum(fl_pay_amt) fl_pay_amt,
-- sum(sale_amt) sale_amt,
-- sum(rp_sale_amt) rp_sale_amt,
-- sum(bbc_sale_amt) bbc_sale_amt,
-- sum(bbc_ly_sale_amt) bbc_ly_sale_amt,
-- sum(bbc_zy_sale_amt) bbc_zy_sale_amt,
-- sum(fl_sale_amt) fl_sale_amt,
sum(tc_sales) tc_sales
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202308'
group by 
smt,
region_name,
province_name,
city_group_name,
work_no,
sales_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc;


-- 管家汇总
select 
smt,
region_name,
province_name,
city_group_name,
service_user_work_no,
service_user_name,
service_profit_basic,
service_profit_finish,
service_target_rate,
service_target_rate_tc,
sum(pay_amt) pay_amt,
-- sum(sale_amt) sale_amt,
sum(tc_service) tc_service
from 
(
select 
smt,
region_name,
province_name,
city_group_name,
rp_service_user_work_no as service_user_work_no,
rp_service_user_name as service_user_name,
rp_service_profit_basic as service_profit_basic,
rp_service_profit_finish as service_profit_finish,
rp_service_target_rate as service_target_rate,
rp_service_target_rate_tc as service_target_rate_tc,
rp_pay_amt as pay_amt,
rp_sale_amt as sale_amt,
tc_rp_service as tc_service
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202308'
and rp_service_user_work_no<>''
union all
select 
smt,
region_name,
province_name,
city_group_name,
fl_service_user_work_no as service_user_work_no,
fl_service_user_name as service_user_name,
fl_service_profit_basic as service_profit_basic,
fl_service_profit_finish as service_profit_finish,
fl_service_target_rate as service_target_rate,
fl_service_target_rate_tc as service_target_rate_tc,
fl_pay_amt as pay_amt,
fl_sale_amt as sale_amt,
tc_fl_service as tc_service
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202308'
and fl_service_user_work_no<>''

union all
select 
smt,
region_name,
province_name,
city_group_name,
bbc_service_user_work_no as service_user_work_no,
bbc_service_user_name as service_user_name,
bbc_service_profit_basic as service_profit_basic,
bbc_service_profit_finish as service_profit_finish,
bbc_service_target_rate as service_target_rate,
bbc_service_target_rate_tc as service_target_rate_tc,
bbc_pay_amt as pay_amt,
bbc_sale_amt as sale_amt,
tc_bbc_service as tc_service
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202308'
and bbc_service_user_work_no<>''
)a
group by 
smt,
region_name,
province_name,
city_group_name,
service_user_work_no,
service_user_name,
service_profit_basic,
service_profit_finish,
service_target_rate,
service_target_rate_tc;

一：计算范围：
1、业务类型：日配、福利、BBC，因回款核销单中无业务类型，关联销售单后取销售单中的业务类型
2、新方案沿用了旧方案的特殊计算包含（特殊客户计入大客户提成（上海'130733','128865','130078'）、福建仓名称包含”V2DC“的不计算、销售员管家对应关系与等级，其他在新方案中不适用或未处理（如服务费）

二、差异原因：
1、回款核销金额覆盖业务类型引起差异：
旧方案中 回款核销金额为该客户全部回款金额，包含客户下日配福利bbc以外的订单（如省区大宗和批发内购），回款核销金额无需区分业务类型，只用于算从奖金包中拿多少比例提成，有奖金包兜底
新方案中 回款核销金额需要按业务类型分别执行不同计算规则，需区分业务类型，存在与销售单关联不上的情况

2、回款核销金额覆盖月份引起差异：
旧方案中 只显示旧方案中涉及的年月（2022年6月以后），新方案中计算5月核销的历史所有月份回款
3、两个方案本身的原因：各类系数、旧方案
4、新方案结算系统与销售系统关联存在部分订单关联不到：
①主要为BBC 订单结算系统与销售系统都存在订单号带字母的，但是不一定哪边带字母，当前是结算系统BBC包裹单未完全签收显示包裹单，签收完成后显示订单号，销售侧显示订单号，但历史和退单调整单不统一
②回款期初单2020年6月以前的，回款单号非真实，关联不到销售单号（解决：兜底规则，找不到对应月毛利率，统一按日配业务、按第一档0.2%处理的）
③纯现金客户在结算系统无体现（解决：按当月销售算等额回款），返利单在结算系统中一个单号对应销售侧多个订单（解决：已合并处理）






--查询客户-取消 每月扣 1=25924与124074合并后日配月累计销售额30万以内返利销售总金额的 3%，超过30万以上部分返利销售总金额的 5%
select 
customer_code,substr(sdt,1,6) as smonth,
performance_region_name,
performance_province_name,
business_type_name,
sum(sale_amt) as sale_amt,
sum(profit) as profit
from csx_dws.csx_dws_sale_detail_di
where substr(sdt,1,6)='202310'
and channel_code in('1','7','9')
and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
and business_type_code in('1')
and customer_code in('125924','124074')
group by 
customer_code,substr(sdt,1,6),
performance_region_name,
performance_province_name,
business_type_name;




-- 客户销售 签呈用
select 
	region_name_customer,
	province_name_customer,
	city_group_name_customer,	
	customer_code,
	customer_name,
	smonth,
	sale_amt,
	rp_sale_amt,
	bbc_sale_amt_zy,
	bbc_sale_amt_ly,
	fl_sale_amt,
	profit,
	rp_profit,
	bbc_profit_zy,
	bbc_profit_ly,
	fl_profit
from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business
where smt='202309';




--结果查询
select update_time,smt
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202312'
limit 10;

select 
	count(1) counts,
	sum(pay_amt) as pay_amt,
	sum(tc_sales) as tc_sales,
	sum(tc_rp_service) as tc_rp_service,		
	sum(tc_fl_service) as tc_fl_service,	
	sum(tc_bbc_service) as tc_bbc_service,
	sum(tc_rp_service)+sum(tc_fl_service)+sum(tc_bbc_service) as tc_service
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202310';


-- 查数
select bill_type,source_sys,customer_code,source_bill_no,happen_date,bill_date,paid_date,pay_amt,sale_amt,business_type_name,sale_amt_jiushui
from csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail
where smt='202309'
-- and customer_code='113260';
and customer_code in(
'112803',
'119042',
'124680',
'176063',
'213636',
'225698',
'226337',
'231457',
'232126',
'232816'
);

select close_bill_code,claim_bill_code,close_account_code,
	customer_code,credit_code,company_code,happen_date,sdt,
	bill_amt,pay_amt,paid_time,close_bill_by,write_off_time,write_off_by
-- from csx_dwd.csx_dwd_sss_close_bill_account_record_di
-- 单据核销流水明细月快照表
from csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf
-- 核销日期分区
where smt=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')
and sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
and date_format(happen_date,'yyyy-MM-dd')>='2022-06-01'
and delete_flag ='0'
-- and customer_code='113260';
and customer_code in(
'112803',
'119042',
'124680',
'176063',
'213636',
'225698',
'226337',
'231457',
'232126',
'232816'
);

-- 查客户商品销售明细	
select inventory_dc_code,business_type_name,customer_code,order_code,order_channel_detail_name,goods_code,goods_name,
sdt,cost_price,sale_price,sale_qty,sale_amt,profit,IF(sale_amt=0,0,profit/sale_amt) as aa
from csx_dws.csx_dws_sale_detail_di
where sdt>='20230901'
and sdt<'20231001'
and customer_code='112803'
-- and goods_code='1531959'
and order_code='OM23011500004621'
order by sdt;

-- 核销明细
select close_bill_code,money_back_id,close_account_code,customer_code,customer_name,
bill_amt,pay_amt,happen_date,delete_flag,delete_reason,paid_time,update_by,update_time,credit_code,sdt
from csx_dwd.csx_dwd_sss_close_bill_account_record_di
where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
and date_format(happen_date,'yyyy-MM-dd')>='2022-06-01'
and delete_flag ='0'
-- and customer_code='113260';
and close_bill_code='OM23011500004621';


-- 
select *
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	-- where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
and source_bill_no='2308210608078474'	

-- 客户核销金额-临时表
select customer_code,sum(pay_amt)  pay_amt
from csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale
where  customer_code in ('105849')
group by customer_code;

select *
from csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale -- limit 10
where  customer_code in ('104885')

select *
from csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale -- limit 10
where  customer_code in ('121870')
and source_bill_no in('OC23061600000008','OM23072500002387')

-- 快照数据
select 
close_bill_code,claim_bill_code,close_account_code,customer_code,credit_code,company_code,happen_date,sdt,
bill_amt,pay_amt,paid_time,close_bill_by,write_off_time,write_off_by,smt
from csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf
where smt in('202308','202309')
and date_format(happen_date,'yyyy-MM-dd')>='2022-06-01'
and (smt='202308' or substr(sdt,1,6)=smt)
and delete_flag ='0'
and customer_code in ('121870')
-- and close_bill_code='OM23070500000212'

-- 提成
select *
from csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail
where smt in('202309')
and customer_code in ('121870')

-- 历史核销剩余金额小于本月核销金额的订单与客户数统计
select count(1) aa,
count(case when abs(unpay_amt)<abs(pay_amt_old) then 1 end) bb,
count(distinct case when abs(unpay_amt)<abs(pay_amt_old) then customer_code end) cc
from csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi
where smt='202309'
-- and abs(unpay_amt)>abs(pay_amt_old)

-- 历史核销剩余金额小于本月核销金额的订单明细
select source_bill_no,customer_code,customer_name,order_amt,unpay_amt,pay_amt_old,pay_amt
from csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi
where smt='202309'
and abs(unpay_amt)<abs(pay_amt_old)
order by customer_code




select 
	customer_code,substr(sdt,1,6) as smonth,
	-- 各类型销售额
	sum(sale_amt) as sale_amt,
	sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
	-- sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
	sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then sale_amt else 0 end) as bbc_sale_amt_zy,
	sum(case when business_type_code in('6') and operation_mode_code=1 then sale_amt else 0 end) as bbc_sale_amt_ly,
	sum(case when business_type_code in('2') then sale_amt else 0 end) as fl_sale_amt,
	-- 各类型定价毛利额
	sum(profit) as profit, 
	sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
	-- sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
	sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then profit else 0 end) as bbc_profit_zy,
	sum(case when business_type_code in('6') and operation_mode_code=1 then profit else 0 end) as bbc_profit_ly,		
	sum(case when business_type_code in('2') then profit else 0 end) as fl_profit
from csx_dws.csx_dws_sale_detail_di
where substr(sdt,1,6)='202404'
	and channel_code in('1','7','9')
	and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
	and business_type_code in('1','2','6')
	and customer_code in('243831','243828','113260')
group by customer_code,substr(sdt,1,6)
;


-- 签呈处理类别
select *
from 
(
select *,
row_number() over(partition by category_second order by smt_date desc) as rno
from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf
where smt='202404'
)a
where rno=1