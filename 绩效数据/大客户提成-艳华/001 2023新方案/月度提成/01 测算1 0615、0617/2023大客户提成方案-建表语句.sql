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
,bill_month
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
,cust_bbc_zy_profit_rate_tc
,cust_bbc_ly_profit_rate_tc
,cust_fl_profit_rate_tc
,rp_sales_fp_rate
,fl_sales_sale_fp_rate
,bbc_sales_sale_fp_rate
,rp_service_user_fp_rate
,fl_service_user_fp_rate
,bbc_service_user_fp_rate
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
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt='202305';

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
where smt='202305'
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
where smt='202305'
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
where smt='202305'
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
where smt='202305'
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
