
-- 项目供应商-客户
-- drop table csx_analyse_tmp.csx_analyse_tmp_xm_cust_sale;
-- create table csx_analyse_tmp.csx_analyse_tmp_xm_cust_sale
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_xm_customer_sale_mi partition(smt)
select
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.customer_code) biz_id,
	substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''),1,6) as smonth,
	d.performance_region_code,
	d.performance_region_name,
	d.performance_province_code,
	d.performance_province_name,
	d.performance_city_code,
	d.performance_city_name,
	a.customer_code,
	d.customer_name,
	d.sales_user_number,
	d.sales_user_name,
	-- substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''),1,6) as smonth,
	b.xm_sale_amt,
	-- 202403签呈-每月 229358 签呈申请毛利率为6%计算
	if(a.customer_code='229358' and b.xm_sale_amt<>0,b.xm_sale_amt*0.06,b.xm_profit) as xm_profit,
	b.xm_sale_amt_th,
	if(a.customer_code='229358' and b.xm_sale_amt<>0,0.06,b.xm_profit/abs(b.xm_sale_amt)) as xm_profit_rate,
	-b.xm_sale_amt_th/b.xm_sale_amt as xm_sale_amt_th_rate,
	
	b.xm_sale_amt_1,
	if(a.customer_code='229358' and b.xm_sale_amt_1<>0,b.xm_sale_amt_1*0.06,b.xm_profit_1) as xm_profit_1,
	b.xm_sale_amt_th_1,		
	if(a.customer_code='229358' and b.xm_sale_amt_1<>0,0.06,b.xm_profit_1/abs(b.xm_sale_amt_1)) as xm_profit_rate_1,
	-b.xm_sale_amt_th_1/b.xm_sale_amt_1 as xm_sale_amt_th_rate_1,
	
	b.xm_sale_amt_2,
	if(a.customer_code='229358' and b.xm_sale_amt_2<>0,b.xm_sale_amt_2*0.06,b.xm_profit_2) as xm_profit_2,
	b.xm_sale_amt_th_2,
	if(a.customer_code='229358' and b.xm_sale_amt_2<>0,0.06,b.xm_profit_2/abs(b.xm_sale_amt_2)) as xm_profit_rate_2,
	-b.xm_sale_amt_th_2/b.xm_sale_amt_2 as xm_sale_amt_th_rate_2,	
	
	c.receivable_amount_target,		-- 回款目标:取1号预测回款金额
	c.current_receivable_amount,		-- 当期回款金额	
	if(current_receivable_amount<0,-current_receivable_amount,0)/receivable_amount_target as back_rate,		-- 回款率
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 	
from
-- 查找项目供应商 取2021年之后的项目供应商
(
	select distinct customer_code,
		business_type_name as sales_channel_name
	from csx_dws.csx_dws_sale_detail_di
	where  sdt>='20210101'
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    and business_type_code='4'
	-- 202204签呈处理 安徽客户已从项目供应商转成自营 当月生效
	and customer_code not in ('114406','111930','107890','108051','108713','109363','109357',
				'109460','123462','117610','111364','111734','111964','116185','113617','112906','114841',
				'112207','115023','113122','113211','113425','113634','113735','115738','114796','114809',
				'114940','115392','115151','115335','115860','116959','116027','116027','116432','117496',
				'121032','117458','117558','118731','117889','117961','117989','123459','118221','118259',
				'118509','118574','118759','118870','119047','119034','119247','119250','119255','119224',
				'119246','119209','119022','119214','119253','119257','119262','119227','119254','119242',
				'121855','120147','120376','120294','121039','120616','120754','122264','123956','121483',
				'121495','121994','122406','122394','122588','122497','122559','122577','124068','124356',
				'124387','124416','124401','124527','121276','114246','117817','121472','121298','121467',
				'119397','121475','120939','120321','120255','120404','120999','121625','121398','122534',
				'122567','122623','122603','123533','122988','124219','123483','123605','124601','125161',
				'123987','124473','124498','125284','125898','105163','114075','119132','121780','122335','122501'
	-- 202403签呈-每月 华北部分客户签呈申请不考核			
				,'120176','117103','116655','115352','119695','118411','125128','130257','130328','130293','116569',
				'116590','117669','119665','120700','121934','124553','127372','127804','122896',
	-- 202404签呈-每月 华北部分客户签呈申请不考核	
				'215440','228441','126979','126984'	
				  ) 
)a
-- 业绩毛利与退货率	
left join	
(
	select 
		customer_code,
		substr(sdt,1,6) as smonth,
		-- case when inventory_dc_name like '%V2DC%' then '2.0模式' else '1.0模式' end as xm_flag,   -- 项目供应商模式
		-- sum(sale_amt) as sale_amt,
		-- sum(profit) as profit,
		sum(case when business_type_code in ('4') then sale_amt else 0 end) as xm_sale_amt,
		sum(case when business_type_code in ('4') then profit else 0 end) as xm_profit,
		sum(case when business_type_code in ('4') and (order_channel_code not in ('4','5','6') and refund_order_flag=1) then sale_amt else 0 end) as xm_sale_amt_th,
		
		-- 202403签呈-202402-202403 部分客户申请按1.0计算 229360
		sum(case when business_type_code in ('4') and (inventory_dc_name not like '%V2DC%') then sale_amt else 0 end) as xm_sale_amt_1,
		sum(case when business_type_code in ('4') and (inventory_dc_name not like '%V2DC%') then profit else 0 end) as xm_profit_1,
		sum(case when business_type_code in ('4') and (inventory_dc_name not like '%V2DC%') and (order_channel_code not in ('4','5','6') and refund_order_flag=1) then sale_amt else 0 end) as xm_sale_amt_th_1,		
		
		sum(case when business_type_code in ('4') and inventory_dc_name like '%V2DC%' then sale_amt else 0 end) as xm_sale_amt_2,
		sum(case when business_type_code in ('4') and inventory_dc_name like '%V2DC%' then profit else 0 end) as xm_profit_2,
		sum(case when business_type_code in ('4') and inventory_dc_name like '%V2DC%' and (order_channel_code not in ('4','5','6') and refund_order_flag=1) then sale_amt else 0 end) as xm_sale_amt_th_2			
	from
	(
	select 
		customer_code,sdt,business_type_code,order_channel_code,refund_order_flag,
		-- 202403签呈-每月 华东江苏南京  WC87仓按1.0处理；华北 WC87仓按1.0处理
		inventory_dc_code,
		case when inventory_dc_code='WC87' then '江苏彩食鲜徐州2131项目供应商DC' else inventory_dc_name end as inventory_dc_name,
		sale_amt,profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and business_type_code='4'
	)a
	group by  
		customer_code,
		substr(sdt,1,6)
)b on a.customer_code=b.customer_code
-- SAP回款目标与回款	
left join	
(
	select 
		channel_name,		-- 客户类型
		-- company_code,		-- 公司代码
		-- company_name,		-- 公司名称
		customer_code,		-- 客户编码
		-- credit_code,		-- 信控编号
		-- credit_business_attribute_code,		-- 信控业务属性编码
		-- credit_business_attribute_name,		-- 信控业务属性名称					
		-- account_period_code,		-- 账期编码
		-- account_period_name,		-- 账期名称
		-- account_period_value,		-- 账期值
		sum(receivable_amount_target) as receivable_amount_target,		-- 回款目标:取1号预测回款金额
		sum(current_receivable_amount) as current_receivable_amount		-- 当期回款金额			
	from csx_analyse.csx_analyse_fr_sap_customer_credit_forecast_collection_report_df  -- 承华  预测回款金额-帆软
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')	
	group by channel_name,customer_code	
)c on a.customer_code=c.customer_code
left join 
	(
	select 
		distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,performance_city_code,performance_city_name
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
	and customer_type_code=4
	)d on d.customer_code=a.customer_code
where b.xm_sale_amt<>0 or c.receivable_amount_target<>0 or c.current_receivable_amount<>0
;



--hive 项目管理部提成
drop table if exists csx_analyse.csx_analyse_fr_tc_xm_customer_sale_mi;
create table csx_analyse.csx_analyse_fr_tc_xm_customer_sale_mi(
`biz_id`	string	COMMENT	'业务主键',
`performance_region_code`	string	COMMENT	'大区编码',
`performance_region_name`	string	COMMENT	'大区名称',
`performance_province_code`	string	COMMENT	'省份编码',
`performance_province_name`	string	COMMENT	'省份名称',
`performance_city_code`	string	COMMENT	'城市组编码',
`performance_city_name`	string	COMMENT	'城市组名称',
`customer_code`	string	COMMENT	'客户编码',
`customer_name`	string	COMMENT	'客户名称',
`sales_user_number`	string	COMMENT	'销售员工号',
`sales_user_name`	string	COMMENT	'销售员',
`xm_sale_amt`	string	COMMENT	'销售额',
`xm_profit`	string	COMMENT	'毛利额',
`xm_sale_amt_th`	string	COMMENT	'退货金额',
`xm_profit_rate`	string	COMMENT	'毛利率',
`xm_sale_amt_th_rate`	string	COMMENT	'退货率',
`xm_sale_amt_1`	string	COMMENT	'销售额_1.0模式',
`xm_profit_1`	string	COMMENT	'毛利额_1.0模式',
`xm_sale_amt_th_1`	string	COMMENT	'退货金额_1.0模式',
`xm_profit_rate_1`	string	COMMENT	'毛利率_1.0模式',
`xm_sale_amt_th_rate_1`	string	COMMENT	'退货率_1.0模式',
`xm_sale_amt_2`	string	COMMENT	'销售额_2.0模式',
`xm_profit_2`	string	COMMENT	'毛利额_2.0模式',
`xm_sale_amt_th_2`	string	COMMENT	'退货金额_2.0模式',
`xm_profit_rate_2`	string	COMMENT	'毛利率_2.0模式',
`xm_sale_amt_th_rate_2`	string	COMMENT	'退货率_2.0模式',
`receivable_amount_target`	string	COMMENT	'回款目标',
`current_receivable_amount`	string	COMMENT	'回款金额',
`back_rate`	string	COMMENT	'回款率',
`update_time`	timestamp	COMMENT    '更新时间',
`smt_ct`	string	COMMENT	'日期分区复制'
) COMMENT '项目管理部提成'
PARTITIONED BY (smt string COMMENT '日期分区')
;





select *
from csx_analyse_tmp.csx_analyse_tmp_xm_cust_sale;




