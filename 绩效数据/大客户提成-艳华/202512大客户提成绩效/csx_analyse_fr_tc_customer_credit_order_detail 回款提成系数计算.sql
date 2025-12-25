-- ******************************************************************** 
-- @功能描述：csx_analyse_fr_tc_customer_credit_order_detail 提成系数计算
-- @创建者： 饶艳华 
-- @创建者日期：2023-06-16 21:53:29 
-- @修改者日期：
-- @修改人：
-- @修改内容：20241023 调整北京 央视、301医院调整回款时间系数，回款时间60-90天（含）按照100%，90-120天（含）按照80%，以此类推。
-- 2、增加‘调整回款时间系数：按照销售月_结算日_打款日% 作为唯一键计算系数
-- ******************************************************************** 



set hive.tez.container.size=8192;

-- 纯现金客户标记 月BBC销售金额大于0，月授信支付金额等于0
drop table if exists csx_analyse_tmp.tmp_tc_cust_chunxianjin;
create  table csx_analyse_tmp.tmp_tc_cust_chunxianjin
as
select a.customer_code,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) smonth,
	a.sale_amt,
	a.profit,
	if(a.bbc_sale_amt>0 and a.sale_amt=a.bbc_sale_amt and coalesce(a.credit_settle_amount,0)=0,'是','否') is_chunxianjin
from 
(
	select a.customer_code,a.smonth,
		sum(a.sale_amt) as sale_amt,
		sum(a.profit) as profit,
		sum(a.bbc_sale_amt) as bbc_sale_amt,
		sum(b.credit_settle_amount) as credit_settle_amount -- 授信结算金额
	from
	(
		select 
			business_type_name,performance_province_name,customer_code,customer_name,original_order_code,substr(sdt,1,6) as smonth,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(case when business_type_code in('1','4','5','6') then sale_amt else 0 end) as rp_bbc_sale_amt,
			sum(case when business_type_code in(6) then sale_amt else 0 end) as bbc_sale_amt,
			sum(case when business_type_code in(2,10) then sale_amt else 0 end) as fl_sale_amt
		from csx_dws.csx_dws_sale_detail_di
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
		and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')
		and shipper_code='YHCSX'
	    and order_channel_detail_code not in ('24','28')   -- 剔除永辉生活、永辉线上
		group by business_type_name,performance_province_name,customer_code,customer_name,original_order_code,substr(sdt,1,6)
	)a 
	left join 
	(
		select
		order_code,
		sum(credit_settle_amt) credit_settle_amount
		from
		(
		select distinct
			order_code,
			bill_code,
			credit_settle_amt
		from csx_dwd.csx_dwd_bbc_wshop_bill_order_detail_di
		where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-5),'-','')
		  --  and shipper_code='YHCSX'
		) tmp group by order_code	
	)b on b.order_code=a.original_order_code
	group by a.customer_code,a.smonth
)a
where a.bbc_sale_amt>0 
and a.sale_amt=a.bbc_sale_amt 
and coalesce(a.credit_settle_amount,0)=0;


-- 核销订单+纯现金客户本月销售明细
drop table if exists csx_analyse_tmp.tmp_tc_cust_order_detail;
create  table csx_analyse_tmp.tmp_tc_cust_order_detail
as
select 
	bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	sdt,
	source_bill_no,	-- 来源单号
	customer_code,	-- 客户编码
	credit_code,	-- 信控号
	happen_date,	-- 发生时间		
	company_code,	-- 签约公司编码
	source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	reconciliation_period,  -- 对账周期
	date_format(bill_date, 'yyyy-MM-dd') bill_date, -- 结算日期
	overdue_date,	-- 逾期开始日期	
	paid_date,	-- 核销日期	
	order_amt,	-- 源单据对账金额
	unpay_amt,
	pay_amt,	-- 核销金额
	cast(business_type_code as decimal(2,1)) business_type_code,
	business_type_name,
	cast(status as int) status,  -- 是否有效 0.无效 1.有效
	sale_amt,
	profit,
	sale_amt_jiushui,
	profit_jiushui
from csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi
where smt=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')

-- 纯现金
union all
select 
	null as bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	b.sdt,
	b.order_code as source_bill_no,	-- 来源单号
	b.customer_code,	-- 客户编码
	b.credit_code,	-- 信控号
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') happen_date,	-- 发生时间		
	b.company_code,	-- 签约公司编码
	'纯现金' as source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	null reconciliation_period,  -- 对账周期
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') bill_date, -- 结算日期
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') overdue_date,	-- 逾期开始日期	
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') paid_date,	-- 核销日期	
	b.sale_amt order_amt,	-- 源单据对账金额
	null as unpay_amt,
	b.sale_amt pay_amt,	-- 核销金额
	b.business_type_code,
	b.business_type_name,
	b.status,  -- 是否有效 0.无效 1.有效
	b.sale_amt,
	b.profit,
	b.sale_amt_jiushui,
	b.profit_jiushui
from csx_analyse_tmp.tmp_tc_cust_chunxianjin a 
join
(
	select 
		company_code,
		case when business_type_code in(6) and operation_mode_code=1 then 6.1
			when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
			end business_type_code,
		
		case when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
			when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
			end business_type_name,			

		performance_province_name,customer_code,credit_code,order_code,sdt,
		if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
		-- sum(sale_amt) as sale_amt,
		-- sum(profit) as profit,
		sum(case when goods_code not in ('8718','8708','8649','840509') then sale_amt end) as sale_amt,
		sum(case when goods_code not in ('8718','8708','8649','840509') then profit end) as profit,
		sum(case when goods_code in ('8718','8708','8649','840509') then sale_amt end) as sale_amt_jiushui,
		sum(case when goods_code in ('8718','8708','8649','840509') then profit end) as profit_jiushui
	from csx_dws.csx_dws_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and channel_code in('1','7','9')	
	and order_channel_detail_code not in ('24','28')        -- 剔除永辉生活、永辉线上

	and shipper_code='YHCSX'
	group by company_code,
	case when business_type_code in(6) and operation_mode_code=1 then 6.1
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
		end,
	case when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
		end,
	performance_province_name,customer_code,credit_code,order_code,sdt,
	if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)  -- 是否有效 0.无效 1.有效
)b on a.customer_code=b.customer_code;


-- 从这边调整后面创建的表为临时表20250926
-- 结算单回款+BBC纯现金客户
drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu;
create  table csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
as
select
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	-- a.sdt,
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
	b.customer_name,
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	c.account_period_code,	-- 账期编码
	c.account_period_name,	-- 账期名称
	c.account_period_value,	-- 账期值
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	a.paid_date,	-- 核销日期	
	datediff(a.paid_date, a.bill_date) dff,
	case when datediff(a.paid_date, a.bill_date)<=15 then 1.1
		when datediff(a.paid_date, a.bill_date)<=31 then 1
		when datediff(a.paid_date, a.bill_date)<=60 then 0.8
		when datediff(a.paid_date, a.bill_date)<=90 then 0.6
		when datediff(a.paid_date, a.bill_date)<=120 then 0.4
		when datediff(a.paid_date, a.bill_date)<=150 then 0.2
		when datediff(a.paid_date, a.bill_date)>150 then 0.1
	end dff_rate,
	if(a.sale_amt_jiushui/(a.sale_amt_jiushui+a.sale_amt)>0,
		a.order_amt* (a.sale_amt/(a.sale_amt_jiushui+a.sale_amt)),a.order_amt) order_amt,	-- 源单据对账金额
	if(a.sale_amt_jiushui/(a.sale_amt_jiushui+a.sale_amt)>0,
		a.unpay_amt* (a.sale_amt/(a.sale_amt_jiushui+a.sale_amt)),a.unpay_amt) unpay_amt,	-- 历史核销剩余金额
	if(a.sale_amt_jiushui/(a.sale_amt_jiushui+a.sale_amt)>0,
		a.pay_amt* (a.sale_amt/(a.sale_amt_jiushui+a.sale_amt)),a.pay_amt) pay_amt,	-- 核销金额
	a.business_type_code,
	a.business_type_name,
	a.status,  -- 是否有效 0.无效 1.有效
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui,
	b.region_code,b.region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
	b.sales_id,
	b.work_no,
	b.sales_name,
	b.rp_service_user_id,
	b.rp_service_user_work_no,
	b.rp_service_user_name,
	b.fl_service_user_id,
	b.fl_service_user_work_no,
	b.fl_service_user_name,		
	b.bbc_service_user_id,
	b.bbc_service_user_work_no,
	b.bbc_service_user_name,
	-- 提成分配系数
	b.rp_sales_fp_rate,
	b.fl_sales_fp_rate,
	b.bbc_sales_fp_rate,
	b.rp_service_user_fp_rate,
	b.fl_service_user_fp_rate,
	b.bbc_service_user_fp_rate	
from csx_analyse_tmp.tmp_tc_cust_order_detail a
-- 客户信息与提成系数
left join
(
	select
		region_code,region_name,province_code,province_name,city_group_code,city_group_name,
		regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') as sdt,customer_no,customer_name,
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
		-- 提成系数按
		rp_sales_fp_rate,
		fl_sales_fp_rate,
		bbc_sales_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate
		-- if(rp_sales_sale_fp_rate=0.7,0.6,if(rp_sales_sale_fp_rate=0.3,0.4,if(rp_sales_sale_fp_rate=0.2,0.3,if(rp_sales_sale_fp_rate=0.1,0.2,rp_sales_sale_fp_rate)))) as rp_sales_fp_rate,
		-- if(fl_sales_fp_rate=0.7,0.6,if(fl_sales_fp_rate=0.3,0.4,if(fl_sales_fp_rate=0.2,0.3,if(fl_sales_fp_rate=0.1,0.2,fl_sales_fp_rate)))) as fl_sales_fp_rate,
		-- if(bbc_sales_fp_rate=0.7,0.6,if(bbc_sales_fp_rate=0.3,0.4,if(bbc_sales_fp_rate=0.2,0.3,if(bbc_sales_fp_rate=0.1,0.2,bbc_sales_fp_rate)))) as bbc_sales_fp_rate,
		-- if(rp_service_user_sale_fp_rate=0.7,0.6,if(rp_service_user_sale_fp_rate=0.3,0.4,if(rp_service_user_sale_fp_rate=0.2,0.3,if(rp_service_user_sale_fp_rate=0.1,0.2,rp_service_user_sale_fp_rate)))) as rp_service_user_fp_rate,
		-- if(fl_service_user_sale_fp_rate=0.7,0.6,if(fl_service_user_sale_fp_rate=0.3,0.4,if(fl_service_user_sale_fp_rate=0.2,0.3,if(fl_service_user_sale_fp_rate=0.1,0.2,fl_service_user_sale_fp_rate)))) as fl_service_user_fp_rate,
		-- if(bbc_service_user_sale_fp_rate=0.7,0.6,if(bbc_service_user_sale_fp_rate=0.3,0.4,if(bbc_service_user_sale_fp_rate=0.2,0.3,if(bbc_service_user_sale_fp_rate=0.1,0.2,bbc_service_user_sale_fp_rate)))) as bbc_service_user_fp_rate	
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
)b on b.customer_no=a.customer_code
-- 客户信控的账期
left join
     (
         select customer_code             customer_no,
                customer_name,
                company_code,
                company_name,
                credit_code,
                performance_city_code     city_group_code,
                performance_city_name     city_group_name,
                performance_province_code province_code,
                performance_province_name province_name,
                performance_region_code   region_code,
                performance_region_name   region_name,
                channel_code,                            --  渠道编码
                channel_name,                            --  渠道名称
                sales_user_id             sales_id,      --  销售员id
                sales_user_number         work_no,       --  销售员工号
                sales_user_name           sales_name,    --  销售员名称
                account_period_code, --  账期类型
                account_period_name,  --  账期名称
                account_period_value,  --  帐期天数
                credit_limit,                            --  信控额度
                temp_credit_limit,                       --  临时额度
                temp_begin_time,                         --  临时额度起始时间
                temp_end_time,                            --  临时额度截止时间
                business_attribute_code,                 -- 信控业务属性编码
                business_attribute_name                  -- 信控业务属性名称
         from csx_dim.csx_dim_crm_customer_company_details
         where sdt = 'current' and status = 1
            and shipper_code='YHCSX'
     ) c on a.customer_code = c.customer_no and a.company_code = c.company_code and a.credit_code = c.credit_code
left join   -- CRM客户信息取月最后一天
	(
		select 
			customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			-- 202302签呈 上海 130733 每月纳入大客户提成计算 仅管家拿提成
			case when channel_code='9' and customer_code not in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260') then '业务代理' end as ywdl_cust,				
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and shipper_code='YHCSX'
		and channel_code in('1','7','9')
		and ((customer_type_code=4
		and customer_name not like '%内%购%'
		and customer_name not like '%临保%'
		and channel_code<>'9') 
		or (customer_code in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260')
		or customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and category_second like '%纳入大客户提成计算%'
			)))		
	)d on d.customer_code=a.customer_code
where d.customer_code is not null
;




-- BBC一个订单部分自营部分联营，拆分比例
-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail partition(smt)
select
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),
		a.source_bill_no,a.paid_date,a.customer_code) biz_id,
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称
	a.account_period_value,	-- 账期值
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	-- a.bill_date, -- 结算日期
	
	-- 调整结算日 暂时在代码中调整
	-- 126275 将销售日期为6.15-8.15期间的BBC，结算日调整为8.16，且最高回款系数100%
	case when a.customer_code='126275' and a.business_type_name like 'BBC%' 
	and a.happen_date >='2023-06-15' and a.happen_date <='2023-08-15' then '2023-08-16'
	else a.bill_date end as bill_date, -- 结算日期
	
	a.overdue_date,	-- 逾期开始日期	
	-- a.paid_date,	-- 核销日期	
	case when (regexp_replace(substr(a.happen_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (substr(f.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') 
		 then trunc(add_months('${sdt_yes_date}',-1),"MM")
	else a.paid_date end paid_date,	
	a.dff,
	-- a.dff_rate,
	case when (regexp_replace(substr(a.happen_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (substr(f.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') then f.hk_dff_rate 
		 when f1.category_second is not null 
		 	and (substr(f1.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f1.adjust_business_type='全业务')  then f1.hk_dff_rate
		 when g.customer_code is not null then if(a.dff_rate>1,1,a.dff_rate)
		 -- 回款金额负数，回款时间系数为110%时按100%算
		 when a.pay_amt<0 and a.dff_rate=1.1 then 1	
		 -- 调整北京 央视、301医院调整回款时间系数，回款时间60-90天（含）按照100%，90-120天（含）按照80%，以此类推。
		 when a.customer_code in ('252183','252191','252193','252181','250767','151497','252182','252185','252186','252189','252195','106287') and a.dff <= 90 then 1
		 when a.customer_code in ('252183','252191','252193','252181','250767','151497','252182','252185','252186','252189','252195','106287') and a.dff between 91 and 120 then 0.8
		 when a.customer_code in ('252183','252191','252193','252181','250767','151497','252182','252185','252186','252189','252195','106287') and a.dff between 121 and 150 then 0.6
		 when a.customer_code in ('252183','252191','252193','252181','250767','151497','252182','252185','252186','252189','252195','106287') and a.dff >= 151  then 0.4

	else a.dff_rate end dff_rate,
	-- a.order_amt,	-- 源单据对账金额
	-- a.pay_amt,	-- 核销金额
	case when b.sale_amt is not null and a.business_type_name='BBC联营' then a.order_amt*b.sale_amt_bbc_ly_rate 
		 when b.sale_amt is not null and a.business_type_name='BBC自营' then a.order_amt*b.sale_amt_bbc_zy_rate 
	else a.order_amt end order_amt,	-- 源单据对账金额
	
	case when b.sale_amt is not null and a.business_type_name='BBC联营' then a.pay_amt*b.sale_amt_bbc_ly_rate 
		 when b.sale_amt is not null and a.business_type_name='BBC自营' then a.pay_amt*b.sale_amt_bbc_zy_rate 
	else a.pay_amt end pay_amt,	-- 核销金额	
	
	a.business_type_code,
	a.business_type_name,
	a.status,  -- 是否有效 0.无效 1.有效
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui,
	a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	

	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	-- a.unpay_amt,	-- 历史核销剩余金额
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu a
left join 
(
select source_bill_no,
sum(sale_amt) sale_amt,
sum(case when business_type_name='BBC联营' then sale_amt end) sale_amt_bbc_ly,
sum(case when business_type_name='BBC自营' then sale_amt end) sale_amt_bbc_zy,
sum(case when business_type_name='BBC联营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_ly_rate,
sum(case when business_type_name='BBC自营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_zy_rate
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
where business_type_name like 'BBC%' 
group by source_bill_no
)b on a.source_bill_no=b.source_bill_no
left join 
		(
	select customer_code,smt_date as smonth,category_second
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%剔除客户%'		
	)e on a.customer_code=e.customer_code
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_first like '%调整回款时间系数%'		
	)f on a.customer_code=f.customer_code
-- 当出现重复的发生月、结算、打款日时，使用以下来关联系数
left join 
		(
	select customer_code,smt_date as smonth,
		category_first,
		category_second,
		adjust_business_type,
		regexp_extract(remark,'([0-9]{6}_[0-9]{8}_[0-9]{8})') happen_bill_paid_date,
		cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整回款时间系数：按照销售月_结算日_打款日%'		
	)f1 on a.customer_code=f1.customer_code 
		and concat_ws('_',substr(regexp_replace(to_date(a.happen_date),'-',''),1,6),regexp_replace(bill_date,'-',''),regexp_replace(paid_date,'-',''))=f1.happen_bill_paid_date	

-- 直送客户和项目供应商客户回款系数调整：110%调整为100% 安徽签呈，其他省区相同处理	
left join 
		(
	select distinct customer_code,smt_date as smonth
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and (remark like '%直送客户%' or remark like '%项目供应商客户%' or  remark like '%前置仓客户%')		
	)g on a.customer_code=g.customer_code	
where (a.sale_amt is not null 
or a.source_sys='BEGIN')
and e.category_second is null;	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初

