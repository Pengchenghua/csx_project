select * from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi where smt='202509'
-- 大客户提成旧方案SQL 数据导出 SQL 创建表  csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi 
-- ******************************************************************** 
-- @功能描述：大客户提成旧方案SQL
-- @创建者： 饶艳华 
-- @创建者日期：2023-08-30 17:56:40 
-- @修改者日期：
-- @修改人：
-- @修改内容：更新系数,
-- ******************************************************************** 
create table csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
as
with tmp_customer_info as 
(
select 
	distinct 
	concat_ws('-',substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6),a.customer_no) as biz_id,
	a.customer_id,a.customer_no,a.customer_name,
	a.channel_code,a.channel_name,
	a.region_code,a.region_name,
	a.province_code,a.province_name,
	a.city_group_code,a.city_group_name,
	sales_id_new as sales_id,
	work_no_new as work_no,
	sales_name_new as sales_name,
	rp_service_user_id_new as rp_service_user_id,
	rp_service_user_work_no_new as rp_service_user_work_no,		
	rp_service_user_name_new as rp_service_user_name,

	fl_service_user_id_new as fl_service_user_id,
	fl_service_user_work_no_new as fl_service_user_work_no,
	fl_service_user_name_new as fl_service_user_name,

	bbc_service_user_id_new as bbc_service_user_id,	
	bbc_service_user_work_no_new as bbc_service_user_work_no,
	bbc_service_user_name_new as bbc_service_user_name,	
	
	-- 202405签呈 每月 浙江全部福利BBC业务调整提成比例，销售员按100%，管家0%  province_code='13' 浙江省 region_code='2' 华南大区
	-- 202409签呈 每月 浙江全部福利BBC业务调整提成比例，销售员按100%，管家0%  取消9月重新计算
	-- 华西从12月1号开始生效，无销售员的，管家按照原系数

	case 
		-- when c.income_type in('不算提成','离职') then 0
-- 		when a.province_code='906' and  a.sales_id_new <>'' then 1
	 	when a.region_code='2' and a.province_code !='906' and a.sales_id_new <>'' then 0.6
		when length(rp_service_user_id_new)<>0 and length(sales_id_new)>0 then 0.6
		when length(sales_id_new)>0 then 1
		end as rp_sales_fp_rate,
	case 
		 -- when c.income_type in('不算提成','离职') then 0
-- 		 when a.province_code='906' and  a.work_no_new <>'' then 1
		 when a.region_code='2'  and a.province_code !='906' and a.work_no_new <>'' then 0.6
		 
		 -- when a.province_code='13' and sdt>='20240501' and a.work_no_new <>'' then 1
		 when length(fl_service_user_id_new)<>0 and length(work_no_new)>0 then 0.6
		 when length(work_no_new)>0 then 1
		 end as fl_sales_fp_rate,	
	case 
		 -- when c.income_type in('不算提成','离职') then 0
-- 		 when a.province_code='906' and  a.work_no_new <>'' then 1
		 when a.region_code='2'  and a.province_code !='906' and a.work_no_new <>'' then 0.6
		 
		 -- when a.province_code='13' and sdt>='20240501' and a.work_no_new <>'' then 1
		 when length(bbc_service_user_id_new)<>0 and length(work_no_new)>0 then 0.6
		 when length(work_no_new)>0 then 1
		 end as bbc_sales_fp_rate,
	-- 按照新方案浙江、 华西延长至5月，无销售员按照原方案执行
	-- 安徽(11)、浙江未挂销售的客户包含B,由管家独立维护,该客户分配系数申请按40%进行核算提成。
    
	case when  a.province_code in ('23','24','32') and b1.s_level='高级' then 0.3
	    when  a.province_code in ('23','24','32') and b1.s_level='中级' then 0.2
	    when  a.province_code in ('23','24','32') and b1.s_level='初级' then 0.15
		when a.province_code in ('13','23','24','32') and (a.work_no_new ='' or sales_name_new like '%B') then b1.level_sale_rate*2
		when a.province_code in ('11') and a.work_no_new ='' then 0.4
		else b1.level_sale_rate  end as rp_service_user_fp_rate,
	case 
	-- when a.province_code='13' and sdt>='20240501' then 0 
		when  a.province_code in ('23','24','32') and b2.s_level='高级' then 0.3
	    when  a.province_code in ('23','24','32') and b2.s_level='中级' then 0.2
	    when  a.province_code in ('23','24','32') and b2.s_level='初级' then 0.15
		when a.province_code in ('13','23','24','32') and (a.work_no_new ='' or sales_name_new like '%B') then b2.level_sale_rate*2
		when a.province_code in ('11') and a.work_no_new ='' then 0.4
		else b2.level_sale_rate end as fl_service_user_fp_rate,
	case 
	-- when a.province_code='13' and sdt>='20240501' then 0 
		when  a.province_code in ('23','24','32') and b3.s_level='高级' then 0.3
	    when  a.province_code in ('23','24','32') and b3.s_level='中级' then 0.2
	    when  a.province_code in ('23','24','32') and b3.s_level='初级' then 0.15
		when a.province_code in ('13','23','24','32') and (a.work_no_new ='' or sales_name_new like '%B') then b3.level_sale_rate*2
		when a.province_code in ('11') and a.work_no_new ='' then 0.4
		else b3.level_sale_rate end as bbc_service_user_fp_rate,     	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6) as smt -- 统计日期
from 
(
	select *
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)a
left join
(
	select service_user_work_no,level_sale_rate,s_level
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b1 on a.rp_service_user_work_no_new=b1.service_user_work_no	
left join
(
	select service_user_work_no,level_sale_rate,s_level
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b2 on a.fl_service_user_work_no_new=b2.service_user_work_no
left join
(
	select service_user_work_no,level_sale_rate,s_level
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b3 on a.bbc_service_user_work_no_new=b3.service_user_work_no

left join 
(
	select * 
	-- from csx_analyse.csx_analyse_tc_customer_special_rules_mf 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_first like '%大客户提成-调整对应人员比例%'
) d on d.customer_code=a.customer_no
where d.category_first is null
union all 
select 
	biz_id,
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
	cast(rp_sales_sale_fp_rate as decimal(20,6)) rp_sales_sale_fp_rate,
	cast(fl_sales_sale_fp_rate as decimal(20,6)) fl_sales_sale_fp_rate,
	cast(bbc_sales_sale_fp_rate as decimal(20,6)) bbc_sales_sale_fp_rate,
	cast(rp_service_user_sale_fp_rate as decimal(20,6)) rp_service_user_fp_rate,
	cast(fl_service_user_sale_fp_rate as decimal(20,6)) fl_service_user_fp_rate,
	cast(bbc_service_user_sale_fp_rate as decimal(20,6)) bbc_service_user_fp_rate,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	smt
from csx_analyse.csx_analyse_tc_customer_person_rate_special_rules_mf
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
) 
-- insert overwrite table csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi partition(smt)
select 	biz_id,
	a.customer_id,
	a.customer_no,
	b.customer_name,
	a.channel_code,
	a.channel_name,
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
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
    rp_sales_fp_rate,
    fl_sales_fp_rate,	
    bbc_sales_fp_rate,
	-- 按照新方案浙江、华西无销售员的管家按照旧方案系数	 
	-- 安徽(11)、浙江未挂销售的客户包含B,由管家独立维护,该客户分配系数申请按40%进行核算提成。
    -- 北京延迟一个月按照原方案执行
	rp_service_user_fp_rate,
	fl_service_user_fp_rate,
	bbc_service_user_fp_rate,     	
	update_time,
	smt
from tmp_customer_info a 
left join 
(select customer_code,customer_name from csx_dim.csx_dim_crm_customer_info where sdt='current' ) b on a.customer_no=b.customer_code
;



--调整
-- csx_analyse_tmp.tmp_tc_cust_chunxianjin,csx_analyse_tmp.tmp_tc_cust_order_detail,csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
-- 输出表 csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail,
-- 结算单回款+BBC纯现金客户
drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_01;
create  table csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_01
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



--csx_analyse_fr_tc_customer_credit_order_detail
-- BBC一个订单部分自营部分联营，拆分比例
-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail;
create   table csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail_01
as
-- insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail partition(smt)
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






-- csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2,
-- csx_analyse_tmp.tmp_tc_person_profit_total_split,
-- csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc,
-- csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc,
-- csx_analyse_tmp.tmp_tc_customer_sale_profit_ls,
-- csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1,
-- csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1,
-- csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0,
-- csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info,
-- csx_analyse_tmp.tmp_tc_person_profit_total,
-- csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail

-- 签呈处理：部分订单按实际回款时间系数计算（给预付款标签）
drop table if exists csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2;
create  table csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2
as
select a.*,
	case 
		 -- 按实际回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e.date_star and e.date_end) and e.category_second is not null 
		 and (substr(e.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e.adjust_business_type='全业务') then '是' 
		 -- 按实际回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (substr(f.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') then '是' 

		 -- 指定回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e2.date_star and e2.date_end) and e2.category_second is not null 
		 and (substr(e2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e2.adjust_business_type='全业务') then '是' 
		 -- 指定回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f2.date_star and f2.date_end) and f2.category_second is not null 
		 and (substr(f2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f2.adjust_business_type='全业务') then '是' 		 
		 else '否' end yufu_flag,

	case 
		 -- 指定回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e2.date_star and e2.date_end) and e2.category_second is not null 
		 and (substr(e2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e2.adjust_business_type='全业务') then e2.hk_dff_rate 
		 -- 指定回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f2.date_star and f2.date_end) and f2.category_second is not null 
		 and (substr(f2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f2.adjust_business_type='全业务') then f2.hk_dff_rate 
		 -- 指定客户最高系数
		when ( (f3.adjust_business_type=a.business_type_name or f3.adjust_business_type='全业务' or f3.adjust_business_type=substr(a.business_type_name,1,2) )
		        and f3.adju_flag=1	and dff_rate>=1 ) then f3.hk_dff_rate
		 else dff_rate end dff_rate_new
from
(
	select *
	from csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail_01
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and status=1
)a
-- 部分订单按实际回款时间系数
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-发生日期%'		
	)e on a.customer_code=e.customer_code 
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-打款日期%'		
	)f on a.customer_code=f.customer_code	
-- 部分订单按指定回款时间系数	
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按指定回款时间系数-发生日期%'		
	)e2 on a.customer_code=e2.customer_code 
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按指定回款时间系数-打款日期%'		
	)f2 on a.customer_code=f2.customer_code	
left join 
-- 限制回款最高系数
	(
	select customer_code,
		smt_date as smonth,
		category_first,
		category_second,
		adjust_business_type,
		date_star,
		date_end,
		'1' adju_flag,
		cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整回款最高系数%'		
	)f3 on a.customer_code=f3.customer_code	
;	
	

	

-- 客户+结算月+回款时间系数：各业务类型毛利率提成比例
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc;
create  table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
as
select a.*,

	coalesce(e.rp_rate,
	case when rp_profit_rate<0.08 then 0.002
		when rp_profit_rate>=0.08 and rp_profit_rate<0.12 then 0.005
		when rp_profit_rate>=0.12 and rp_profit_rate<0.16 then 0.007
		when rp_profit_rate>=0.16 and rp_profit_rate<0.2 then 0.009
		when rp_profit_rate>=0.2 and rp_profit_rate<0.25 then 0.013
		when rp_profit_rate>=0.25 then 0.015
		else 0.002 end) as cust_rp_profit_rate_tc,

	coalesce(e.bbc_rate_zy,
	case when bbc_zy_profit_rate<0.08 then 0.002
		when bbc_zy_profit_rate>=0.08 and bbc_zy_profit_rate<0.12 then 0.005
		when bbc_zy_profit_rate>=0.12 and bbc_zy_profit_rate<0.16 then 0.007
		when bbc_zy_profit_rate>=0.16 and bbc_zy_profit_rate<0.2 then 0.009
		when bbc_zy_profit_rate>=0.2 and bbc_zy_profit_rate<0.25 then 0.013
		when bbc_zy_profit_rate>=0.25 then 0.015
		else 0.002 end) as cust_bbc_zy_profit_rate_tc,		
		
	coalesce(e.bbc_rate_ly,
	case when bbc_ly_profit_rate<0.03 then 0.002
		when bbc_ly_profit_rate>=0.03 and bbc_ly_profit_rate<0.07 then 0.0035
		when bbc_ly_profit_rate>=0.07 and bbc_ly_profit_rate<0.1 then 0.0045
		when bbc_ly_profit_rate>=0.1 and bbc_ly_profit_rate<0.13 then 0.0065
		when bbc_ly_profit_rate>=0.13 and bbc_ly_profit_rate<0.17 then 0.0095
		when bbc_ly_profit_rate>=0.17 and bbc_ly_profit_rate<0.23 then 0.013
		when bbc_ly_profit_rate>=0.23 then 0.015
		else 0.002 end) as cust_bbc_ly_profit_rate_tc,

	coalesce(e.fl_rate,
	case when fl_profit_rate<0.03 then 0.002
		when fl_profit_rate>=0.03 and fl_profit_rate<0.07 then 0.0035
		when fl_profit_rate>=0.07 and fl_profit_rate<0.1 then 0.0045
		when fl_profit_rate>=0.1 and fl_profit_rate<0.13 then 0.0065
		when fl_profit_rate>=0.13 and fl_profit_rate<0.17 then 0.0095
		when fl_profit_rate>=0.17 and fl_profit_rate<0.23 then 0.013
		when fl_profit_rate>=0.23 then 0.015
		else 0.002 end) as cust_fl_profit_rate_tc		

from 
(
select a.*,
	-- profit/abs(sale_amt) as profit_rate,
	-- rp_profit/abs(rp_sale_amt) as rp_profit_rate,
	-- bbc_profit/abs(bbc_sale_amt) as bbc_profit_rate,
	-- bbc_ly_profit/abs(bbc_ly_sale_amt) as bbc_ly_profit_rate,
	-- bbc_zy_profit/abs(bbc_zy_sale_amt) as bbc_zy_profit_rate,
	-- fl_profit/abs(fl_sale_amt) as fl_profit_rate
	if(b.sale_amt is not null,b.prorate,b2.prorate) as profit_rate,
	if(b.sale_amt is not null,b.rp_prorate,b2.rp_prorate) as rp_profit_rate,
	if(b.sale_amt is not null,(b.bbc_profit_zy+b.bbc_profit_ly)/abs(b.bbc_sale_amt_zy+b.bbc_sale_amt_ly),
		(b2.bbc_profit_zy+b2.bbc_profit_ly)/abs(b2.bbc_sale_amt_zy+b2.bbc_sale_amt_ly))as bbc_profit_rate,
	if(b.sale_amt is not null,b.bbc_prorate_zy,b2.bbc_prorate_zy) as bbc_zy_profit_rate,
	if(b.sale_amt is not null,b.bbc_prorate_ly,b2.bbc_prorate_ly) as bbc_ly_profit_rate,	
	if(b.sale_amt is not null,b.fl_prorate,b2.fl_prorate) as fl_profit_rate,

	-- 历史月销售额
	if(b.sale_amt is not null,b.sale_amt,b2.sale_amt) as sale_amt_real,
	if(b.sale_amt is not null,b.rp_sale_amt,b2.rp_sale_amt) as rp_sale_amt_real,
	if(b.sale_amt is not null,b.bbc_sale_amt_zy,b2.bbc_sale_amt_zy) as bbc_sale_amt_zy_real,
	if(b.sale_amt is not null,b.bbc_sale_amt_ly,b2.bbc_sale_amt_ly) as bbc_sale_amt_ly_real,
	if(b.sale_amt is not null,b.fl_sale_amt,b2.fl_sale_amt) as fl_sale_amt_real,	
	
	-- 服务费
	-- b.service_falg,
	-- b.service_fee,
	
	b.service_falg as service_falg,
    b.service_fee as service_fee,
	-- 本月销售额毛利额
	c.sale_amt as by_sale_amt,
	c.rp_sale_amt as by_rp_sale_amt,
	c.bbc_sale_amt_zy as by_bbc_sale_amt_zy,
	c.bbc_sale_amt_ly as by_bbc_sale_amt_ly,
	c.fl_sale_amt as by_fl_sale_amt,
	c.profit as by_profit,
	c.rp_profit as by_rp_profit,
	c.bbc_profit_zy as by_bbc_profit_zy,
	c.bbc_profit_ly as by_bbc_profit_ly,
	c.fl_profit as by_fl_profit
	
from 
(
	select
		region_code,
		region_name,
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		customer_code,	-- 客户编码
		customer_name,
		credit_code,	-- 信控号	
		company_code,	-- 签约公司编码
		account_period_code,	-- 账期编码
		account_period_name,	-- 账期名称		
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
		-- 提成分配系数
		rp_sales_fp_rate,
		fl_sales_fp_rate,
		bbc_sales_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate,	
		substr(regexp_replace(bill_date,'-',''),1,6) as bill_month, -- 结算月
		bill_date,  -- 结算日期
		paid_date,  -- 核销日期（打款日期）
		yufu_flag,
		-- '否' as yufu_flag,
		-- if(paid_date<happen_date,'是','否') as yufu_flag,
		substr(regexp_replace(happen_date,'-',''),1,6) as happen_month, -- 销售月		
		-- 202308签呈 126275 将销售日期为6.15-8.15期间的BBC，结算日调整为8.16，且最高回款系数100%
		-- 20241220 签呈新增限制最高回款系数
		case when a.customer_code='126275' and dff_rate_new>1 then 1
			else dff_rate_new end as dff_rate,  -- 回款时间系数
		sum(pay_amt) pay_amt,	-- 核销金额
		sum(case when business_type_code in (1,4,5) then pay_amt else 0 end) as rp_pay_amt,
		sum(case when business_type_name like 'BBC%' then pay_amt else 0 end) as bbc_pay_amt,
		sum(case when business_type_name='BBC联营' then pay_amt else 0 end) as bbc_ly_pay_amt,
		sum(case when business_type_name='BBC自营' then pay_amt else 0 end) as bbc_zy_pay_amt,
		sum(case when business_type_code in(2,10) then pay_amt else 0 end) as fl_pay_amt,
		
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in (1,4,5) then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_name like 'BBC%' then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_name='BBC联营' then sale_amt else 0 end) as bbc_ly_sale_amt,
		sum(case when business_type_name='BBC自营' then sale_amt else 0 end) as bbc_zy_sale_amt,
		sum(case when business_type_code in(2,10) then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit,
		sum(case when business_type_code in (1,4,5) then profit else 0 end) as rp_profit,
		sum(case when business_type_name like 'BBC%' then profit else 0 end) as bbc_profit,
		sum(case when business_type_name='BBC联营' then profit else 0 end) as bbc_ly_profit,
		sum(case when business_type_name='BBC自营' then profit else 0 end) as bbc_zy_profit,
		sum(case when business_type_code in(2,10) then profit else 0 end) as fl_profit	
	from csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2 a 

	group by 	region_code,
		region_name,
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		a.customer_code,	-- 客户编码
		customer_name,
		credit_code,	-- 信控号	
		company_code,	-- 签约公司编码
		account_period_code,	-- 账期编码
		account_period_name,	-- 账期名称			
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
		-- 提成分配系数
		rp_sales_fp_rate,
		fl_sales_fp_rate,
		bbc_sales_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate,
		substr(regexp_replace(bill_date,'-',''),1,6), -- 结算月
		bill_date,	
		paid_date,
		yufu_flag,
		-- if(paid_date<happen_date,'是','否'),
		substr(regexp_replace(happen_date,'-',''),1,6),  -- 销售月
		case when a.customer_code='126275' and dff_rate_new>1 then 1
			else dff_rate_new end   -- 回款时间系数
)a	
left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business b on a.customer_code=b.customer_code and a.happen_month=b.smonth
left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business c on a.customer_code=c.customer_code and c.smonth=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
-- 因某客户可能后期纳入大客户提成计算，无历史处理签呈后的毛利率，若没有历史月的毛利率 则取最新计算的历史月毛利率
left join csx_analyse_tmp.tmp_tc_customer_sale_profit_ls b2 on a.customer_code=b2.customer_code and a.happen_month=b2.smonth

)a	
left join
	(
	select smt_date,customer_code,category_second,
		max(case when adjust_business_type in('日配','全业务') then back_amt_tc_rate end) as rp_rate,
		max(case when adjust_business_type in('BBC','BBC自营','全业务') then back_amt_tc_rate end) as bbc_rate_zy,
		max(case when adjust_business_type in('BBC','BBC联营','全业务') then back_amt_tc_rate end) as bbc_rate_ly,
		max(case when adjust_business_type in('福利','全业务') then back_amt_tc_rate end) as fl_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%调整提成比例%'
	group by smt_date,customer_code,category_second
	) e on e.customer_code=a.customer_code
;	



-- 签呈处理：扣减回款金额
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1;
create  table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1
as
select
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号	
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称		
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
	a.bill_month, -- 结算月
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.yufu_flag,
	a.happen_month, -- 销售月		
	a.dff_rate,  -- 回款时间系数
	a.pay_amt+nvl(b.adjust_amt,0) as pay_amt,	-- 核销金额
	a.rp_pay_amt+nvl(b.rp_adjust_amt,0) as rp_pay_amt,
	a.bbc_pay_amt+nvl(b.bbc_adjust_amt_zy,0)+nvl(b.bbc_adjust_amt_ly,0) as bbc_pay_amt,
	a.bbc_ly_pay_amt+nvl(b.bbc_adjust_amt_ly,0) as bbc_ly_pay_amt,
	a.bbc_zy_pay_amt+nvl(b.bbc_adjust_amt_zy,0) as bbc_zy_pay_amt,
	a.fl_pay_amt+nvl(b.fl_adjust_amt,0) as fl_pay_amt,

	
	-- 各类型销售额
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt,
	a.bbc_ly_sale_amt,
	a.bbc_zy_sale_amt,
	a.fl_sale_amt,
	-- 各类型定价毛利额
	a.profit,
	a.rp_profit,
	a.bbc_profit,
	a.bbc_ly_profit,
	a.bbc_zy_profit,
	a.fl_profit,	

	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_zy_profit_rate,
	a.bbc_ly_profit_rate,	
	a.fl_profit_rate,

	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,
	
	a.cust_rp_profit_rate_tc,
	a.cust_bbc_zy_profit_rate_tc,			
	a.cust_bbc_ly_profit_rate_tc,
	a.cust_fl_profit_rate_tc	
from 
(
	select *,
		-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金 --202406之前：销售月_回款金额
	-- 销售月_回款金额
	concat(happen_month,'_',cast(pay_amt as decimal(26,2))) as happen_month_pay_amt,
	-- 销售月_打款日期_回款金额,当出现销售月与金额重复里，需要调整代码销售月_打款日期_回款金  202406 202406_202406_0000
	concat(happen_month,'_',regexp_replace(paid_date,'-',''),'_',cast(pay_amt as decimal(26,2))) as happen_paid_month_pay_amt
	from csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
)a 
left join
(
  select customer_code,
	smt_date as smonth,
	split(remark,'：')[1] as happen_month_pay_amt,  --扣减回款金额：销售月_打款日期_回款金额
	0-adjust_amount as adjust_amt,
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_adjust_amt, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_adjust_amt_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_adjust_amt_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_adjust_amt
  from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减回款金额%'	
)b on a.customer_code=b.customer_code 
	-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金额
	and  if(a.bill_month<'202406',a.happen_month_pay_amt,happen_paid_month_pay_amt)=b.happen_month_pay_amt 
    -- and  a.happen_month_pay_amt =b.happen_month_pay_amt 
;



DROP TABLE IF EXISTS csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0;

CREATE TABLE csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0
AS
WITH base_data AS (
    SELECT 
        a.*,
        IF(ABS(a.pay_amt) < ABS(a.sale_amt), a.pay_amt, a.sale_amt) AS pay_amt_use,
        d1.profit_basic AS sales_profit_basic,
        d1.profit AS sales_profit_finish,
        d1.profit_target_rate AS sales_target_rate,
        d1.profit_target_rate_tc AS sales_target_rate_tc,
        d2.profit_basic AS rp_service_profit_basic,
        d2.profit AS rp_service_profit_finish,
        d2.profit_target_rate AS rp_service_target_rate,
        d2.profit_target_rate_tc AS rp_service_target_rate_tc,
        d3.profit_basic AS fl_service_profit_basic,
        d3.profit AS fl_service_profit_finish,
        d3.profit_target_rate AS fl_service_target_rate,
        d3.profit_target_rate_tc AS fl_service_target_rate_tc,
        d4.profit_basic AS bbc_service_profit_basic,
        d4.profit AS bbc_service_profit_finish,
        d4.profit_target_rate AS bbc_service_target_rate,
        d4.profit_target_rate_tc AS bbc_service_target_rate_tc,
        CASE 
            WHEN f.category_second <> '' THEN f.adjust_rate 
            WHEN j.renew_flag = '1' AND a.happen_month >= '202504' and happen_month>=j.smt  THEN j.renew_cust_rate
            WHEN e.new_cust_flag = '1' THEN new_cust_rate
            ELSE 1
        END AS new_cust_rate
    FROM csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1 a 
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d1 ON d1.work_no = a.work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d2 ON d2.work_no = a.rp_service_user_work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d3 ON d3.work_no = a.fl_service_user_work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d4 ON d4.work_no = a.bbc_service_user_work_no
    LEFT JOIN (
        SELECT DISTINCT customer_code, 
            if(first_sale_month<'202504' ,1.2,1.0) AS new_cust_rate, 
            '1' new_cust_flag
        FROM (
            SELECT customer_code, substr(MIN(first_business_sale_date), 1, 6) first_sale_month
            FROM csx_dws.csx_dws_crm_customer_business_active_di
            WHERE sdt = 'current' 
              AND business_type_code = '1'
              AND shipper_code = 'YHCSX'
            GROUP BY customer_code
        ) a
        WHERE first_sale_month >= substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -12)), '-', ''), 1, 6)
          AND first_sale_month >= '202308'
    ) e ON a.customer_code = e.customer_code
    LEFT JOIN (
        SELECT smt_date, customer_code, category_second, adjust_rate
        FROM csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
        WHERE smt = substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)), '-', ''), 1, 6)
          AND smt_date = substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)), '-', ''), 1, 6)
          AND category_second LIKE '%调整日配新客系数%'
    ) f ON f.customer_code = a.customer_code
    LEFT JOIN (
    -- 老客续签0.8系数,按照终止日期取最早一条
    select * from (
        SELECT customer_code, 
            0.8 AS renew_cust_rate, 
            '1' AS renew_flag ,
            smt,
            row_number()over(partition by customer_code order by smt asc  ) rn
        FROM csx_analyse.csx_analyse_tc_renew_customer_df
        WHERE smt >= '202504'
          AND htzzrq >= last_day(add_months('${sdt_yes_date}', -1))
          AND row_rn = 1
        GROUP BY customer_code,
            smt
    ) a where rn=1
     ) j ON j.customer_code = a.customer_code
),
calculated_data AS (
    SELECT 
        base.*,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_sales_fp_rate) * new_cust_rate * coalesce(sales_target_rate_tc, 1) AS tc_sales_rp,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_bbc_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_bbc_ly,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_fl,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_service_user_fp_rate) * coalesce(rp_service_target_rate_tc, 1) AS tc_rp_service,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_service_user_fp_rate) * coalesce(fl_service_target_rate_tc, 1) AS tc_fl_service,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service_ly,
        ((bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) +
         (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate)) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_sales_fp_rate) * new_cust_rate AS original_tc_sales_rp,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_sales_fp_rate) AS original_tc_sales_bbc_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_sales_fp_rate) AS original_tc_sales_bbc_ly,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_sales_fp_rate) AS original_tc_sales_fl,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_service_user_fp_rate) AS original_tc_rp_service,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_service_user_fp_rate) AS original_tc_fl_service,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) AS original_tc_bbc_service_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) AS original_tc_bbc_service_ly,
        ((bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) +
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate)) AS original_tc_bbc_service
    FROM base_data base
)
SELECT 
    *,
    (original_tc_sales_rp + original_tc_sales_bbc_zy + original_tc_sales_bbc_ly + original_tc_sales_fl) AS original_tc_sales,
    (tc_sales_rp + tc_sales_bbc_zy + tc_sales_bbc_ly + tc_sales_fl) AS tc_sales
FROM calculated_data
 ;
 



drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1;
create  table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1
as
select 
	-- concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.region_code,a.customer_code,a.bill_month,cast(a.dff_rate as string)) biz_id,
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号	
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称		
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
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	a.pay_amt,	-- 核销金额
	a.rp_pay_amt,
	a.bbc_pay_amt,
	a.bbc_ly_pay_amt,
	a.bbc_zy_pay_amt,
	a.fl_pay_amt,		
	
	-- 各类型销售额
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt,
	a.bbc_ly_sale_amt,
	a.bbc_zy_sale_amt,
	a.fl_sale_amt,
	-- 各类型定价毛利额
	a.profit,
	a.rp_profit,
	a.bbc_profit,
	a.bbc_ly_profit,
	a.bbc_zy_profit,
	a.fl_profit,
	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_ly_profit_rate,
	a.bbc_zy_profit_rate,
	a.fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,
	
	-- 若系统账期为预付货款，则按原回款时间系数
	-- 若是预付款客户，打款日期小于上月1号则，按原回款时间系数但最高100%
	-- 若打款日期小于上月1号则提成为0，若为服务费则=当月回款额/当月销售额*服务费标准*回款系数
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.tc_sales_rp,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_ly,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.tc_sales_fl,0))
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_sales,
	
	-- 取消打款
	-- if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	
	
	-- 增加字段，计算不考虑毛利目标达成情况的提成
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.original_tc_sales_rp,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_ly,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.original_tc_sales_fl,0))
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_sales,
		
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_bbc_service,
	
	
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 		
	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0 a
-- 系统账期预付款客户
 -- 信控公司账期 
left join 
(
	select customer_code,credit_code,company_code,
		account_period_code,account_period_name,		-- 账期编码,账期名称
		account_period_value,		-- 账期值
		account_period_abbreviation_name,		-- 账期简称
		credit_limit,temp_credit_limit
		from csx_dim.csx_dim_crm_customer_company_details
		where sdt='current'
		    and shipper_code='YHCSX'
)e1 on a.credit_code=e1.credit_code and a.company_code=e1.company_code
-- 预付款客户
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('日配','全业务')
	)b1 on b1.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('BBC','全业务')
	)b2 on b2.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('福利','全业务')
	)b3 on b3.customer_code=a.customer_code
-- 调整打款日期：按打款日期对应的实际回款时间系数计算	
left join 
		(
	select customer_code,smt_date as smonth,category_second,adjust_business_type,
	  date_star,date_end,
	  date_format(from_unixtime(unix_timestamp(paid_date_new,'yyyyMMdd')),'yyyy-MM-dd') as paid_date_new 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整打款日期%'		
	)c on a.customer_code=c.customer_code and a.paid_date=c.paid_date_new
left join
	(
	select smt_date,customer_code,
		concat(customer_code,effective_period,remark) as dd,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and (category_second like'%不算提成%'
	-- or category_second like'%服务费%'
	or category_second like'%提成减半%')
	)d on d.customer_code=a.customer_code
;




-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)) biz_id,
	
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
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
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	sum(pay_amt) as pay_amt,	-- 核销金额
	sum(rp_pay_amt) as rp_pay_amt,
	sum(bbc_pay_amt) as bbc_pay_amt,
	sum(bbc_ly_pay_amt) as bbc_ly_pay_amt,
	sum(bbc_zy_pay_amt) as bbc_zy_pay_amt,
	sum(fl_pay_amt) as fl_pay_amt,		
	
	-- 各类型销售额
	sum(sale_amt) as sale_amt,
	sum(rp_sale_amt) as rp_sale_amt,
	sum(bbc_sale_amt) as bbc_sale_amt,
	sum(bbc_ly_sale_amt) as bbc_ly_sale_amt,
	sum(bbc_zy_sale_amt) as bbc_zy_sale_amt,
	sum(fl_sale_amt) as fl_sale_amt,
	-- 各类型定价毛利额
	sum(profit) as profit,
	sum(rp_profit) as rp_profit,
	sum(bbc_profit) as bbc_profit,
	sum(bbc_ly_profit) as bbc_ly_profit,
	sum(bbc_zy_profit) as bbc_zy_profit,
	sum(fl_profit) as fl_profit,
	
	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,
	
	sum(tc_sales) as tc_sales,
	sum(tc_rp_service) as tc_rp_service,		
	sum(tc_fl_service) as tc_fl_service,	
	sum(tc_bbc_service) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	

	-- 增加字段，计算不考虑毛利目标达成情况的提成
	sum(original_tc_sales) as original_tc_sales,
	sum(original_tc_rp_service) as original_tc_rp_service,		
	sum(original_tc_fl_service) as original_tc_fl_service,	
	sum(original_tc_bbc_service) as original_tc_bbc_service,	
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 	
	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1 a
group by 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)),
	
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
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
	a.bill_month,

	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	cast(a.dff_rate as decimal(20,6)),
	coalesce(a.cust_rp_profit_rate_tc,0.002), 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,	
	
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit;	
	