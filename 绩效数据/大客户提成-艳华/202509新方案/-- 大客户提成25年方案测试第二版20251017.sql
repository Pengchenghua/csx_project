-- 大客户提成25年方案测试第二版20251017
-- ******************************************************************** 
-- @功能描述：
-- @创建者： 饶艳华 
-- @创建者日期：2025-09-30 17:56:40 
-- @修改者日期：
-- @修改人：
-- @修改内容：更新系数,新方案测试
-- ******************************************************************** 

-- 计算管家提成分配系数
-- drop table  csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi 
create table  csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi as  
with 
tmp_position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
tmp_sales_info as (
  select a.*,b.name as user_position_name,c.name as leader_position_name from 
  (select
    a.user_id,
    a.user_number,
    a.user_name,
    a.source_user_position,
    a.leader_user_id,
    b.user_number as leader_user_number,
    b.user_name as leader_user_name,
    b.source_user_position as leader_user_position
  from
       csx_dim.csx_dim_uc_user a
    left join (
      select
        user_id,
        user_number,
        user_name,
        source_user_position
      from
        csx_dim.csx_dim_uc_user a
      where
        sdt = regexp_replace(last_day(add_months('${edate}',-1)),'-','')
        and status = 0
    ) b on a.leader_user_id = b.user_id
  where
    sdt = regexp_replace(last_day(add_months('${edate}',-1)),'-','')
    and status = 0
    )a 
    left join tmp_position_dic b on a.source_user_position=b.code
    left join tmp_position_dic c on a.leader_user_position=c.code
) 
,
tmp_business_sale_info as
(
select a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  rp.business_attribute_user_id as rp_user_id,
  rp.business_attribute_user_number as rp_user_number,
  rp.business_attribute_user_name as rp_user_name,
  rp.business_attribute_user_position as rp_user_position,
  fl.business_attribute_user_id as fl_user_id,
  fl.business_attribute_user_number as fl_user_number,
  fl.business_attribute_user_name as fl_user_name,
  fl.business_attribute_user_position as fl_user_position,
  bbc.business_attribute_user_id as bbc_user_id,
  bbc.business_attribute_user_number as bbc_user_number,
  bbc.business_attribute_user_name as bbc_user_name,
  bbc.business_attribute_user_position as bbc_user_position
from (
  select performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
  group by performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name
) a
left join (
  select customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
    and business_attribute_name = '日配'
  group by customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
) rp on a.customer_code = rp.customer_code
left join (
  select customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
    and business_attribute_name = '福利'
  group by customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
) fl on a.customer_code = fl.customer_code
left join (
  select customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
  from csx_dim.csx_dim_crm_customer_business_ownership
  where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    and business_attribute_user_id <> 0
    and business_attribute_name = 'BBC'
  group by customer_code,
    business_attribute_user_id,
    business_attribute_user_name,
    business_attribute_user_number,
    business_attribute_user_position
) bbc on a.customer_code = bbc.customer_code
),
tmp_customer_info as (
  select distinct concat_ws('-',substr(regexp_replace(add_months('${edate}', -1), '-', ''),1, 6  ), a.customer_no ) as biz_id,
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
    sales_id_new as sales_id,
    work_no_new as work_no,
    sales_name_new as sales_name,
    rp_user_id as rp_sales_id,
    rp_user_number as rp_sales_number,
    rp_user_name as rp_sales_name,
    rp_user_position as rp_sales_position,
    fl_user_id as fl_sales_id,
    fl_user_number as fl_sales_number,
    fl_user_name as fl_sales_name,
    fl_user_position as fl_sales_position,
    bbc_user_id as bbc_sales_id,
    bbc_user_number as bbc_sales_number,
    bbc_user_name as bbc_sales_name,
    bbc_user_position as bbc_sales_position,
    rp_service_user_id_new as rp_service_user_id,
    rp_service_user_work_no_new as rp_service_user_work_no,
    rp_service_user_name_new as rp_service_user_name,
    fl_service_user_id_new as fl_service_user_id,
    fl_service_user_work_no_new as fl_service_user_work_no,
    fl_service_user_name_new as fl_service_user_name,
    bbc_service_user_id_new as bbc_service_user_id,
    bbc_service_user_work_no_new as bbc_service_user_work_no,
    bbc_service_user_name_new as bbc_service_user_name,
    -- 销售系数
    0.6 as rp_sales_fp_rate,
    case
      when length(fl_service_user_id_new) <> 0
      and length(work_no_new) > 0 then 0.6
      when length(work_no_new) > 0 then 1
    end as fl_sales_fp_rate,
    case
      when length(bbc_service_user_id_new) <> 0
      and length(work_no_new) > 0 then 0.6
      when length(work_no_new) > 0 then 1
    end as bbc_sales_fp_rate,
    -- 管家系数
    0 as rp_service_user_fp_rate,
    0 as fl_service_user_fp_rate,
    0 as bbc_service_user_fp_rate,
    0 customer_profit_rate,
    0 city_profit_rate,
    from_utc_timestamp(current_timestamp(), 'GMT') update_time,
    substr(regexp_replace(add_months('${edate}', -1), '-', ''), 1,  6) as smt -- 统计日期
  from (
      select a.*
      from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a
      where sdt = regexp_replace(last_day(add_months('${edate}', -1)), '-', '')
    ) a
    left join tmp_business_sale_info b on a.customer_no = b.customer_code
) -- insert overwrite table   csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi partition(smt)
select biz_id,
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
  c.user_position_name,
  rp_sales_id as rp_sales_id,
  rp_sales_number,
  rp_sales_name,
  d.user_position_name as rp_sales_position,
  fl_sales_id,
  fl_sales_number,
  fl_sales_name,
  f.user_position_name as fl_sales_position,
  bbc_sales_id,
  bbc_sales_number,
  bbc_sales_name,
  j.user_position_name as bbc_sales_position,
  rp_service_user_id,
  rp_service_user_work_no,
  rp_service_user_name,
  fl_service_user_id,
  fl_service_user_work_no,
  fl_service_user_name,
  bbc_service_user_id,
  bbc_service_user_work_no,
  bbc_service_user_name,
  case
    when strategy_status = 1
    and coalesce(rp_service_user_work_no, '') = '' then 0.5
    when strategy_status = 1
    and coalesce(rp_service_user_work_no, '') != '' then 0.2
    else rp_sales_fp_rate
  end rp_sales_fp_rate,
  case
    when strategy_status = 1
    and coalesce(fl_service_user_work_no, '') = '' then 0.5
    when strategy_status = 1
    and coalesce(fl_service_user_work_no, '') != '' then 0.2
    when f.user_position_name in('福利销售BD','福利销售经理') then 0 
    else fl_sales_fp_rate
  end fl_sales_fp_rate,
  case
    when strategy_status = 1
    and coalesce(bbc_service_user_work_no, '') = '' then 0.5
    when strategy_status = 1
    and coalesce(bbc_service_user_work_no, '') != '' then 0.2
    when j.user_position_name in('福利销售BD','福利销售经理') then 0 
    else bbc_sales_fp_rate
  end bbc_sales_fp_rate,
  -- 按照新方案浙江、华西无销售员的管家按照旧方案系数	 
  -- 安徽(11)、浙江未挂销售的客户包含B,由管家独立维护,该客户分配系数申请按40%进行核算提成。
  -- 北京延迟一个月按照原方案执行
  rp_service_user_fp_rate,
  fl_service_user_fp_rate,
  bbc_service_user_fp_rate,
  update_time,
  customer_profit_rate,
  city_profit_rate,
  CASE
    WHEN a.city_group_name IN ('北京市','福州市','重庆主城','深圳市','成都市','上海松江','南京主城','合肥市','西安市','石家庄市','江苏苏州','杭州市','郑州市','广东广州') THEN 'A/B'
    WHEN a.city_group_name IN ('厦门市','宁波市','泉州市','莆田市','南平市','南昌市','贵阳市','宜宾','武汉市' ) THEN 'C'
    WHEN a.city_group_name IN ('三明市', '阜阳市', '台州市', '龙岩市', '万州区', '江苏盐城', '黔江区', '永川区') then 'D'
    else 'D'
  END as city_type_name,
  smt
from tmp_customer_info a
  left join (
    select customer_code,
      customer_name,
      strategy_status
    from csx_dim.csx_dim_crm_customer_info
    where sdt = 'current'
  ) b on a.customer_no = b.customer_code
 left join 
 tmp_sales_info c on a.work_no=c.user_number
 left join 
 tmp_sales_info d on a.rp_sales_number=d.user_number
  left join 
 tmp_sales_info f on a.fl_sales_number=f.user_number
   left join 
 tmp_sales_info j  on a.bbc_sales_number=j.user_number
;

select * from csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi
;



-- E:\彩食鲜工作内容\输出文件\绩效数据\大客户提成-艳华\202509新方案\csx_analyse_fr_tc_customer_credit_order_detail 回款提成系数计算.sql
-- 从这边调整后面创建的表为临时表20250926
-- 结算单回款+BBC纯现金客户
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_01;
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
	b.rp_sales_id,
	b.rp_sales_number,
	b.rp_sales_name,
	b.fl_sales_id,
	b.fl_sales_number,
	b.fl_sales_name,
	b.bbc_sales_id,
	b.bbc_sales_number,
	b.bbc_sales_name,
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
		regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') as sdt,
		customer_no,
		customer_name,
		sales_id,
		work_no,
		sales_name,
		rp_sales_id,
		rp_sales_number,
		rp_sales_name,
		fl_sales_id,
		fl_sales_number,
		fl_sales_name,
		bbc_sales_id,
		bbc_sales_number,
		bbc_sales_name,
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
-- 使用LEFT JOIN方式优化SQL逻辑，解决Hive子查询限制问题
left join   -- CRM客户信息取月最后一天
(
    select 
        t1.customer_code,
        t1.customer_name,
        t1.sales_user_number,
        t1.sales_user_name,
        t1.performance_region_code,
        t1.performance_region_name,
        t1.performance_province_code,
        t1.performance_province_name,
        t1.performance_city_code,
        t1.performance_city_name,
        -- 202302签呈 上海 130733 每月纳入大客户提成计算 仅管家拿提成
        case when t1.channel_code='9' 
            and t1.customer_code not in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260') 
        then '业务代理' end as ywdl_cust,				
        case when (t1.customer_name like '%内%购%' or t1.customer_name like '%临保%') then '内购' end as ng_cust
    from csx_dim.csx_dim_crm_customer_info t1
    left join (
        select distinct customer_code
        from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
        where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
            and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
            and category_second like '%纳入大客户提成计算%'
    ) t2 on t1.customer_code = t2.customer_code
    where t1.sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        and t1.shipper_code='YHCSX'
        and t1.channel_code in('1','7','9')
        and (
            -- 主要条件：客户类型为4，且不是内购或临保客户（渠道不为9）
            (t1.customer_type_code=4
                and t1.customer_name not like '%内%购%'
                and t1.customer_name not like '%临保%'
                and t1.channel_code<>'9') 
            or 
            -- 特殊客户列表
            t1.customer_code in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260')
            or 
            -- 符合特殊规则的客户
            t2.customer_code is not null
        )
) d on d.customer_code=a.customer_code
where d.customer_code is not null
;




-- BBC一个订单部分自营部分联营，拆分比例
-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail
-- as
create table csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail as 
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
	a.bill_date, -- 结算日期
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
	a.rp_sales_id,
	a.rp_sales_number,
	a.rp_sales_name,
	a.fl_sales_id,
	a.fl_sales_number,
	a.fl_sales_name,
	a.bbc_sales_id,
	a.bbc_sales_number,
	a.bbc_sales_name,
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
	a.fl_sales_fp_rate fl_sales_sale_fp_rate,
	a.bbc_sales_fp_rate bbc_sales_sale_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	

	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	-- a.unpay_amt,	-- 历史核销剩余金额
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_01 a
left join 
(
select source_bill_no,
sum(sale_amt) sale_amt,
sum(case when business_type_name='BBC联营' then sale_amt end) sale_amt_bbc_ly,
sum(case when business_type_name='BBC自营' then sale_amt end) sale_amt_bbc_zy,
sum(case when business_type_name='BBC联营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_ly_rate,
sum(case when business_type_name='BBC自营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_zy_rate
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_01
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

