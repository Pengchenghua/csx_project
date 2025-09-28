create table csx_analyse.csx_analyse_fr_hr_business_agent_mi (
smonth	string	comment 	'月份',
performance_province_name	string COMMENT '省份名称',
performance_city_name	string	 COMMENT '城市名称',
customer_code	string	 COMMENT '客户编码',
customer_name	string	COMMENT '客户名称',
sign_date	string	comment '签约日期',
sign_company_code	string	comment '公司编码',
sales_user_number	string	comment '代理人工号',
sales_user_name	string	comment '代理人名称',
sale_amt	decimal(38,6)	comment '销售额',
profit	decimal(38,6)	COMMENT '毛利额',
profit_rate	decimal(38,6) COMMENT '毛利率',	
salery_sales	decimal(38,6)	comment '销售额奖金包',
salery_fnl	decimal(38,6)	comment '毛利奖金包',
salery_cal	decimal(38,6)	comment '奖金包*系数',
unpaid_amt_by	decimal(38,6)	COMMENT '本月应收金额', 
unpaid_amt_sy	decimal(38,6)	COMMENT '上月应收金额',
payment_amount_by	decimal(38,6)	COMMENT '本月核销金额',
payment_amount_sy	decimal(38,6)	COMMENT '上月核销金额',
paid_amt	decimal(38,6)	COMMENT '本月回款金额',
residue_amt	decimal(38,6)	COMMENT '本月回款未核销金额',
residue_amt_sy	decimal(38,6)	COMMENT '上月回款未核销金额',
sign_ratio	int	comment '签约系数默认1',
salery_cal_1	decimal(38,6) comment '最终奖金包提成',	
cust_flag	int	comment '客户签呈',
salery_cal_2	decimal(38,6) 	 comment '手动计算最终奖金包提成',
update_time current_timestamp comment '更新时间'
)comment '业务代理人绩效'
partitioned by (smt string	comment  '月度分区')
stored as parquet;


-- ******************************************************************** 
-- @功能描述：业务代理绩效
-- @创建者： 彭承华 
-- @创建者日期：2024-01-24 15:54:28 
-- @修改者日期：
-- @修改人：
-- @修改内容：修改日期参数\按照核销金额计算提成
-- ******************************************************************** 

-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;
-- 关闭平台默认HDFS小文件合并，控制 Tez 引擎在执行 Hive 查询时是否进行文件合并。
SET hive.merge.tezfiles=false;

-- 销售
drop table csx_analyse_tmp.csx_analyse_tmp_hr_business_sale ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_business_sale 
 as 
--  (
 select substr(sdt,1,6) smonth,
	    a.order_code,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_number sales_user_number,
		b.sales_name   sales_user_name ,
		b.sign_date,
		a.sign_company_code,
		flag,
		type_name,
	    sale_rate,
	    profit_rate_ceiling,
	    sale_rate_celing,
	    adjusted_profit_ratio,
		sum(sale_amt )sale_amt,
		sum(profit) profit,
		sum(profit)/sum( sale_amt ) prorate,
		end_date
	from csx_dws.csx_dws_sale_detail_di a
join
-- 关联客户信息
 (select a.*,
    if(b.customer_code is not null ,1,0) as flag,
    coalesce(type_name,1)  type_name,  -- 1 方案执行 2、按销售回款 3、销售回款+毛利率超额部份*销售回款占比sale_rate_celing，4其他特殊处理看备注说明
    coalesce(sale_rate,0)  sale_rate,
    coalesce(profit_rate_ceiling,0) profit_rate_ceiling,
    coalesce(sale_rate_celing,0) sale_rate_celing,
    coalesce(adjusted_profit_ratio,0) adjusted_profit_ratio,
    case when b.user_name is not null and  user_name !=''  then '-' else sales_user_number end  as sales_number,
    case when b.user_name is not null and  user_name !=''  then b.user_name else sales_user_name end  as sales_name,
    coalesce(note,'')note,
    coalesce(b.status,1) as status,
    coalesce(substr(regexp_replace(to_date(b.end_time),'-',''),1,6),'203112') end_date
from 
(select DISTINCT substr(sdt,1,6) smonth,
	customer_code,
	customer_name,
	channel_code,
    business_agent_user_number sales_user_number,   -- 业务代理人工号
    business_agent_user_name sales_user_name,       -- 业务代理人名称
    sign_company_code,
	regexp_replace(to_date(first_sign_time), '-', '') as sign_date
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')  -- 上月最后1日
	    and shipper_code='YHCSX'
)a 
left join
-- 关联签呈客户
csx_ods.csx_ods_data_analysis_prd_write_service_broker_sign_df b on a.customer_code=b.customer_code 
where (channel_code=9 or b.customer_code is not null )
) b on b.customer_code=a.customer_code 
	where ((sdt>='20200901' and sdt<regexp_replace(add_months(trunc('${yesterdate}','MM'),0),'-','') and a.customer_code not in ('121584','125355','118825','108267') )  -- 上上月1日 -- 昨日月1日
-- 	and customer_code not in ('112092')
-- 福建剔除以下客户K4仓 20240223
    or  (( a.customer_code in ('121584','125355','118825') or  a.sub_customer_code in ('Z003980') )
        and  inventory_dc_code not in ('W0K4') and sdt>='20200901' and sdt<regexp_replace(add_months(trunc('${yesterdate}','MM'),0),'-',''))
        )
	and status<>0
	and shipper_code='YHCSX'
	group by substr(sdt,1,6), 
	    a.order_code,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_number,
		b.sales_name,  
		b.sign_date,
		a.sign_company_code,
		flag,
	    type_name,
	    sale_rate,      -- 销售回款提成
	    profit_rate_ceiling,
	    sale_rate_celing,
	    end_date,
	    adjusted_profit_ratio

; 


drop table csx_analyse_tmp.csx_analyse_tmp_hr_business_agent;
create table csx_analyse_tmp.csx_analyse_tmp_hr_business_agent as 
with temp_dali_cust_01 as (		 
select 
    a.smonth,
	a.performance_province_name performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.customer_name cust_name,
	a.sign_date,
	sign_company_code,
	a.sales_user_number,
	a.sales_user_name,
	flag,
	type_name,
	sale_rate,
	profit_rate_ceiling,
	adjusted_profit_ratio,
	sale_rate_celing,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit,
	sum(a.profit)/sum(a.sale_amt) prorate,
	round(sum(a.sale_amt)*0.02,2) salery_sales,
	round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>0 and a.customer_code not in('104179','112092','113260'),(sum(a.profit)/sum(a.sale_amt)-0.15),0)*sum(e.payment_amount_by)*0.5,4) salery_fnl,
	round(sum(a.sale_amt)*0.02,2)+round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>=0 and a.customer_code not in('104179','112092','113260'),(sum(a.profit)/sum(a.sale_amt)-0.15),0)*sum(a.sale_amt)*0.5,2) salery_cal,
	sum(c.unpaid_amount)unpaid_amt_by,
	sum(d.unpaid_amount)unpaid_amt_sy,
	sum(e.payment_amount_by) payment_amount_by,
	sum(e.payment_amount_sy) payment_amount_sy,
	end_date,
	regexp_replace('${yesterdate}','-','') as sdt
from 
	 csx_analyse_tmp.csx_analyse_tmp_hr_business_sale  a
left join 
	(
	 select 
		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	 from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售单对账
	 where sdt=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')
	       and date(happen_date)<=last_day(add_months('${yesterdate}',-1))
	 group by source_bill_no
	)c on c.order_code=a.order_code
left join 
	(
	 select 
		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售单对账
	where sdt=regexp_replace(last_day(add_months('${yesterdate}',-2)),'-','')
	and date(happen_date)<=last_day(add_months('${yesterdate}',-2))
	group by source_bill_no
	)d on d.order_code=a.order_code	
left join 
	(
	-- 核销流水明细表中已核销金额  涉及到公司编码，需要按照单号关联
	select customer_code,
	    regexp_replace(substr(happen_date,1,7),'-','') smonth,
	    close_bill_code,
	    company_code,
	   --  regexp_replace(substr(paid_time,1,10),'-','') PAIT_DATE,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>= regexp_replace(add_months(trunc('${yesterdate}','MM'),-1),'-','') 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')
			 then pay_amt end ) payment_amount_by,    -- 本月核销
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=regexp_replace(add_months(trunc('${yesterdate}','MM'),-2),'-','') 
		  and regexp_replace(substr(paid_time,1,10),'-','')<= regexp_replace(last_day(add_months('${yesterdate}',-2)),'-','')
			 then pay_amt end ) payment_amount_sy,	    -- 上月核销		 
	sum(pay_amt) paid_amount                         -- 总核销
	
	from
       csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf a	
	where  smt=substr(regexp_replace(last_day(add_months('${yesterdate}',-1)),'-',''),1,6)
	and (regexp_replace(substr(happen_date,1,10),'-','')<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','') or happen_date='' or happen_date is NULL)
	and regexp_replace(substr(paid_time,1,10),'-','') <=     regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','') 
	and delete_flag ='0'
	group by customer_code,
	    regexp_replace(substr(happen_date,1,7),'-','')
	    ,company_code,
	    close_bill_code
	)e on  e.smonth=a.smonth and a.sign_company_code=e.company_code and a.customer_code=e.customer_code
	and e.close_bill_code=a.order_code 
	-- and e.smonth=a.smonth 
	-- e.close_bill_no=a.order_code	 e.customer_code=a.customer_code
group by  a.smonth,
	a.performance_province_name  ,
	a.performance_city_name,
	a.customer_code,
	a.customer_name  ,
	a.sign_date,
	a.sign_company_code,
	a.sales_user_number,
	a.sales_user_name,
	flag,
	type_name,
	sale_rate,
	profit_rate_ceiling,
	sale_rate_celing,
	end_date,
	adjusted_profit_ratio
	)	,
by_paid as 	( --  获取本月客户回款金额
  select
    substr(sdt,1,6) as smonth,company_code,
    customer_code, --  客户编码
    sum(claim_amt) as claim_amt,	-- 回款金额（未使用，含补救单）
    sum(paid_amt) as paid_amt,	-- 回款已核销金额
    sum(residue_amt) as residue_amt	-- 回款未核销金额
  from csx_dwd.csx_dwd_sss_money_back_di --  sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>=regexp_replace(add_months(trunc('${yesterdate}','MM'),-1),'-','') and sdt<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')) 
  and regexp_replace(substr(update_time,1,10),'-','')<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')  -- 回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amt<>'0' or residue_amt<>'0') -- 剔除补救单和对应原单
  and shipper_code='YHCSX'
  group by customer_code, substr(sdt,1,6) ,company_code
)
,
-- 历史回款金额
ls_paid as (   select
    
    customer_code, --  客户编码
    sum(claim_amt) as claim_amt,	-- 回款金额（未使用，含补救单）
    sum(paid_amt) as paid_amt,	-- 回款已核销金额
    sum(residue_amt) as residue_amt	-- 回款未核销金额
  from csx_dwd.csx_dwd_sss_money_back_di --  sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>='20200601' and sdt<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')) 
  and regexp_replace(substr(update_time,1,10),'-','')<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')  -- 回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amt<>'0' or residue_amt<>'0') -- 剔除补救单和对应原单 
  and shipper_code='YHCSX'
  group by customer_code
)
select a.smonth
,a.performance_province_name
,performance_city_name
,a.customer_code
,a.cust_name
,a.sign_date
,a.sign_company_code
,a.sales_user_number
,a.sales_user_name
,sale_amt
,profit
,prorate
,salery_sales	-- 回款奖金包
,salery_fnl		-- 毛利奖金包
,salery_cal		-- 奖金包-乘系数前
,unpaid_amt_by	-- 本月应收
,unpaid_amt_sy	-- 上月应收
,payment_amount_by	-- 本月核销
,payment_amount_sy  -- 上月核销
-- 按照108267 子客户核算，以下数据为0 显示
,paid_amt		-- 本月回款已核销
,residue_amt	-- 本月回款未核销金额
,residue_amt_all	-- 上月回款未核销金额
-- , payment_ratio_amt
,sign_ratio,
 
	if( a.smonth<=end_date, salery_cal_1,0) as  salery_cal_1,
    cust_flag,
    salery_cal_2,
	sdt
 
from (
select a.smonth
,end_date
,a.performance_province_name
,performance_city_name
,a.customer_code
,a.cust_name
,a.sign_date
,a.sign_company_code
,a.sales_user_number
,a.sales_user_name
,sale_amt
,profit
,prorate
,salery_sales	-- 回款奖金包
,salery_fnl		-- 毛利奖金包
,salery_cal		-- 奖金包-乘系数前
,unpaid_amt_by	-- 本月应收
,unpaid_amt_sy	-- 上月应收
,payment_amount_by	-- 本月核销
,payment_amount_sy  -- 上月核销
-- 按照108267 子客户核算，以下数据为0 显示
,if(a.customer_code in ('108267'),0,paid_amt		)	paid_amt		-- 本月回款已核销
,if(a.customer_code in ('108267'),0,residue_amt		)	residue_amt	-- 本月回款未核销金额
,if(a.customer_code in ('108267'),0,residue_amt_all	)	residue_amt_all	-- 上月回款未核销金额
-- , payment_ratio_amt
,sign_ratio,
-- 113260 按照毛利率区间计算16%-17% 6.17% 、17%-18% 7.19%、18%-19% 8.22%、19%-20% 9.25%、20%以上 10.28%
--  customer_code in ('128951','127649')签呈客户 按照回款比例计算
case when a.customer_code='113260' and prorate >= 0.16  and prorate <  0.17 then payment_amount_by*0.0617 
    when a.customer_code='113260' and prorate >=  0.17  and prorate <  0.18 then  payment_amount_by*0.0719
    when a.customer_code='113260' and prorate >=  0.18  and prorate <  0.19 then  payment_amount_by*0.0822
    when a.customer_code='113260' and prorate >=  0.19  and prorate <  0.20 then  payment_amount_by*0.0925
    when a.customer_code='113260' and prorate >=  0.20  then payment_amount_by*0.1028
    when a.customer_code='254068' and prorate >= 0.13 and prorate < 0.17 then payment_amount_by*0.02
    when a.customer_code='254068' and prorate >= 0.17 then payment_amount_by*0.03
    when a.customer_code='114872' and prorate <= 0.1 then 0  
    when a.customer_code='114872' and prorate >  0.1 then  if((prorate-0.15)>0.06,0.02*payment_amount_by+0.06/2*payment_amount_by,0.02*payment_amount_by+(prorate-0.15)*payment_amount_by/2)
    when a.customer_code='126501' then if(prorate>=0.18,payment_amount_by*0.04, payment_amount_by*0.02) -- 该客户的提：毛利率＜18%按回款额的2%，毛利率≥18%按回款额的4%
    when a.customer_code='123328' and prorate>=0.15 and prorate<0.22 then payment_amount_by*0.01
    when a.customer_code='123328' and prorate>=0.22 then payment_amount_by*0.02 
    -- 227054 上海静安消防 202409之前按照2%，202409之后仅计算销售回款额的5%
    when a.customer_code='227054'  then if(a.smonth<='202409' , payment_amount_by*0.02,payment_amount_by*0.05)
    -- 类型5 属于按照销售回款，不超额阶段计算的
    when a.type_name=5 then payment_amount_by*sale_rate
	when a.type_name= 2   then if(a.smonth<= end_date,payment_amount_by*sale_rate ,0)
	-- 118676 三明消防毛利奖金包+销售回款基础包
	when a.type_name= 3 then if((prorate>profit_rate_ceiling) >adjusted_profit_ratio and adjusted_profit_ratio!=0 ,adjusted_profit_ratio*sale_rate_celing*payment_amount_by,
	                            if((prorate>profit_rate_ceiling),(prorate-profit_rate_ceiling)*sale_rate_celing*payment_amount_by,0))+payment_amount_by*sale_rate
	else 
	-- if(coalesce(payment_amount_by,0)=0,0,least(coalesce(sale_amt,0),coalesce(payment_amount_by,0))/coalesce(sale_amt,0)*coalesce(salery_sales,0)*sign_ratio
	-- +least(coalesce(payment_amount_by,0),coalesce(sale_amt,0))/coalesce(sale_amt,0)*coalesce(salery_fnl,0))	
	salery_fnl+salery_sales
	end as salery_cal_1,
    flag as  cust_flag,
	case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then payment_amount_by*0.0617 
    when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then  payment_amount_by*0.0719
    when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then  payment_amount_by*0.0822
    when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then  payment_amount_by*0.0925
    when a.customer_code='113260' and prorate >='0.20'   then payment_amount_by*0.1028
    when a.customer_code='114872' and prorate >  0.1 then  if((prorate-0.15)>0.05,payment_amount_by*0.02+0.06*payment_amount_by/2,payment_amount_by*(prorate-0.15)/2)
    when a.customer_code='114872' and prorate <= 0.1 then 0
    when a.customer_code in ('105750','105703','113679','113678','113746','113805','113760','118503','176508','106698','119213','119235','116947') then payment_amount_by*0.01
	when a.customer_code='128951' then payment_amount_by*0.04 
	when a.customer_code in('102924','120465','102524','171028','166470','130941','224504') then payment_amount_by*0.03
	when a.customer_code in ('113390','114038','230760') then payment_amount_by*0.05
	when a.customer_code in ('101884') then payment_amount_by*0.07
	-- 118676 三明消防毛利奖金包+销售回款基础包 当大于15%毛利率时 按照毛利额*50%
	when a.customer_code in('118676') then if(prorate>0.15,(prorate-0.15)*0.5*payment_amount_by,0)+payment_amount_by*0.02
	when a.customer_code in('122322','236143') then  if(prorate>0.15,(prorate-0.15)*0.5*payment_amount_by,0)+payment_amount_by*0.03
	when a.customer_code in ('121054','128672','125068','232541','106299','122773','128865','130078','108494','128734','129222','240210','203926','243828','243831','234071','112092','116401') then paid_amt*0.02
	else 
	    if( coalesce(unpaid_amt_by,0)=0 and unpaid_amt_sy is NULL,0,
          if(unpaid_amt_by>=sale_amt,0,
    	        if(unpaid_amt_sy is NULL,
				(1-if(coalesce(unpaid_amt_by,0)/sale_amt>1,1,coalesce(unpaid_amt_by,0)/sale_amt))*salery_cal,-- unpaid_amt_sy is null,都取的这个数据
    			      if(coalesce(unpaid_amt_sy,0)>=coalesce(unpaid_amt_by,0),
    				        if(coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)>sale_amt,sale_amt,
							      coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)),0)/sale_amt*salery_cal
							))) end  salery_cal_2,
				substr(regexp_replace(to_date(add_months('${yesterdate}',-1)),'-','') ,1,6) 		sdt
-- 113260 按照毛利率区间计算16%-17% 6.17% 、17%-18% 7.19%、18%-19% 8.22%、19%-20% 9.25%、20%以上 10.28%
--  ,case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then b.paid_amt*0.0617 
--     when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then b.paid_am*0.0719
--     when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then b.paid_am*0.0822
--     when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then b.paid_am*0.0925
--     when a.customer_code='113260' and prorate >='0.20'   then b.paid_am*0.1028
--     else 0 end salery_cal_2
from (
select 
a.smonth
,a.performance_province_name
,performance_city_name
,a.customer_code
,a.cust_name
,a.sign_date
,a.sign_company_code
,a.sales_user_number
,a.sales_user_name
,type_name
,flag
,sale_rate
,profit_rate_ceiling
,sale_rate_celing
,adjusted_profit_ratio
,coalesce(a.sale_amt,0) sale_amt
,coalesce(a.profit,0) profit
,coalesce(a.prorate,0) prorate
-- 方案提成 
,case when coalesce(payment_amount_by/10000,0)<=50 then payment_amount_by*0.02
      when coalesce(payment_amount_by/10000,0)>50 and  coalesce(payment_amount_by/10000,0)<=100 then payment_amount_by*0.022
      when coalesce(payment_amount_by/10000,0)>100 and  coalesce(payment_amount_by/10000,0)<=200 then payment_amount_by*0.024
      when coalesce(payment_amount_by/10000,0)>200 and  coalesce(payment_amount_by/10000,0)<=300 then payment_amount_by*0.026
      when coalesce(payment_amount_by/10000,0)>300  then payment_amount_by*0.028
 end salery_sales	-- 回款奖金包
,coalesce(a.salery_fnl,0) salery_fnl		-- 毛利奖金包
,coalesce(a.salery_cal,0) salery_cal		-- 奖金包-乘系数前
,coalesce(a.unpaid_amt_by,0) unpaid_amt_by	-- 本月应收
,coalesce(a.unpaid_amt_sy,0) unpaid_amt_sy	-- 上月应收
,coalesce(a.payment_amount_by,0) payment_amount_by	-- 本月核销
,coalesce(a.payment_amount_sy,0) payment_amount_sy  -- 上月核销
,coalesce(b.paid_amt,0) paid_amt					-- 本月回款已核销
,coalesce(b.residue_amt,0) residue_amt				-- 本月回款未核销金额
,coalesce(e.residue_amt ,0) residue_amt_all			-- 上月回款未核销金额
,case when coalesce(payment_amount_by/10000,0)<=50 then payment_amount_by*0.02
      when coalesce(payment_amount_by/10000,0)>50 and  coalesce(payment_amount_by/10000,0)<=100 then payment_amount_by*0.022
      when coalesce(payment_amount_by/10000,0)>100 and  coalesce(payment_amount_by/10000,0)<=200 then payment_amount_by*0.024
      when coalesce(payment_amount_by/10000,0)>200 and  coalesce(payment_amount_by/10000,0)<=300 then payment_amount_by*0.026
      when coalesce(payment_amount_by/10000,0)>300  then payment_amount_by*0.028
 end payment_ratio_amt
,1 as sign_ratio
,end_date
 -- 逻辑里有这个判断截止上月应收-截止本月应收<本月核销，按数值小的计算 
	-- if(a.sign_date<='20200930',1.5,1) 
    -- if((unpaid_amt_by is NULL or unpaid_amt_by=0)  and unpaid_amt_sy is NULL,0,
    --       if(unpaid_amt_by>=sale_amt,0,
    -- 	        if(unpaid_amt_sy is NULL,
				-- (1-if(coalesce(unpaid_amt_by,0)/sale_amt>1,1,coalesce(unpaid_amt_by,0)/sale_amt))*salery_cal,-- unpaid_amt_sy is null,都取的这个数据
    -- 			      if(coalesce(unpaid_amt_sy,0)>=coalesce(unpaid_amt_by,0),
    -- 				        if(coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)>sale_amt,sale_amt,
				-- 			      coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)),0)/sale_amt*salery_cal
				-- 				  ))) salery_cal_1	
						 
	--  *if(a.sign_date<='20200930',1.5,1) 																					
from temp_dali_cust_01 a
left join 
        by_paid b on a.customer_code = b.customer_code and a.smonth=b.smonth and a.sign_company_code=b.company_code
left join
        ls_paid e on a.customer_code = e.customer_code    
) a 
) a 
-- where a.customer_code in ('235185','230038')
;