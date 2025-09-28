
-- ******************************************************************** 
-- @功能描述：业务代理绩效
-- @创建者： 彭承华 
-- @创建者日期：2024-01-24 15:54:28 
-- @修改者日期：2025-05-18
-- @修改人：调整本月回款及上月回款，剔除上月有回款，本月红冲数据
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
create table csx_analyse_tmp.csx_analyse_tmp_hr_business_sale as
--  with tmp_sale_detail as (
with tmp_crm_info as 
(select a.*,
    if(b.customer_code is not null ,1,0) as flag,
    coalesce(type_name,1)  type_name,  -- 1 方案执行 2、按销售回款 3、销售回款+毛利率超额部份*销售回款占比sale_rate_celing，4其他特殊处理看备注说明
    coalesce(sale_rate,0)  sale_rate,   -- 销售提成系数
    coalesce(profit_rate_ceiling,0) profit_rate_ceiling, -- 毛利率超出界限 
    coalesce(sale_rate_celing,0) sale_rate_celing,      -- 毛利率上限
    coalesce(adjusted_profit_ratio,0) adjusted_profit_ratio, -- 超出毛利率上限提成比例
    coalesce(fixed_amt,0) fixed_amt,    -- 固定提成金额
    case when b.user_name is not null and  user_name !=''  then '-' else sales_user_number end  as sales_number,
    case when b.user_name is not null and  user_name !=''  then b.user_name else sales_user_name end  as sales_name,
    coalesce(b.note,'') as note,
    coalesce(b.status,1) as status,
    coalesce(substr(regexp_replace(to_date(b.end_time),'-',''),1,6),'203112') end_date,
    if( coalesce(business_name,'')='' or business_name='全业务','全业务',if(business_name='BBC','BBC',concat(business_name,'业务'))) as business_type_name,
    guarantee_profit_rate
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
)
-- select * from tmp_crm_info where customer_code='236459';
,
-- 含直送仓业务客户
 filtered_customers AS (
  SELECT DISTINCT customer_code 
  FROM csx_ods.csx_ods_data_analysis_prd_write_service_broker_sign_df 
  WHERE note LIKE '%_含直送仓%'
)
-- select * from tmp_crm_info WHERE customer_code in ('170971' ,'105567','250200')

select substr(sdt,1,6) smonth,
	    a.order_code,
	    case when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
		 end as source_bill_no_new,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		a.sub_customer_code,
		a.sub_customer_name,
		b.sales_number sales_user_number,
		b.sales_name   sales_user_name ,
		b.sign_date,
		a.sign_company_code,
	    b.flag,
	    b.type_name,
	    b.sale_rate,
	    b.profit_rate_ceiling,
	    b.sale_rate_celing,
	    b.adjusted_profit_ratio,
		sum(sale_amt )sale_amt,
		sum(profit) profit,
		coalesce(sum(profit)/sum( sale_amt ),0) prorate,
		b.end_date,
		b.fixed_amt,
		b.business_type_name,
		a.business_type_name sale_business_type_name,
		b.guarantee_profit_rate
	from csx_dws.csx_dws_sale_detail_di a
 join
-- 关联客户信息
tmp_crm_info b on b.customer_code=a.customer_code  
        and a.business_type_name=b.business_type_name
	where (
	        (sdt>='20230101' and sdt<regexp_replace(add_months(trunc('${yesterdate}','MM'),0),'-','') 
		  -- and  inventory_dc_code not in ('WB57','WB54','WB53','W0K4','WB44')  -- 福建剔除以下客户K4仓 从4月取消直送仓
		   -- 剔除子客户
		   AND  a.sub_customer_code NOT IN ('Z079454','Z099560','Z086058','102524','Z100070','Z099923','Z064006','Z065730','Z074477','Z074473','Z069386','Z074236',
		                                    'Z074762','Z068958','Z089538','Z063242','Z084424','Z102597','Z100485','Z102656','Z084002','Z083277','Z096203','Z102658','Z062118'
		                                    ,'Z096560','Z079791')
	       and ( a.customer_code not in ('245148') 
                or  ( a.customer_code in ('245148') 
                    and  a.sub_customer_code in ('245148','Z084554','Z084555','Z084556','Z084557','Z084560','Z092946','Z084558','Z084559') )
                )
            )

        )
	and b.status<>0
	and a.shipper_code='YHCSX'
	and b.business_type_name not  in ('全业务') -- 指定销售业务类型
	and a.sdt>='20230101'
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
	    adjusted_profit_ratio,
	    fixed_amt,
		b.business_type_name,
		a.business_type_name,
		a.sub_customer_code,
		a.sub_customer_name,
		guarantee_profit_rate,
		case when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
		 end
union all 

select substr(sdt,1,6) smonth,
	    a.order_code,
	    case when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
		 end  as source_bill_no_new,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		a.sub_customer_code,
		a.sub_customer_name,
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
		coalesce(sum(profit)/sum( sale_amt ),0) prorate,
		end_date,
		fixed_amt,
		b.business_type_name,
		a.business_type_name sale_business_type_name,
		guarantee_profit_rate
	from csx_dws.csx_dws_sale_detail_di a
 join
-- 关联客户信息
tmp_crm_info b on b.customer_code=a.customer_code  
--  and a.business_type_name=b.business_type_name
	where ((sdt>='20230101' and sdt<regexp_replace(add_months(trunc('${yesterdate}','MM'),0),'-','') 
		  -- and  inventory_dc_code not in ('WB57','WB54','WB53','W0K4','WB44')  -- 福建剔除以下客户K4仓
		   -- 剔除子客户
		   AND  sub_customer_code NOT IN ('Z079454','Z099560','Z086058','102524','Z100070','Z099923','Z064006','Z065730','Z074477','Z074473','Z069386','Z074236',
		                                    'Z074762','Z068958','Z089538','Z063242','Z084424','Z102597','Z100485','Z102656','Z084002','Z083277','Z096203','Z102658',
											'Z062118','Z096560','Z079791' )
	       and (( a.customer_code not in ('245148') )
                or  ( a.customer_code in ('245148') 
                and  a.sub_customer_code in ('245148','Z084554','Z084555','Z084556','Z084557','Z084560','Z092946','Z084558','Z084559') 
                )
            )
        )
        
        )
	and status<>0
	and shipper_code='YHCSX'
	and b.business_type_name    in ('全业务')   -- 按照全业务
	and a.sdt>='20230101'
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
	    adjusted_profit_ratio,
	    fixed_amt,
		b.business_type_name,
		a.business_type_name,
		a.sub_customer_code,
		a.sub_customer_name,
		guarantee_profit_rate,
		case when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
		 end
-- 	 ) select * from tmp_sale_detail a  
-- 		where 1=1
-- 	   and customer_code in ('170971' ,'105567','250200')
	    


	    
; 

-- select * from csx_analyse_tmp.csx_analyse_temp_dali_cust_01 where customer_code='245230'
-- 销售单关联结算单
drop table csx_analyse_tmp.csx_analyse_temp_dali_cust_01;
create table csx_analyse_tmp.csx_analyse_temp_dali_cust_01 as 
with tmp_sale_detail as
 (
select smonth,
	    -- a.order_code,
	    source_bill_no_new,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.sales_user_number,
		a.sales_user_name ,
		a.sign_date,
		a.sign_company_code,
		flag,
		type_name,
	    sale_rate,
	    profit_rate_ceiling,
	    sale_rate_celing,
	    adjusted_profit_ratio,
		sum(sale_amt )sale_amt,
		sum(profit) profit,
		coalesce(sum(profit)/sum( sale_amt ),0) prorate,
		end_date,
		fixed_amt,
		business_type_name,
		sale_business_type_name,
		guarantee_profit_rate
	from csx_analyse_tmp.csx_analyse_tmp_hr_business_sale a 
	-- where a.source_bill_no_new in ('OM25011600005730','OM25011500002913','OC25042500018')
	group by smonth,
	    -- a.order_code,
	    source_bill_no_new,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.sales_user_number,
		a.sales_user_name ,
		a.sign_date,
		a.sign_company_code,
		flag,
		type_name,
	    sale_rate,
	    profit_rate_ceiling,
	    sale_rate_celing,
	    adjusted_profit_ratio,
		end_date,
		fixed_amt,
		business_type_name,
		sale_business_type_name,
		guarantee_profit_rate
	),
 temp_bill_settle_detail as
 (
	 select 
	    case when source_sys='BBC' and substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-2)
		 when source_sys='BBC' and substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-1)
		 else split(a.source_bill_no,'-')[0]
		 end as source_bill_no_new,
-- 		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	 from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  a -- 销售单对账
	 where sdt=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')
	       and date(happen_date)<=last_day(add_months('${yesterdate}',-1))
	 group by
-- 	 source_bill_no,
	 case when source_sys='BBC' and substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-2)
		 when source_sys='BBC' and substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-1)
		 else split(a.source_bill_no,'-')[0]
		 end
	),
temp_bill_settle_detail_01 as (
	 select 
-- 		source_bill_no as order_code,	--  来源单号
		case when substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-2)
			 when substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-1)
			 else split(a.source_bill_no,'-')[0]
			 end as source_bill_no_new,
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di a -- 销售单对账
	where sdt=regexp_replace(last_day(add_months('${yesterdate}',-2)),'-','')
	and date(happen_date)<=last_day(add_months('${yesterdate}',-2))
	group by 
-- 	source_bill_no,
	case when substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-2)
			 when substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-1)
			 else split(a.source_bill_no,'-')[0]
			 end
	),
tmp_last_bill_account_record_snapshot as 
-- 跨月红冲的剔除
	(
		select 
			close_bill_code,
			max(bill_amt) bill_amt,
			sum(pay_amt) pay_amt,
			if(max(bill_amt)>0,if(max(bill_amt)-sum(pay_amt)>0,max(bill_amt)-sum(pay_amt),0),
			        if(max(bill_amt)-sum(pay_amt)<0,max(bill_amt)-sum(pay_amt),0)) as unpay_amt
		-- 单据核销流水明细月快照表
		from     csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf
		-- 202308及以后月快照中8月所有核销情况及以后每月快照中当月核销
		where smt>='202301'
		and smt<  regexp_replace(substr(add_months('${yesterdate}',-1),1,7),'-','')
		-- 8月作为期初核销情况及以后每个计算月当月的核销，作为历史核销过的依据
		and (smt='202301'
		or (smt>'202301' and smt<=regexp_replace(substr(add_months('${yesterdate}',-1),1,7),'-','') 
			and substr(sdt,1,6)=smt and substr(sdt,1,6)<  regexp_replace(substr(add_months('${yesterdate}',-1),1,7),'-','') ))
		and date_format(happen_date,'yyyy-MM-dd')>='2023-01-01'
		and delete_flag ='0'
		group by 
			close_bill_code
	),
	
-- 上月核销金额
tmp_last_bill_account_record_snapshot_01 as 
	(select
		  customer_code,
		  claim_bill_code,
		  close_bill_code,
		  delete_flag,
		  delete_reason,
		  sum(  pay_amt  ) pay_amt   -- 上月核销
		from
		   csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf
		where  smt=substr(regexp_replace(last_day(add_months('${yesterdate}',-2)),'-',''),1,6)
	    	and regexp_replace(substr(paid_time,1,10),'-','') between    regexp_replace(trunc(last_day(add_months('${yesterdate}',-2)),'MM'),'-','') 	and  regexp_replace(last_day(add_months('${yesterdate}',-2)),'-','')
		--   and claim_bill_code = 'RL25031900175'
		  and delete_flag='0'
		  -- and delete_reason like '%发票红冲%'
		 group by customer_code,
		  claim_bill_code,
		  close_bill_code,
		  delete_flag,
		  delete_reason
	),
-- 本月核销明细
tmp_current_bill_record_snapshot as 
(-- 核销流水明细表:本月核销金额
			select *,
			sum (pay_amt) over(partition by close_bill_code) as pay_amt_bill
			from 
			(
				select 
					close_bill_code,				
					claim_bill_code,
					close_account_code,
					customer_code,
					happen_date,
					company_code,
					date_format(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')), 'yyyy-MM-dd') paid_date,
					sum(pay_amt) pay_amt
				-- 单据核销流水明细月快照表
				from csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf
				-- 核销日期分区
				where  smt=substr(regexp_replace(last_day(add_months('${yesterdate}',-1)),'-',''),1,6)
                    and (regexp_replace(substr(happen_date,1,10),'-','')<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','') or happen_date='' or happen_date is NULL)
                    and regexp_replace(substr(paid_time,1,10),'-','') <=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','') 
                    and date_format(happen_date,'yyyy-MM-dd')>='2023-01-01'
				    and delete_flag ='0'
				-- 	and shipper_code='YHCSX'
				group by 
				close_bill_code,
				claim_bill_code,
				close_account_code,
				customer_code,
				happen_date,
				company_code,
				date_format(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')), 'yyyy-MM-dd')
			)a
),
tmp_bill_account_record_snapshot as (

select customer_code,
    smonth,
    source_bill_no_new,
    company_code,
    sum(a.pay_amt) pay_amt,
-- 	sum(b_pay_amt) b_pay_amt,
	sum(last_pay_amt) last_pay_amt,
	sum(unpay_amt) unpay_amt,
	max(pay_amt_bill) pay_amt_bill,
	sum(case when regexp_replace(substr(paid_date,1,10),'-','')>= regexp_replace(add_months(trunc('${yesterdate}','MM'),-1),'-','') 
			  and regexp_replace(substr(paid_date,1,10),'-','')<= regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')
			 then a.new_pay_amt end ) payment_amount_by,   -- 本月核销
	sum(case when regexp_replace(substr(a.paid_date,1,10),'-','')>=regexp_replace(add_months(trunc('${yesterdate}','MM'),-2),'-','') 
		  and regexp_replace(substr(paid_date,1,10),'-','')<= regexp_replace(last_day(add_months('${yesterdate}',-2)),'-','')
			 then coalesce(last_pay_amt,0) end ) payment_amount_sy
	from (
	-- 核销流水明细表中已核销金额  涉及到公司编码，需要按照单号关联
	select a.customer_code,
	    regexp_replace(substr(happen_date,1,7),'-','') smonth,
	   -- close_bill_code,
	    case when substr(split(a.close_bill_code,'-')[0],1,1)='B' and substr(split(a.close_bill_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.close_bill_code,'-')[0],2,length(split(a.close_bill_code,'-')[0])-2)
			 when substr(split(a.close_bill_code,'-')[0],1,1)='B' and substr(split(a.close_bill_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.close_bill_code,'-')[0],2,length(split(a.close_bill_code,'-')[0])-1)
			 else split(a.close_bill_code,'-')[0]
			 end as source_bill_no_new,
	    company_code,
	    paid_date,
	   -- a.claim_bill_code,
	    sum(a.pay_amt) pay_amt,
	    sum(b.pay_amt) b_pay_amt,
	    sum(c.pay_amt) last_pay_amt,
	    max(unpay_amt) unpay_amt,
	    max(pay_amt_bill) pay_amt_bill,
	    sum(case when b.close_bill_code is null  then a.pay_amt when unpay_amt=0 then 0
	            when unpay_amt<>0 and a.pay_amt_bill<>0 and abs(a.pay_amt_bill/b.unpay_amt)>1 then a.pay_amt/abs(a.pay_amt_bill/unpay_amt)
	            else a.pay_amt end 
	        ) new_pay_amt
-- 	sum(pay_amt) paid_amount                        -- 总核销
	from
      tmp_current_bill_record_snapshot a	
	left join 
	 (select *  
	  from tmp_last_bill_account_record_snapshot 
	     
	   ) b
	    on  a.close_bill_code=b.close_bill_code  
	   -- and a.claim_bill_code=b.claim_bill_code 
	 left join 
	 (select * 
	 from tmp_last_bill_account_record_snapshot_01 
	    where delete_flag=0
	  ) c
	    on a.claim_bill_code=c.claim_bill_code and a.close_bill_code=c.close_bill_code 
	group by a.customer_code,
	   -- a.claim_bill_code,
	    regexp_replace(substr(happen_date,1,7),'-','')
	    ,company_code,
	   -- close_bill_code,
	    case when substr(split(a.close_bill_code,'-')[0],1,1)='B' and substr(split(a.close_bill_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.close_bill_code,'-')[0],2,length(split(a.close_bill_code,'-')[0])-2)
			 when substr(split(a.close_bill_code,'-')[0],1,1)='B' and substr(split(a.close_bill_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.close_bill_code,'-')[0],2,length(split(a.close_bill_code,'-')[0])-1)
			 else split(a.close_bill_code,'-')[0]
			 end ,
			 paid_date
	)a 
	group by customer_code,
    smonth,
    source_bill_no_new,
    company_code
	)
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
	coalesce(sum(a.profit)/sum(a.sale_amt),0) prorate,
	round(sum(a.sale_amt)*0.02,2) salery_sales,
	round(if(coalesce(sum(a.profit)/sum(a.sale_amt),0)-0.15 > 0 and a.customer_code not in('104179','112092','113260'),(coalesce(sum(a.profit)/sum(a.sale_amt),0)-0.15),0)*sum(coalesce(e.payment_amount_by,0))*0.5,4) salery_fnl,
	round(sum(a.sale_amt)*0.02,2)+round(if( coalesce(sum(a.profit)/sum(a.sale_amt),0)-0.15 >= 0 and a.customer_code not in('104179','112092','113260'),(coalesce(sum(a.profit)/sum(a.sale_amt),0)-0.15),0)*sum(a.sale_amt)*0.5,2) salery_cal,
	sum(coalesce(c.unpaid_amount,0)) unpaid_amt_by,
	sum(coalesce(d.unpaid_amount,0)) unpaid_amt_sy,
	sum(coalesce(e.payment_amount_by,0)) payment_amount_by,   -- 本月核销
	sum(coalesce(e.payment_amount_sy,0)) payment_amount_sy,	    -- 上月核销
	end_date,
	fixed_amt,
	guarantee_profit_rate,
	regexp_replace('${yesterdate}','-','') as sdt
from 
	 tmp_sale_detail  a
left join 
	 temp_bill_settle_detail c on c.source_bill_no_new=a.source_bill_no_new
left join 
	 temp_bill_settle_detail_01 d on d.source_bill_no_new=a.source_bill_no_new	
left join 
	tmp_bill_account_record_snapshot e on   a.sign_company_code=e.company_code and a.customer_code=e.customer_code
	and e.source_bill_no_new=a.source_bill_no_new 
--  where a.customer_code in ('245230','245148','236459')
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
	adjusted_profit_ratio,
	fixed_amt,
	guarantee_profit_rate
	
	
;
-- 计算提成
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_business_agent;
-- create table csx_analyse_tmp.csx_analyse_tmp_hr_business_agent as 


-- 猎取本月回款金额
with by_paid as 	( --  获取本月客户回款金额
  select
    substr(sdt,1,6) as smonth,
    company_code,
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
  where (sdt>='20230101' and sdt<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')) 
  and regexp_replace(substr(update_time,1,10),'-','')<=regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')  -- 回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amt<>'0' or residue_amt<>'0') -- 剔除补救单和对应原单 
  and shipper_code='YHCSX'
  group by customer_code
)
insert overwrite table csx_analyse.csx_analyse_fr_hr_business_agent_mi partition(smt)
select 
a.smonth  sale_month
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
 
if( a.smonth<=end_date, coalesce(salery_cal_1,0),0) as  salery_cal_1,
cust_flag,
salery_cal_2,
current_timestamp() update_time,
substr(sdt,1,6) as smonth,
substr(sdt,1,6) as smt
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
case when a.customer_code='113260'  then payment_amount_by* 
    case when  prorate >= 0.16  and prorate <  0.17 then 0.0617 
        when prorate <  0.18 then  0.0719
        when prorate <  0.19 then  0.0822
        when prorate <  0.20 then  0.0925
        when prorate >=  0.20  then 0.1028
        end 
    when a.customer_code='254068' then  payment_amount_by* 
        case when prorate >= 0.13 and prorate < 0.17 then 0.02
             when prorate >= 0.17 then 0.03
         end
    when a.customer_code='102924' then  payment_amount_by* 
        case when prorate >= 0.19 then 0.03
             when prorate >= 0.17 then 0.02
             when prorate < 0.17 then 0.01
         end
    WHEN a.customer_code = '114872' THEN   0.02 * payment_amount_by +  LEAST(GREATEST(prorate - 0.15, 0), 0.06) * payment_amount_by / 2
    when a.customer_code='126501' then if(prorate>=0.18,payment_amount_by*0.04, payment_amount_by*0.02) -- 该客户的提：毛利率＜18%按回款额的2%，毛利率≥18%按回款额的4%
    when a.customer_code='123328' and prorate>=0.15 and prorate<0.22 then payment_amount_by*0.01
    when a.customer_code='123328' and prorate>=0.22 then payment_amount_by*0.02 
    -- 227054 上海静安消防 202409之前按照2%，202409之后仅计算销售回款额的5%
    when a.customer_code='227054'  then if(a.smonth<='202409' , payment_amount_by*0.02,payment_amount_by*0.05)
    
    -- 类型5 核销回款*0.02+超额毛利率*50%* 有限制保底毛利率 prorate>=guarantee_profit_rate
    -- 当毛利率小于保底时，提成为0，当回款额提成 2%+超定价毛利率 15%部分的 50%。最高不超过回款额提成 4%。超出：这个计算=回款额提成2%+ 0.02*回款额，没超出部：计算 =回款额提成2%+ （毛利率-超出部分15%）*50%*回款额
    when a.type_name=5  then IF(
      prorate < guarantee_profit_rate, 
      0,
      IF(
        (prorate - profit_rate_ceiling) * sale_rate_celing >= adjusted_profit_ratio 
        AND adjusted_profit_ratio != 0, 
        adjusted_profit_ratio * payment_amount_by + payment_amount_by * sale_rate,
        (prorate - profit_rate_ceiling) * sale_rate_celing * payment_amount_by + payment_amount_by * sale_rate
      )
    ) 
	when a.type_name= 2   then if(a.smonth<= end_date,payment_amount_by*sale_rate ,0)
	-- 核销回款*0.02+超额毛利率*50%*
	when a.type_name= 3 then  if( (prorate-profit_rate_ceiling)*sale_rate_celing >=adjusted_profit_ratio and adjusted_profit_ratio!=0 ,adjusted_profit_ratio*payment_amount_by,
	                            if((prorate>profit_rate_ceiling) ,(prorate-profit_rate_ceiling)*sale_rate_celing*payment_amount_by,0))+payment_amount_by*sale_rate
	when a.type_name=6 then case when  sale_amt>0 then coalesce(fixed_amt,0) else 0 end 
    WHEN a.type_name = 7 THEN 
        payment_amount_by*(
          sale_rate + 
          CASE 
            WHEN prorate <= profit_rate_ceiling THEN 0
            WHEN COALESCE(sale_rate_celing, 0) != 0 
              AND (prorate - profit_rate_ceiling)/2 > COALESCE(sale_rate_celing, 0) THEN sale_rate_celing
            ELSE (prorate - profit_rate_ceiling) / 2 
          END  
        ) 
        -- 当正常提成大于销售回款*系数，取回款*系数，否则正常计算
	when a.type_name=8 then if(coalesce(salery_fnl,0)+ coalesce(salery_sales,0) >payment_amount_by*sale_rate,payment_amount_by*sale_rate, coalesce(salery_fnl,0)+ coalesce(salery_sales,0))
	else 
	-- if(coalesce(payment_amount_by,0)=0,0,least(coalesce(sale_amt,0),coalesce(payment_amount_by,0))/coalesce(sale_amt,0)*coalesce(salery_sales,0)*sign_ratio
	-- +least(coalesce(payment_amount_by,0),coalesce(sale_amt,0))/coalesce(sale_amt,0)*coalesce(salery_fnl,0))	
	coalesce(salery_fnl,0)+ coalesce(salery_sales,0)
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
	when a.customer_code in ('121054','128672','125068','232541','106299','122773','128865','130078','108494','128734','129222','240210','203926','243828','243831','234071','112092','116401') then payment_amount_by*0.02
	else 
	    if( coalesce(unpaid_amt_by,0)=0 and unpaid_amt_sy is NULL,0,
          if(unpaid_amt_by>=sale_amt,0,
    	        if(unpaid_amt_sy is NULL,
				(1-if(coalesce(unpaid_amt_by,0)/sale_amt>1,1,coalesce(unpaid_amt_by,0)/sale_amt))*salery_cal,-- unpaid_amt_sy is null,都取的这个数据
    			      if(coalesce(unpaid_amt_sy,0)>=coalesce(unpaid_amt_by,0),
    				        if(coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)>sale_amt,sale_amt,
							      coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)),0)/sale_amt*salery_cal
							))) end  salery_cal_2,
				substr(regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','') ,1,6) 		sdt
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
,fixed_amt
,guarantee_profit_rate
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
from csx_analyse_tmp.csx_analyse_temp_dali_cust_01 a
left join 
        by_paid b on a.customer_code = b.customer_code and a.smonth=b.smonth and a.sign_company_code=b.company_code
left join
        ls_paid e on a.customer_code = e.customer_code    
) a 
) a 
-- where a.performance_province_name in ('福建省','江西省')
;