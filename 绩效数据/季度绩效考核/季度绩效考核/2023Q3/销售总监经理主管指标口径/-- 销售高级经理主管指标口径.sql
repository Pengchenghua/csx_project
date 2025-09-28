-- 销售总监经理主管指标口径 

-- -- -- -- -- -- -基本销售人员信息  服务管家-- -客服经理-- -- 高级客服经理
drop table csx_analyse_tmp.customer_sale_service_manager_leader01;
create  table csx_analyse_tmp.customer_sale_service_manager_leader01
as 
select
sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_id_new,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_id,
			rp_leader_user_number,
			rp_leader_name,
			rp_city_leader_id,
			b.user_number as rp_leader_city_user_number,
      		b.name as rp_leader_city_name,
			fl_service_user_id_new,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_id,
			fl_leader_user_number,
			fl_leader_name,
			fl_city_leader_id,
			c.user_number as fl_leader_city_user_number,
      		c.name as fl_leader_city_name,
			bbc_service_user_id_new,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_id,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_city_leader_id,
			d.user_number as bbc_leader_city_user_number,
      		d.name as bbc_leader_city_name
from (
      select
      sdt,customer_no,customer_name,region_name,province_name,city_group_name,
      			rp_service_user_id_new,
      			rp_service_user_work_no_new,
      			rp_service_user_name_new,
      			rp_leader_id,
      			b.user_number as rp_leader_user_number,
      			b.name as rp_leader_name,
      			b.leader_id as rp_city_leader_id,      			
      			fl_service_user_id_new,
      			fl_service_user_work_no_new,
      			fl_service_user_name_new,
      			fl_leader_id,
      			c.user_number as fl_leader_user_number,
      			c.name as fl_leader_name,
      			c.leader_id as fl_city_leader_id,      			
      			bbc_service_user_id_new,
      			bbc_service_user_work_no_new,
      			bbc_service_user_name_new,
      			bbc_leader_id,
      			d.user_number as bbc_leader_user_number,
      			d.name as bbc_leader_name,
      			d.leader_id as bbc_city_leader_id    
      from (
      select
      sdt,customer_no,customer_name,region_name,province_name,city_group_name,
      			rp_service_user_id_new,
      			rp_service_user_work_no_new,
      			rp_service_user_name_new,
      			b.leader_id as rp_leader_id,
      			fl_service_user_id_new,
      			fl_service_user_work_no_new,
      			fl_service_user_name_new,
      			c.leader_id as fl_leader_id,
      			bbc_service_user_id_new,
      			bbc_service_user_work_no_new,
      			bbc_service_user_name_new,
      			d.leader_id as bbc_leader_id
      from 
      
           (select distinct 
      			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
      			rp_service_user_id_new,
      			rp_service_user_work_no_new,
      			rp_service_user_name_new,
      			fl_service_user_id_new,
      			fl_service_user_work_no_new,
      			fl_service_user_name_new,
      			bbc_service_user_id_new,
      			bbc_service_user_work_no_new,
      			bbc_service_user_name_new
      		from
      			csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
      		where
      			sdt in ('20230930')
      			)a
      left join
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20230930'
      ) b on a.rp_service_user_id_new=b.id
      
      left join 
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20230930'
      ) c on a.fl_service_user_id_new=c.id
      
      left join 
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20230930'
      ) d on a.bbc_service_user_id_new=d.id
      )a 
      
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20230930'
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) b on a.rp_leader_id=b.id
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20230930'
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) c on a.fl_leader_id=c.id
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20230930'
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) d on a.bbc_leader_id=d.id
 )a 
left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt='20230930'
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) b on a.rp_city_leader_id=b.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt='20230930'
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) c on a.fl_city_leader_id=c.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt='20230930'
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) d on a.bbc_city_leader_id=d.id;



-- B销售汇总

          select 
				performance_province_name province_name,
				performance_city_name city_group_name,
				substr(sdt,1,6) smonth,
				sales_user_number,
				sales_user_name,
				user_position,
				dic_value,
				sum(sale_amt) as sale_amt,
				sum(profit) profit,
				sum(profit_no_tax) excluding_tax_profit,
				sum(sale_amt_no_tax) excluding_tax_sales
           from     csx_dws.csx_dws_sale_detail_di a 
           left join 
           (select user_number,user_name,dic_key,user_position,	dic_value from   csx_dim.csx_dim_uc_user a 
      left join 
     (select dic_type,dic_key,dic_value from  csx_dim.csx_dim_csx_basic_data_md_dic where sdt='current' and dic_type='POSITION') b on a.user_position=b.dic_key
      where sdt='20231204'
      and status=0
      and delete_flag=0) b on a.sales_user_number=b.user_number
           where sdt >='20231001'  and sdt <= '20231130'
		    and customer_code not in ('120459','121206')
			    and business_type_code in ('1','2','4','6') and channel_code in ('1','7','9') 
		   group by 		performance_province_name  ,
				performance_city_name  ,
				substr(sdt,1,6)  ,
				sales_user_number,
				sales_user_name,
				user_position,
				dic_value
		
		;
-- B端销售明细
select
    a.province_name,
    a.city_group_name,
  	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
    a.customer_code,
    b.customer_name,
    a.smonth,
    a.business_type_name,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(excluding_tax_sales) excluding_tax_sales,
    sum(excluding_tax_profit) excluding_tax_profit
from   (
          select 
                customer_code,
				performance_province_name province_name,
				performance_city_name city_group_name,
				substr(sdt,1,6) smonth,
				business_type_name,
				sum(sale_amt) as sale_amt,
				sum(profit) profit,
				sum(profit_no_tax) excluding_tax_profit,
				sum(sale_amt_no_tax) excluding_tax_sales
           from   csx_dws.csx_dws_sale_detail_di
           where sdt >='20230701'  and sdt <= '20230930'
		    and customer_code not in ('120459','121206')
			    and business_type_code in ('1','2','4','6') and channel_code in ('1','7','9') 
		   group by customer_code,performance_province_name,performance_city_name,substr(sdt,1,6),business_type_name
		)a			
join 
   (
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
		) b on a.customer_code=b.customer_no 
group by 
      a.province_name,
      a.city_group_name,
	b.rp_service_user_work_no_new,
	b.rp_service_user_name_new,
	rp_leader_user_number,
	rp_leader_name,
	rp_leader_city_user_number,
    rp_leader_city_name,
	b.fl_service_user_work_no_new,
	b.fl_service_user_name_new,
	fl_leader_user_number,
	fl_leader_name,
	fl_leader_city_user_number,
    fl_leader_city_name,
	b.bbc_service_user_work_no_new,
	b.bbc_service_user_name_new,
	bbc_leader_user_number,
	bbc_leader_name,
	bbc_leader_city_user_number,
    bbc_leader_city_name,
      a.customer_code,
      b.customer_name,
      a.business_type_name,
	  a.smonth   
      ;

-- 对帐率汇总
select
	--  substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
	sum(a.bill_amt_all),-- 对账金额
	sum(a.sale_amt_all),-- 财务含税销售额
	sum(a.bill_amt_all)/sum(a.sale_amt_all)  as statement_ratio  --  对账率
	--  a.kp_ratio -- 开票率	
from
	(
	select
		customer_code,company_code,company_name,
		sum(bill_amt_all) bill_amt_all,-- 对账金额
		sum(sale_amt_all) sale_amt_all -- 财务含税销售额	
		--  invoice_amount_all/sale_amt_all as kp_ratio
		--  statement_ratio,-- 对账率
		--  kp_ratio,-- 开票率
		--  sdt
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20230910','20230810','20231010')
	group by customer_code,company_code,company_name
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and sale_amt_all !=0
group by b.province_name,
	rp_leader_user_number,
	rp_leader_name,
	rp_leader_city_user_number,
    rp_leader_city_name,
	fl_leader_user_number,
	fl_leader_name,
	fl_leader_city_user_number,
    fl_leader_city_name,
	bbc_leader_user_number,
	bbc_leader_name,
	bbc_leader_city_user_number,
    bbc_leader_city_name 
	;

-- 对账率明细
select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
	a.bill_amt_all,-- 对账金额
	-- a.unstatement_amount,-- 未对账金额
	--  a.invoice_amount_all,-- 开票金额
	a.sale_amt_all,-- 财务含税销售额
	a.bill_amt_all/a.sale_amt_all  as statement_ratio  --  对账率
	--  a.kp_ratio -- 开票率	
from
	(
	select
		customer_code,company_code,company_name,
		sum(bill_amt_all) bill_amt_all,-- 对账金额
		sum(sale_amt_all) sale_amt_all, -- 财务含税销售额
	
		--  invoice_amount_all/sale_amt_all as kp_ratio
		--  statement_ratio,-- 对账率
		--  kp_ratio,-- 开票率
		 sdt
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20230910','20230810','20231010')
	group by customer_code,company_code,company_name,sdt
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and sale_amt_all !=0  

-- 回款率汇总
select

	b.province_name as province_name,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
	sum(receivable_amount_target) receivable_amount_target,
	sum(current_receivable_amount) current_receivable_amount,
    sum(if(receivable_amount_target<=0,0,(current_receivable_amount*-1)))/sum(if(receivable_amount_target<=0,0,receivable_amount_target)) as receivable_rate -- 回款率

from
(
 select 
	substr(sdt,1,6) as smonth,
	company_code,		--  公司代码
	company_name,		--  公司名称
	customer_code,		--  客户编码
	customer_name,
	--  credit_code,		--  信控编号
	--  credit_business_attribute_code,		--  信控业务属性编码
	--  credit_business_attribute_name,		--  信控业务属性名称					
	--  account_period_code,		--  账期编码
	--  account_period_name,		--  账期名称
	--  account_period_value,		--  账期值
    sum(receivable_amount_target) receivable_amount_target,  		--  回款目标:取1号预测回款金额
    sum(current_receivable_amount) current_receivable_amount		--  当期回款金额	
from csx_analyse.csx_analyse_fr_sap_customer_credit_forecast_collection_report_df  --  承华  预测回款金额-帆软
where sdt in ('20230731','20230831','20230930')
group by 	company_code,		--  公司代码
	company_name,		--  公司名称
	customer_code,		--  客户编码
	customer_name,substr(sdt,1,6)
	)a 
join 
   (
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
	where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and receivable_amount_target !=0
group by b.province_name,
	rp_leader_user_number,
	rp_leader_name,
	rp_leader_city_user_number,
    rp_leader_city_name,
	fl_leader_user_number,
	fl_leader_name,
	fl_leader_city_user_number,
    fl_leader_city_name,
	bbc_leader_user_number,
	bbc_leader_name,
	bbc_leader_city_user_number,
    bbc_leader_city_name 
	;

-- 回款率明细 
select
    a.smonth,
	b.province_name as province_name,
	a.customer_code,
	a.customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
	receivable_amount_target,
	current_receivable_amount,	
   if(receivable_amount_target<=0,0,(current_receivable_amount*-1)/receivable_amount_target) as receivable_rate -- 回款率
from
(
 select 
	substr(sdt,1,6) as smonth,
	company_code,		--  公司代码
	company_name,		--  公司名称
	customer_code,		--  客户编码
	customer_name,
	--  credit_code,		--  信控编号
	--  credit_business_attribute_code,		--  信控业务属性编码
	--  credit_business_attribute_name,		--  信控业务属性名称					
	--  account_period_code,		--  账期编码
	--  account_period_name,		--  账期名称
	--  account_period_value,		--  账期值
    sum(receivable_amount_target) receivable_amount_target,  		--  回款目标:取1号预测回款金额
    sum(current_receivable_amount) current_receivable_amount		--  当期回款金额		
   -- sum(if(current_receivable_amount<0,-current_receivable_amount,0))/sum(receivable_amount_target) back_rate,
from csx_analyse.csx_analyse_fr_sap_customer_credit_forecast_collection_report_df  --  承华  预测回款金额-帆软
where sdt in ('20230731','20230831','20230930')
group by 	company_code,		--  公司代码
	company_name,		--  公司名称
	customer_code,		--  客户编码
	customer_name,substr(sdt,1,6)
	)a 
join 
   (
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
	where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and receivable_amount_target !=0   
 

 ; 
-- 新签合同金额

SELECT
	   c.performance_province_name,
       c.performance_city_name,
       c.province_manager_user_number,
       c.province_manager_user_name,
       c.city_manager_user_number,
       c.city_manager_user_name, 
       c.sales_manager_user_number,
       c.sales_manager_user_name, 
       c.supervisor_user_number,
       c.supervisor_user_name, 			
sum(if(b.htbh is not null,b.htjey/10000,a.estimate_contract_amount)/ 
if(if(b.htbh is not null,yue,regexp_extract(a.contract_cycle_desc,'[0-9]+',0))>12,if(b.htbh is not null,yue,regexp_extract(a.contract_cycle_desc,'[0-9]+',0))/12,1)) jine
from (
		select
			if(to_date(business_sign_time)=to_date(first_business_sign_time),'新商机','老商机') cust_flag,
			business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,
			business_number,
			customer_id,
			customer_code,
			customer_name,
            contract_number,			
			performance_region_name,performance_province_name,performance_city_name,
            business_type_code,
			business_type_name,
			contract_cycle_desc,estimate_contract_amount,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date,business_stage
		from 
			csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			and business_attribute_code in (1,2,5) --  商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
			and status=1  --  是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			and to_date(business_sign_time) >= '2023-07-01'   and to_date(business_sign_time) < '2023-10-01' 
			and performance_province_name !='平台-B'
		)a
left join 
(select 
    t1.htbh,--  合同编码
   (case when length(trim(t1.customernumber))>0 then trim(t1.customernumber) else t3.customer_code end) as customer_no,  --  客户编码
   	htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
	 ROUND(datediff(htzzrq,htqsrq)/30.5,0) yue
from 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
   where sdt='20230930' 
   and length(htbh)>0) t1 
 left join 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_dt4_df 
   where sdt='20230930') t2 
   on t1.id=t2.mainid 
left join 
   (select * 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt='20230930') t3 
   on t2.khmc=t3.customer_name)b   on b.customer_no=a.customer_code  and b.htbh=a.contract_number   

left join   
       (
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= '20230930'
            and channel_code  in ('1','7','9')
        ) c on a.customer_code=c.customer_code 
group by c.performance_province_name,
       c.performance_city_name,
       c.province_manager_user_number,
       c.province_manager_user_name,
       c.city_manager_user_number,
       c.city_manager_user_name, 
       c.sales_manager_user_number,
       c.sales_manager_user_name, 
       c.supervisor_user_number,
       c.supervisor_user_name
;

--  新签合同金额明细
SELECT
	   c.performance_province_name,
       c.performance_city_name,
       c.province_manager_user_number,
       c.province_manager_user_name,
       c.city_manager_user_number,
       c.city_manager_user_name, 
       c.sales_manager_user_number,
       c.sales_manager_user_name, 
       c.supervisor_user_number,
       c.supervisor_user_name, 
       c.sales_user_number,
	   c.sales_user_name,
	   a.cust_flag,
		a.business_number,
		a.business_sign_time,
		a.business_sign_month,
		a.business_number,
		a.customer_code,
		c.customer_name,
       a.contract_number,			
       a.business_type_code,
		a.business_type_name,
		a.business_sign_date,
		a.business_sign_date_2,
		a.first_business_sign_date,
		if(b.htbh is not null,yue,regexp_extract(a.contract_cycle_desc,'[0-9]+',0)) yue,
 if(if(b.htbh is not null,yue,regexp_extract(a.contract_cycle_desc,'[0-9]+',0))>12,if(b.htbh is not null,yue,regexp_extract(a.contract_cycle_desc,'[0-9]+',0))/12,1) zhishu,			
			if(b.htbh is not null,b.htjey/10000,a.estimate_contract_amount) contract_amount,
if(b.htbh is not null,b.htjey/10000,a.estimate_contract_amount)/ if(if(b.htbh is not null,yue,regexp_extract(a.contract_cycle_desc,'[0-9]+',0))>12,if(b.htbh is not null,yue,regexp_extract(a.contract_cycle_desc,'[0-9]+',0))/12,1)
from (
		select
			if(to_date(business_sign_time)=to_date(first_business_sign_time),'新商机','老商机') cust_flag,
			business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,
			business_number,
			customer_id,
			customer_code,
			customer_name,
            contract_number,			
			performance_region_name,performance_province_name,performance_city_name,
            business_type_code,
			business_type_name,
			contract_cycle_desc,estimate_contract_amount,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date,business_stage
		from 
			csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			and business_attribute_code in (1,2,5) --  商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
			and status=1  --  是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			and to_date(business_sign_time) >= '2023-07-01'  and to_date(business_sign_time) < '2023-10-01' 
			and performance_province_name !='平台-B'
		)a
left join 
(select 
    t1.htbh,--  合同编码
   (case when length(trim(t1.customernumber))>0 then trim(t1.customernumber) else t3.customer_code end) as customer_no,  --  客户编码
   	htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
	 ROUND(datediff(htzzrq,htqsrq)/30.5,0) yue
from 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
   where sdt='20230930' 
   and length(htbh)>0) t1 
 left join 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_dt4_df 
   
   where sdt='20230930') t2 
   on t1.id=t2.mainid 
left join 
   (select * 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt='20230930') t3 
   on t2.khmc=t3.customer_name)b   on b.customer_no=a.customer_code  and b.htbh=a.contract_number   

left join   (
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= '20230930'
            and channel_code  in ('1','7','9')
        ) c on a.customer_code=c.customer_code 
        
;

--1.0 A类客户断约 A类客户明细


--  昨日季度 1日，昨日、上季度1日，上季度末; 
set i_sdate_m11 ='20221101';
set i_sdate_m12 ='20221231';

set i_sdate_m21 ='20220701';		
set i_sdate_m22 ='20220930';	

-- A类客户
drop table csx_tmp.tmp_xiaoshou_ratio;
CREATE  table csx_tmp.tmp_xiaoshou_ratio
as
with tmp_xiaoshou_ratio as (	
select
  customer_code,
  province_name,
  city_group_name
from
(
  select 
    customer_code,province_name,city_group_name,profit,sale_amt,
    entry_ratio,
    rno,
    entry_rno,
    lag(entry_rno,1,0) over (partition by province_name,city_group_name order by entry_rno) lag_entry_rno
  from
  (
    select -- 占比累加直至0.7||0.4
      customer_code,
	  province_name,
	  city_group_name,
	  profit,
	  sale_amt,
      entry_ratio,
      rno,
      sum(entry_ratio) over( partition by province_name,city_group_name order by rno  rows between UNBOUNDED PRECEDING and 0 PRECEDING ) as entry_rno
    from
    (
	  select
	  customer_code,province_name,city_group_name,profit,sale_amt,
	  cast(entry_ratio as decimal(30,10)) as entry_ratio,
      row_number() over (partition by province_name,city_group_name order by entry_ratio desc) rno
	  
	  from 
	  ( -- 金额占比
	    select
	    customer_code,province_name,city_group_name,profit,sale_amt,
	    sale_amt/sum(sale_amt) over (partition by province_name,city_group_name) entry_ratio
	    from
		 (
            select 
              customer_code,
			  performance_province_name   province_name,
              performance_city_name   city_group_name,
			  sum(sale_amt) as sale_amt,
			  sum(profit) profit			  
            from   csx_dws.csx_dws_sale_detail_di
            where sdt >='20230401'  and sdt <= '20230630'
			and business_type_code='1'     -- and channel_code in ('1','7','9') 
			AND  order_channel_code<>4
			group  by customer_code,
			performance_province_name,
			performance_city_name
          )a
       )a
	)c
  )d
)e
where ((entry_rno<=0.7 or (entry_rno>0.7 and lag_entry_rno<0.7) or (entry_rno>0.7 and rno=1)) and profit/abs(sale_amt)>=0.098)
  or ((entry_rno<=0.4 or (entry_rno>0.4 and lag_entry_rno<0.4) or (entry_rno>0.4 and rno=1)) and profit/abs(sale_amt)>=0.048)
  ),
 
 -- -A类客户为空时，取该省区城市的top3客户  
 --  drop table csx_tmp.tmp_xiaoshou_ratio01; CREATE  table csx_tmp.tmp_xiaoshou_ratio01 as
tmp_xiaoshou_ratio01 as (	
select
 customer_code,
 province_name,
 city_group_name
from (
select
 a.customer_code,
 a.province_name,
 a.city_group_name
from (select
	          province_name,
			  city_group_name,
			  customer_code,
			  customer_name,
	 row_number() over (partition by province_name,city_group_name order by sale_amt desc) entry_ratio
	from(
     select 
              a.performance_province_name      province_name,
              a.performance_city_name     city_group_name,

			  a.customer_code,
			  c.customer_name,
			  sum(sale_amt) as sale_amt			  
            from   csx_dws.csx_dws_sale_detail_di a 
			left join   (
                          select *
                          from csx_dim.csx_dim_crm_customer_info
                          where sdt= '20230930'
							and channel_code  in ('1','7','9')
                        ) c on a.customer_code=c.customer_code 	
            where a.sdt >='20230401'  and a.sdt <='20230630'
			and a.business_type_code='1' -- and channel_code in ('1','7','9') 
			AND a.order_channel_code<>4
			group  by a.performance_province_name,
              a.performance_city_name,
			  a.customer_code,
			  c.customer_name
          )a
		)a 
left join 
 (
 select
  province_name,
  city_group_name,
  count(customer_code) cnt 
 from tmp_xiaoshou_ratio
 GROUP BY  province_name,
         city_group_name
 ) c	on	c.city_group_name=a.city_group_name
where a.entry_ratio<=3 and (c.city_group_name is null or c.cnt<3)
union all 
select
  customer_code,
  province_name,
  city_group_name 
from tmp_xiaoshou_ratio 
)a 
group by customer_code,
province_name,
city_group_name)
-- insert overwrite table csx_tmp.sale_d_customer_break_about  partition(sdt)
-- -- -A类断约履约客户 mingxi
SELECT
	   c.performance_province_name      sales_province_name,
       c.performance_city_name     city_group_name,
       c.province_manager_user_number fourth_supervisor_work_no,
       c.province_manager_user_name fourth_supervisor_name,
       c.city_manager_user_number third_supervisor_work_no,
       c.city_manager_user_name third_supervisor_name, 
       c.sales_manager_user_number second_supervisor_work_no,
       c.sales_manager_user_name second_supervisor_name, 
       c.supervisor_user_number first_supervisor_work_no,
       c.supervisor_user_name first_supervisor_name, 
	   a.customer_code customer_no,
	   c.customer_name,
       if(b.customer_code is null ,'断约客户',null) typeder  
from  tmp_xiaoshou_ratio01 a
left join (select 
              distinct customer_code
            from   csx_dws.csx_dws_sale_detail_di
            where sdt >='20230701'  and sdt <= '20230930'
			and business_type_code='1' -- and channel_code in ('1','7','9') 
			AND  order_channel_code<>4
            )b	on  a.customer_code=b.customer_code 					
left join   (
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= '20230930'
            and channel_code  in ('1','7','9')
        ) c on a.customer_code=c.customer_code 
;

-- 服务管家 客服经理 高级客服经理

SELECT
	b.province_name,
	b.city_group_name,
	a.customer_code,
	b.customer_name,
    coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
    if(c.customer_code is null ,'断约客户',null) typeder  
from  tmp_xiaoshou_ratio01 a
left join (select 
              distinct customer_code
            from   csx_dws.csx_dws_sale_detail_di
            where sdt >='20230701'  and sdt <= '20230930'
			and business_type_code='1' -- and channel_code in ('1','7','9') 
			AND  order_channel_code<>4
            )c	on  a.customer_code=c.customer_code 					
join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
		) b on a.customer_code=b.customer_no
;

	
--  开票汇总
select

	b.province_name as province_name,

	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,

	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,

	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,

	sum(a.invoice_amount_all),-- 开票金额
	sum(a.sale_amt_all),-- 财务含税销售额

	sum(a.invoice_amount_all)/sum(a.sale_amt_all) kp_ratio -- 开票率	
from
	(
	select
		customer_code,company_code,company_name,
		sum(sale_amt) sale_amt_all,-- 财务含税销售额
		sum(invoice_amount) invoice_amount_all -- 开票金额
	
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20230915','20230815','20231022')
	group by  customer_code,company_code,company_name
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and sale_amt_all !=0
group by b.province_name,
	rp_leader_user_number,
	rp_leader_name,
	rp_leader_city_user_number,
    rp_leader_city_name,
	fl_leader_user_number,
	fl_leader_name,
	fl_leader_city_user_number,
    fl_leader_city_name,
	bbc_leader_user_number,
	bbc_leader_name,
	bbc_leader_city_user_number,
    bbc_leader_city_name;

--  开票明细
select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(rp_leader_name,'') as rp_leader_name,
	coalesce(rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(fl_leader_name,'') as fl_leader_name,
	coalesce(fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(bbc_leader_name,'') as bbc_leader_name,
	coalesce(bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(bbc_leader_city_name,'') as bbc_leader_city_name,
	--  a.bill_amt_all,-- 对账金额
	--  a.unstatement_amount,-- 未对账金额
	a.invoice_amount,-- 开票金额
	a.sale_amt,-- 财务含税销售额
	--  a.statement_ratio-- 对账率
	a.kp_ratio -- 开票率	
from
	(
	select
		customer_code,company_code,company_name,
		invoice_amount,-- 开票金额
		sale_amt,-- 财务含税销售额
		invoice_amount/sale_amt as kp_ratio,
		sdt
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20230915','20230815','20231022')
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,
			rp_service_user_name_new,
			rp_leader_user_number,
			rp_leader_name,
			rp_leader_city_user_number,
      		rp_leader_city_name,
			fl_service_user_work_no_new,
			fl_service_user_name_new,
			fl_leader_user_number,
			fl_leader_name,
			fl_leader_city_user_number,
      		fl_leader_city_name,
			bbc_service_user_work_no_new,
			bbc_service_user_name_new,
			bbc_leader_user_number,
			bbc_leader_name,
			bbc_leader_city_user_number,
      		bbc_leader_city_name
		from csx_analyse_tmp.customer_sale_service_manager_leader01
		where
			sdt in ('20230930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and sale_amt !=0
;

