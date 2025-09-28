
-- -- -- -- -- -- -  服务管家-- -客服经理-- -- 高级客服经理
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


------------对账 
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
		sum(bill_amt) bill_amt_all,-- 对账金额
		sum(sale_amt) sale_amt_all -- 财务含税销售额
	
		--  invoice_amount_all/sale_amt_all as kp_ratio
		--  statement_ratio,-- 对账率
		--  kp_ratio,-- 开票率
		--  sdt
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20230910','20230810','20231017')
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
    bbc_leader_city_name;

--  对账明细
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
		sum(bill_amt) bill_amt_all,-- 对账金额
		sum(sale_amt) sale_amt_all, -- 财务含税销售额
	
		--  invoice_amount_all/sale_amt_all as kp_ratio
		--  statement_ratio,-- 对账率
		--  kp_ratio,-- 开票率
		 sdt
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
	-- 
		sdt in ('20230910','20230810','20231017')
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
	and sale_amt_all !=0;
	
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


---------- 回款率汇总
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
where sdt in ('20230731','20230930','20230930')
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
    bbc_leader_city_name;	
-- -- - 回款率明细
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
where sdt in ('20230731','20230930','20230930')
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
	and receivable_amount_target !=0; 

--  季度实际新签合同金额
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
        ) c on a.customer_code=c.customer_code ;

-- -- -- -- -- -- -- -- -- -- -- 汇总

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
       c.supervisor_user_name;
	   
------------------------------- 客服经理、高级客服经理
-- -- -- -- -- -- -- -- -- -- - 履约金额
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
	  a.smonth ;
-- -- -- -- -- -- -- -- -- -- -销售侧 履约金额
select
    a.province_name      sales_province_name,
    a.city_group_name,
    c.province_manager_user_number fourth_supervisor_work_no,
    c.province_manager_user_name fourth_supervisor_name,
    c.city_manager_user_number third_supervisor_work_no,
    c.city_manager_user_name third_supervisor_name, 
    c.sales_manager_user_number second_supervisor_work_no,
    c.sales_manager_user_name second_supervisor_name, 
    c.supervisor_user_number first_supervisor_work_no,
    c.supervisor_user_name first_supervisor_name,   
    c.sales_user_number,
	c.sales_user_name,
    a.customer_code customer_no,
    c.customer_name,
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
LEFT join
          (
            select *
            from csx_dim.csx_dim_crm_customer_info
            where sdt= '20230930'
           and channel_code  in ('1','7','9')
          ) c on a.customer_code=c.customer_code 
group by 
      a.province_name,
      a.city_group_name,
      c.province_manager_user_number ,
      c.province_manager_user_name ,
      c.city_manager_user_number ,
      c.city_manager_user_name , 
      c.sales_manager_user_number ,
      c.sales_manager_user_name , 
      c.supervisor_user_number ,
      c.supervisor_user_name,
	  c.sales_user_number,
	  c.sales_user_name,
      a.customer_code,
      c.customer_name,
      a.business_type_name,
	  a.smonth ;
		 
-- -- -- -- -- -- -- 商品质量客诉率-DC 客诉率		 
select *,
  nvl(kesu,0)/nvl(a.skuall,0)  kesu_lv
from (select
*,

sum(sku) over(partition by smonth,performance_region_name,performance_province_name,performance_city_name) skuall
from 
(  
select
  a.smonth,
  --  a.performance_region_code,
  a.performance_region_name,
  --  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
    a.inventory_dc_code,
  --  a.inventory_dc_name,
  if(a.classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货'),'生鲜','食百') as  classify_name,
  a.classify_middle_name,
  sum(nvl(b.kesu,0)) kesu,
  sum(nvl(a.sku,0)) sku

from 
(
select
  smonth,
  --  performance_region_code,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  --  IF(performance_province_name='浙江省',performance_city_name,performance_province_name) performance_province_name,
   inventory_dc_code,inventory_dc_name,
  classify_middle_code,
  classify_middle_name,
  sum(sku) sku
from (
select 
  substr(sdt,1,6) smonth,
 performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,inventory_dc_name,
  a.classify_middle_code,
  c.classify_middle_name,
  a.order_code,
  count(distinct a.goods_code) sku
from csx_dws.csx_dws_sale_detail_di a
left join
  (
    select
      goods_code,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
   ) c on c.goods_code = a.goods_code
where  sdt>='20230701' and sdt<='20230930'
      	and channel_code in('1','7','9')
		and business_type_code not in(4,6) --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 --  1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 --  退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
group by substr(sdt,1,6) ,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,inventory_dc_name,
  a.classify_middle_code,
  c.classify_middle_name,
  a.order_code
  )a 
group by smonth,
 --  performance_region_code,
  performance_region_name,
 --  performance_province_code,
 performance_province_name,
 performance_city_name,
   --  IF(performance_province_name='浙江省',performance_city_name,performance_province_name),
   inventory_dc_code,inventory_dc_name,
  classify_middle_code,classify_middle_name	
	) a
left join 
(
  select
    substr(sdt,1,6) smonth,        --  客诉日期
	performance_province_name,
      inventory_dc_code,
    if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code) classify_middle_code,
    count(distinct complaint_code)  kesu -- - 客诉单量
  from csx_analyse.csx_analyse_fr_oms_complaint_detail_di
  where  sdt>='20230701' and sdt<='20230930'
        and complaint_status_code in (20,30)  --  客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
		and complaint_deal_status in (10,40) --  责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and second_level_department_name !=''
		and first_level_department_name='采购'
  group by  substr(sdt,1,6),        --  客诉日期
          inventory_dc_code,
         if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code),performance_province_name
  )b on a.inventory_dc_code=b.inventory_dc_code and a.smonth=b.smonth and a.classify_middle_code=b.classify_middle_code 
        and a.performance_province_name=b.performance_province_name
group by  a.smonth,
  --  a.performance_region_code,
  a.performance_region_name,
  --  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
    a.inventory_dc_code,
  --  a.inventory_dc_name,
  if(a.classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货'),'生鲜','食百'),
  a.classify_middle_name)a 
  )a
;

----------- 采购部门绩效考核：采购占比2023Q4开始使用
----------- 生鲜全国采购占比
select
   a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
   a.classify_middle_name,
   dc_code,
   nvl(amtfenzi,0),
   receive_amt,
   if(receive_amt=0,0,nvl(amtfenzi,0)/receive_amt) zhanbi
from (
select
   a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
   a.classify_middle_name,
     a.dc_code,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,performance_city_name,
   dc_code,
   case when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊','米','蛋','熟食烘焙','干货加工') then classify_middle_name
	 else '食百' end  as classify_middle_name,
   csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
   is_central_tag, --品类+供应商集采  
   sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20230701' and sdt<='20230930'
  AND order_code like 'IN%'
group by performance_region_name,
   performance_province_name,performance_city_name,
   dc_code,
   -- classify_middle_code,
   case when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊','米','蛋','熟食烘焙','干货加工') then classify_middle_name
	 else '食百' end,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag

)a 
left join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
where a.classify_middle_name<>'食百'
group by a.performance_region_name,
   a.performance_province_name,a.performance_city_name,
  a.dc_code,
   a.classify_middle_name
union all   
 -------------  食百
 -- 1.采购占比食百修改
 select
   a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
  a.dc_code,
 --  a.classify_middle_code,
   a.classify_middle_name,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,
   performance_city_name,
   dc_code,
   'B00'  as classify_middle_code,
   '食百'  as classify_middle_name,
   csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
   if(supplier_code in  
          ('20054206','20058482','20055149','20058420','20054478','20057039','20055847','20057161','20028053','20054206','20055149',
          '20058420','20054478','20057039','20039468','20027660','20059618','20049739','20059006','20035903','20056817','20032305',
          '20042204','20054206','20055149','20058420','20054478','20057039','20016117','20040455','20025727','20034091','20030254',
          '20034539','20051022','20059017','20039079','20059123','20035648','20029968','20022637','20051729','200428','20032963',
          '20050591','20049986','20034257','20050612','20038485','125526FZ','20034929','20036699','20023896','20035092','20046667',
          '20024998','20026373','20020561','20035723','20046873','20032995','20032779','20017602','20035727','20056045','20051435',
          '20059131','20053435','X00016','20041413','20044104','20035749','20032958','20055689','20044532','20034040','201538','X00667',
          '20048472','20029808','20051055','20035641','20054206','20055149','20058420','20054478','20057039','20058964','20055789',
          '20055793','20054206','20055149','20058420','20054478','20057039','20060600','20060936','20060131','20055800','20054206',
          '20055149','20058420','20054478','20057039','20057555','20056056','20059481','20056009','20051682','20054206','20055149',
          '20058420','20054478','20057039','20060297','20056195','20053968','20054206','20055149','20058420','20054478','20057039',
          '20058882','20055624','20055687','20058482','20054206','20055149','20058420','20054478','20057039','20052108','20043433',
          '20056766','20060590','20043203','20054594','20058482','20054206','20055149','20054478','20057039','20015198','20058483',
          '20040569','20043620','20051102','20042440','20042593','20042858','20038363','20054501','20032908','20060131','20056731',
          '20041373','20040910','20020331','20051090','20043203','20054594','20054206','20055149','20058420','20054478','20057039',
          '20054206','20055149','20058420','20054478','20057039','20043747','20045257','20052997','20051075','20055950','20006126',
          '20042553','20046634','20058482','20054206','20055149','20058420','20054478','20057039','20015198','20041536','20056731',
          '20006870','20056465','20058345','20058420','20054206','20055149','20054478','20057039','20050477','20047689','20050317',
          '20060648','20055750','20047878','20060682','20051410','20046854','20042176','20029976','20055891','20045573','20058482',
          '20055149','20058420','20054478','20057039','20015198','20047725','20041311','20058323','20058751','20056311','20056359',
          '20053074','20043965','20057951','20047739','20060424','20044770','20055284','20060425','20040892','20041228','20038251',
          '20055311','20058872','20058817','20045251','20041365','20060277','20013388','20054206','20054206','20051865','20016978',
          '20055149','20058420','20054478','20057039','20044284','20047210','20038363','20055111','20043753','20060633','20055653',
          '20017531','20057187','20055827','20024248','20055827','20055430','20038484','20055908','20016994','20043687','20034350',
          '20056540','20045301','20035724','20042327','20050894','20054721','20042321'),'1','0')
      as is_central_tag, -- 品类+供应商集采
     sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20230701' and sdt<='20230930'
  AND order_code like 'IN%'
 and classify_middle_name in 
    ('酒','香烟饮料','休闲食品','面类/米粉类','调味品类','食用油类','罐头小菜','早餐冲调','常温乳品饮料','冷藏冷冻食品','清洁用品','纺织用品','家电','文体用品','家庭用品','易耗品','服装')
group by performance_region_name,
   performance_province_name,performance_city_name,
   dc_code,
   classify_middle_code,
   classify_middle_name,
   csx_purchase_level_code,
   order_business_type,
   if(supplier_code in  
          ('20054206','20058482','20055149','20058420','20054478','20057039','20055847','20057161','20028053','20054206','20055149',
          '20058420','20054478','20057039','20039468','20027660','20059618','20049739','20059006','20035903','20056817','20032305',
          '20042204','20054206','20055149','20058420','20054478','20057039','20016117','20040455','20025727','20034091','20030254',
          '20034539','20051022','20059017','20039079','20059123','20035648','20029968','20022637','20051729','200428','20032963',
          '20050591','20049986','20034257','20050612','20038485','125526FZ','20034929','20036699','20023896','20035092','20046667',
          '20024998','20026373','20020561','20035723','20046873','20032995','20032779','20017602','20035727','20056045','20051435',
          '20059131','20053435','X00016','20041413','20044104','20035749','20032958','20055689','20044532','20034040','201538','X00667',
          '20048472','20029808','20051055','20035641','20054206','20055149','20058420','20054478','20057039','20058964','20055789',
          '20055793','20054206','20055149','20058420','20054478','20057039','20060600','20060936','20060131','20055800','20054206',
          '20055149','20058420','20054478','20057039','20057555','20056056','20059481','20056009','20051682','20054206','20055149',
          '20058420','20054478','20057039','20060297','20056195','20053968','20054206','20055149','20058420','20054478','20057039',
          '20058882','20055624','20055687','20058482','20054206','20055149','20058420','20054478','20057039','20052108','20043433',
          '20056766','20060590','20043203','20054594','20058482','20054206','20055149','20054478','20057039','20015198','20058483',
          '20040569','20043620','20051102','20042440','20042593','20042858','20038363','20054501','20032908','20060131','20056731',
          '20041373','20040910','20020331','20051090','20043203','20054594','20054206','20055149','20058420','20054478','20057039',
          '20054206','20055149','20058420','20054478','20057039','20043747','20045257','20052997','20051075','20055950','20006126',
          '20042553','20046634','20058482','20054206','20055149','20058420','20054478','20057039','20015198','20041536','20056731',
          '20006870','20056465','20058345','20058420','20054206','20055149','20054478','20057039','20050477','20047689','20050317',
          '20060648','20055750','20047878','20060682','20051410','20046854','20042176','20029976','20055891','20045573','20058482',
          '20055149','20058420','20054478','20057039','20015198','20047725','20041311','20058323','20058751','20056311','20056359',
          '20053074','20043965','20057951','20047739','20060424','20044770','20055284','20060425','20040892','20041228','20038251',
          '20055311','20058872','20058817','20045251','20041365','20060277','20013388','20054206','20054206','20051865','20016978',
          '20055149','20058420','20054478','20057039','20044284','20047210','20038363','20055111','20043753','20060633','20055653',
          '20017531','20057187','20055827','20024248','20055827','20055430','20038484','20055908','20016994','20043687','20034350',
          '20056540','20045301','20035724','20042327','20050894','20054721','20042321'),'1','0')
)a 
 join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
group by a.performance_region_name,
   a.performance_province_name,a.performance_city_name,
   a.classify_middle_name,a.dc_code) a ;

---------------- 待发货需求sql历史版本230927
select  
 supplier_code,purchase_org_code,
 business_owner_name
from csx_dim.csx_dim_basic_supplier_purchase
where sdt='current' 
and business_owner_name<>''


CREATE  TABLE `csx_dws_bbc_wshop_shipped_order_goods`(
  `id` bigint COMMENT '主键', 
  `order_code` varchar(64) COMMENT '订单编号', 
  `source_order_code` varchar(64) COMMENT '原订单编号', 
  `customer_code` varchar(64) COMMENT '客户编码', 
  `customer_name` varchar(64) COMMENT '客户名称', 
  `performance_region_code` varchar(64) COMMENT '业绩大区编码', 
  `performance_region_name` varchar(64) COMMENT '业绩大区名称', 
  `performance_province_code` varchar(64) COMMENT '业绩省区编码', 
  `performance_province_name` varchar(64) COMMENT '业绩省区名称', 
  `performance_city_code` varchar(64) COMMENT '业绩城市编码', 
  `performance_city_name` varchar(64) COMMENT '业绩城市名称', 
  `first_category_code` varchar(64) COMMENT '一级客户分类编码', 
  `first_category_name` varchar(64) COMMENT '一级客户分类名称', 
  `second_category_code` varchar(64) COMMENT '二级客户分类编码', 
  `second_category_name` varchar(64) COMMENT '二级客户分类名称', 
  `third_category_code` varchar(64) COMMENT '三级客户分类编码', 
  `third_category_name` varchar(64) COMMENT '三级客户分类名称', 
  `paid_time` timestamp COMMENT '支付时间', 
  `send_plan_time` timestamp COMMENT '预计发货时间', 
  `vip_flag` int COMMENT 'vip标识 0-普通订单 1-vip订单', 
  `finish_flag` int COMMENT '发货完成标志 0-否 1-是', 
  `order_phone_number` varchar(64) COMMENT '下单手机号', 
  `agreement_dc_code` varchar(64) COMMENT '履约地点编码', 
  `agreement_dc_name` varchar(64) COMMENT '履约地点名称', 
  `site_code` varchar(64) COMMENT '站点编码', 
  `site_name` varchar(64) COMMENT '站点名称', 
  `inventory_dc_code` varchar(64) COMMENT '库存地点编码', 
  `inventory_dc_name` varchar(64) COMMENT '库存地点名称', 
  `goods_code` varchar(64) COMMENT '商品编码', 
  `goods_name` varchar(64) COMMENT '商品名称', 
  `purchase_group_code` varchar(64) COMMENT '采购组(课组)编码', 
  `purchase_group_name` varchar(64) COMMENT '采购组(课组)名称', 
  `division_code` varchar(64) COMMENT '部类编号', 
  `division_name` varchar(64) COMMENT '部类描述', 
  `classify_large_code` varchar(64) COMMENT '管理大类编号', 
  `classify_large_name` varchar(64) COMMENT '管理大类名称', 
  `classify_middle_code` varchar(64) COMMENT '管理中类编号', 
  `classify_middle_name` varchar(64) COMMENT '管理中类名称', 
  `classify_small_code` varchar(64) COMMENT '管理小类编号', 
  `classify_small_name` varchar(64) COMMENT '管理小类名称', 
  `replace_goods_code` varchar(64) COMMENT '原商品编码', 
  `supplier_code` varchar(64) COMMENT '供应商编码', 
  `supplier_name` varchar(64) COMMENT '供应商名称', 
  `operation_mode_code` int COMMENT '经营方式编码：0-自营；1-联营', 
  `operation_mode_name` varchar(64) COMMENT '经营方式名称', 
  `supplier_channel` varchar(64) COMMENT '供应商渠道', 
  `goods_amt` decimal(10,2) COMMENT '商品金额', 
  `order_qty` decimal(10,2) COMMENT '下单数量', 
  `shipped_qty` decimal(10,2) COMMENT '出库数量', 
  `lack_qty` decimal(10,2) COMMENT '缺货数量', 
  `cancel_qty` decimal(10,2) COMMENT '取消数量', 
  `not_shipped_qty` decimal(10,2) COMMENT '未发货数量', 
  `create_time` timestamp COMMENT '创建时间', 
  `create_by` varchar(64) COMMENT '创建人', 
  `update_time` timestamp COMMENT '更新时间', 
  `update_by` varchar(64) COMMENT '更新人', 
  `task_sync_time` timestamp COMMENT '任务同步时间',
   `sdt` varchar(64) COMMENT '创建日期{"yyyymmdd"}',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=523 DEFAULT CHARSET=utf8mb4 COMMENT='待发货商品出库信息表';



csx_basic_data.md_shop_info
csx_crm_prod.customer_info
csx_basic_data.md_supplier_info
csxprd_common.wshop_shipped_order_goods
csxprd_common.wshop_shipped_order_goods
csx_ods.csx_ods_csxprd_common_wshop_shipped_order_goods_di

csx_dws.csx_dws_bbc_wshop_shipped_order_goods_di

select
a.*,b.supplier_name,c.customer_name,d.purchase_org,concat(a.supplier_code,d.purchase_org) code,d.org_short_name
from (
select
     order_code
     ,src_order_code
     ,replace_goods_code
     ,supplier_code
     ,customer_code
     ,supplier_channel
     ,dc_code
     ,site_code
     ,site_name
     ,inventory_dc
     ,if(business_style=0,'自营','联营') as business_style
     ,sum(goods_amount)      goods_amount
     ,sum(goods_count)       goods_count
     ,sum(out_count)         out_count
     ,sum(outstock_count)    outstock_count
     ,sum(cancel_count)      cancel_count
     ,sum(not_shipped_count) not_shipped_count
     ,pay_time
     ,mention_date
     ,if(vip_flag=0,'普通订单','vip订单') vip_flag
     ,user_telephone
     ,create_time
     ,create_by
     ,update_time
     ,update_by
FROM csxprd_common.wshop_shipped_order_goods
WHERE finish_flag = 0
and 
SUBSTRING(create_time,1,10)>='${sdate}' and SUBSTRING(create_time,1,10)<='${edate}'
${if(len(dc)==0,"","AND inventory_dc in( '"+dc+"') ")}
${if(len(gys)==0,"","AND supplier_code in( '"+gys+"') ")}
group by order_code
     ,src_order_code
     ,replace_goods_code
     ,supplier_code
     ,customer_code
     ,supplier_channel
     ,dc_code
     ,site_code
     ,site_name
     ,inventory_dc
     ,business_style
	 ,pay_time
     ,mention_date
     ,if(vip_flag=0,'普通订单','vip订单') 
     ,user_telephone
     ,create_time
     ,create_by
     ,update_time
     ,update_by)a
left join (select supplier_code,supplier_name from csx_basic_data.md_supplier_info) b on b.supplier_code=a.supplier_code
left join (select customer_number,customer_name from csx_crm_prod.customer_info where customer_number<>'') c on c.customer_number=a.customer_code
left join (SELECT location_code,purchase_org,org_short_name from csx_basic_data.md_shop_info)d 	 on d.location_code=a.dc_code;








