--客服经理绩效数据
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
      			sdt,customer_no,
				customer_name,
				region_name,
				province_name,
				city_group_name,
				sales_name,         -- 系统维护
				user_position,
				sales_name_new,
				user_position_new,
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
      			sdt in ('20240630')
      			)a
      left join
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20240630'
      ) b on a.rp_service_user_id_new=b.id
      
      left join 
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20240630'
      ) c on a.fl_service_user_id_new=c.id
      
      left join 
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20240630'
      ) d on a.bbc_service_user_id_new=d.id
      )a 
      
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20240630'
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) b on a.rp_leader_id=b.id
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20240630'
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) c on a.fl_leader_id=c.id
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt='20240630'
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) d on a.bbc_leader_id=d.id
 )a 
left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt='20240630'
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) b on a.rp_city_leader_id=b.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt='20240630'
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) c on a.fl_city_leader_id=c.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt='20240630'
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) d on a.bbc_city_leader_id=d.id;

   
------------------------------- 客服经理、高级客服经理 销售明细
--  销售明细
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
           where sdt >='20240401'  and sdt <= '20240630'
		   -- and customer_code not in ('120459','121206')
			    and business_type_code in ('1','2','4','6') 
				and channel_code in ('1','7','9') 
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
			sdt in ('20240630')
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

   
------------------------------- 客服经理、高级客服经理 B端销售数据
with aa as (select
    a.province_name,
    a.city_group_name,
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
    business_type_name,
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
           where sdt >='20240401'  and sdt <= '20240630'
		   -- and customer_code not in ('120459','121206')
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
			sdt in ('20240630')
		) b on a.customer_code=b.customer_no 
group by 
      a.province_name,
      a.city_group_name,
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
    bbc_leader_city_name,
    business_type_name
	  ) 
	  select 
    aa.province_name,
    aa.city_group_name,
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
    bbc_leader_city_name,
    business_type_name,
    sale_amt,
  --  profit,
    profit/sale_amt profit_rate
    -- ,sum(sale_amt)over(partition by rp_leader_user_number,city_group_name) rp_sale_amt,
    -- sum(profit)over(partition by rp_leader_user_number,city_group_name) rp_profit,
    -- sum(profit)over(partition by rp_leader_user_number,city_group_name)/ sum(sale_amt)over(partition by rp_leader_user_number,city_group_name) rp_profit_rate
    from aa ;

-- 对帐率


------------对账 
select
	--  substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	city_group_name,
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
	-- 取次月10号（采用考核季度内每月截止15号的季度数据，若1-15号有5天及以上节假日，则截止日期顺延）
		sdt in ('20240520','20240615','20240715')
	group by customer_code,company_code,company_name
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,
			region_name,
			province_name,
			city_group_name,
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
			sdt in ('20240630')
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
    bbc_leader_city_name,
	city_group_name;




--  对账明细
select
	  substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	city_group_name,
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
		sdt in ('20240520','20240615','20240715')
	group by customer_code,company_code,company_name,sdt
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
				-- sales_name,         -- 系统维护
				-- user_position,
				-- sales_name_new,
				-- user_position_new,
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
			sdt in ('20240630')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and sale_amt_all !=0;
	

-- 开票汇总

-- 任静 23年12月取24年1月11号，其他还是取15号，
--  开票汇总
select

	b.province_name as province_name,
    b.city_group_name as city_group_name,
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
		customer_code,
		company_code,
		company_name,
		sum(sale_amt) sale_amt_all,-- 财务含税销售额
		sum(invoice_amount) invoice_amount_all -- 开票金额
	
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
    -- 次月20日
		sdt in ('20240525','20240620','20240720')
	group by  customer_code,
		company_code,
		company_name
	) a 
	join
		(
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
				-- sales_name,         -- 系统维护
				-- user_position,
				-- sales_name_new,
				-- user_position_new,

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
			sdt in ('20240630')
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
    bbc_leader_city_name,
	city_group_name;


    
--  开票明细
select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	city_group_name,
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
		customer_code,
		company_code,
		company_name,
		invoice_amount,-- 开票金额
		sale_amt,-- 财务含税销售额
		invoice_amount/sale_amt as kp_ratio,
		sdt
	from
		--  csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt in ('20240525','20240620','20240720')
	) a 
	join
		(
		select distinct 
			sdt,
			customer_no,
			customer_name,
				-- sales_name,         -- 系统维护
				-- user_position,
				-- sales_name_new,
				-- user_position_new,
			region_name,
			province_name,
			city_group_name,
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
			sdt in ('20240630')
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
	sum(current_receivable_amount*-1) current_receivable_amount,
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
where sdt in ('20240430','20240531','20240630')
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
			sdt in ('20240630')
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
-- 	sales_name,         -- 系统维护
-- 	user_position,
-- 	sales_name_new,
-- 	user_position_new,
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
	current_receivable_amount*-1 current_receivable_amount,	
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
where sdt in ('20240430','20240531','20240630')
group by 	company_code,		--  公司代码
	company_name,		--  公司名称
	customer_code,		--  客户编码
	customer_name,substr(sdt,1,6)
	)a 
join 
   (
		select distinct 
			sdt,customer_no,customer_name,region_name,province_name,city_group_name,
-- 			sales_name,         -- 系统维护
-- 				user_position,
-- 				sales_name_new,
-- 				user_position_new,
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
		where 1=1
		--	sdt in ('20240630')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
	where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	and receivable_amount_target !=0; 


-- A类客户按照等级
with tmp_level_a as 
(select
  province_code,
  customer_no,
  city_group_name
from
  csx_analyse.csx_analyse_report_sale_customer_level_mf
where
  month in ( '202404','202405','202406')
  AND customer_large_level = 'A'
  AND TAG = 1
group by 
 province_code,
  customer_no,
  city_group_name
  )
  
SELECT
	b.province_name,
	b.city_group_name,
	a.customer_no,
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
from  tmp_level_a a
left join (select 
              distinct customer_code
            from   csx_dws.csx_dws_sale_detail_di
            where sdt >='20240401'  and sdt <= '20240630'
			and business_type_code='1' -- and channel_code in ('1','7','9') 
			AND  order_channel_code<>4
            )c	on  a.customer_no=c.customer_code 					
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
			sdt in ('20240930')
		) b on a.customer_no=b.customer_no
;



-- A类客户明细
-- select * from  csx_tmp.tmp_xiaoshou_ratio
-- A类客户
drop table csx_analyse_tmp.csx_analyse_tmp_kf_xiaoshou_ratio;
CREATE  table csx_analyse_tmp.csx_analyse_tmp_kf_xiaoshou_ratio
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
            where sdt >='20240101'  and sdt <= '20240331'
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
                          where sdt= '20240630'
							and channel_code  in ('1','7','9')
                        ) c on a.customer_code=c.customer_code 	
            where a.sdt >='20240101'  and a.sdt <= '20240331'
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
            where sdt >='20240401'  and sdt <= '20240630'
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
			sdt in ('20240630')
		) b on a.customer_code=b.customer_no
;


select * from csx_analyse_tmp.csx_analyse_tmp_kf_xiaoshou_ratio

;


-- 


-- A类断约汇总
select  province_name,
       city_group_name,
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
      		bbc_leader_city_name,
		   count(customer_code) all_cn,
	   count(case when typeder='断约客户' then  customer_code end ) cust_cn
from csx_analyse_tmp.csx_analyse_tmp_kf_xiaoshou_ratio
group by province_name,
       city_group_name,
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

