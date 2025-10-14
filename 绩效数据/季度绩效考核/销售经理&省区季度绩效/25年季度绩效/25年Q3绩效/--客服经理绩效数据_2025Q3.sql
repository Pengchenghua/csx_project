--客服经理绩效数据
-- -- -- -- -- -- -  服务管家-- -客服经理-- -- 高级客服经理
drop table csx_analyse_tmp.customer_sale_service_manager_leader01;
create  table csx_analyse_tmp.customer_sale_service_manager_leader01
as 
select
			substr(sdt,1,6) as months,
			sdt,
			customer_no,
			customer_name,
			region_name,
			province_name,
			city_group_name,
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
      			sdt,
				customer_no,
				customer_name,
				region_name,
				province_name,
				city_group_name,
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
      			sdt in ('20250930')
    	)a
      left join
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt=regexp_replace(date_sub(current_date(),1),'-','')
      ) b on split(a.rp_service_user_id_new,'、')[0]=b.id
      
      left join 
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt=regexp_replace(date_sub(current_date(),1),'-','')
      ) c on split(a.fl_service_user_id_new,'、')[0]=c.id
      
      left join 
      (
      select id,leader_id
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt=regexp_replace(date_sub(current_date(),1),'-','')
      ) d on split(a.bbc_service_user_id_new,'、')[0]=d.id
      )a 
      
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt=regexp_replace(date_sub(current_date(),1),'-','')
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) b on a.rp_leader_id=b.id
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt=regexp_replace(date_sub(current_date(),1),'-','')
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) c on a.fl_leader_id=c.id
      
      left join 
      (
      select id,leader_id,user_number,name
      from csx_ods.csx_ods_csx_b2b_ucenter_user_df
      where sdt=regexp_replace(date_sub(current_date(),1),'-','')
      and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
      ) d on a.bbc_leader_id=d.id
 )a 
left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt=regexp_replace(date_sub(current_date(),1),'-','')
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) b on a.rp_city_leader_id=b.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt=regexp_replace(date_sub(current_date(),1),'-','')
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) c on a.fl_city_leader_id=c.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt=regexp_replace(date_sub(current_date(),1),'-','')
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) d on a.bbc_city_leader_id=d.id
;
   

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
    sum(excluding_tax_profit) excluding_tax_profit,
    sum(if( partner_type_code in (1, 3),sale_amt,0)) partner_sale_amt,
    sum(if( partner_type_code in (1, 3),profit,0)) partner_profit,
    sum(if( partner_type_code in (1, 3),excluding_tax_sales,0)) partner_excluding_tax_sales,
    sum(if( partner_type_code in (1, 3),excluding_tax_profit,0)) partner_excluding_tax_profit
from   (
          select 
                customer_code,
				performance_province_name province_name,
				performance_city_name city_group_name,
				substr(sdt,1,6) smonth,
				business_type_name,
				partner_type_code,
				sum(sale_amt) as sale_amt,
				sum(profit) profit,
				sum(profit_no_tax) excluding_tax_profit,
				sum(sale_amt_no_tax) excluding_tax_sales
           from   csx_dws.csx_dws_sale_detail_di
           where sdt >='20250701'  and sdt <= '20250930'
			    and (business_type_code in ('1','2','6','10')  
						or inventory_dc_code  in ('WD75', 'WD76', 'WD77', 'WD78', 'WD79', 'WD80', 'WD81')
              		)
				-- and partner_type_code not in (1, 3)
		   group by customer_code,performance_province_name,performance_city_name,substr(sdt,1,6),business_type_name,partner_type_code
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
			sdt in ('20250930')
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
    profit/sale_amt profit_rate,
    partner_sale_amt,
    partner_profit,
    partner_profit/partner_sale_amt partner_profit_rate
    -- ,sum(sale_amt)over(partition by rp_leader_user_number,city_group_name) rp_sale_amt,
    -- sum(profit)over(partition by rp_leader_user_number,city_group_name) rp_profit,
    -- sum(profit)over(partition by rp_leader_user_number,city_group_name)/ sum(sale_amt)over(partition by rp_leader_user_number,city_group_name) rp_profit_rate
    from aa ;


    
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
    sum(excluding_tax_profit) excluding_tax_profit,
    sum(if( partner_type_code in (1, 3),sale_amt,0)) partner_sale_amt,
    sum(if( partner_type_code in (1, 3),profit,0)) partner_profit,
    sum(if( partner_type_code in (1, 3),excluding_tax_sales,0)) partner_excluding_tax_sales,
    sum(if( partner_type_code in (1, 3),excluding_tax_profit,0)) partner_excluding_tax_profit
from   (
          select 
                customer_code,
				performance_province_name province_name,
				performance_city_name city_group_name,
				substr(sdt,1,6) smonth,
				business_type_name,
				partner_type_code,
				sum(sale_amt) as sale_amt,
				sum(profit) profit,
				sum(profit_no_tax) excluding_tax_profit,
				sum(sale_amt_no_tax) excluding_tax_sales
           from   csx_dws.csx_dws_sale_detail_di
           where sdt >='20250701'  and sdt <= '20250930'
			    and (business_type_code in ('1','2','6','10')  
						or inventory_dc_code  in ('WD75', 'WD76', 'WD77', 'WD78', 'WD79', 'WD80', 'WD81')
              		)
				-- and partner_type_code not in (1, 3)
		   group by customer_code,performance_province_name,performance_city_name,substr(sdt,1,6),business_type_name,partner_type_code
		)a			
join 
   (
		select distinct 
			sdt,customer_no,
			customer_name,
			region_name,province_name,
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
			sdt in ('20250930')
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
---------- 回款率汇总

with tmp_sap as
(select
    a.smonth,
	b.province_name as province_name,
	b.city_group_name,
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

    sum(receivable_amount_target) receivable_amount_target,  		--  回款目标:取1号预测回款金额
    sum(current_receivable_amount) current_receivable_amount		--  当期回款金额		
   -- sum(if(current_receivable_amount<0,-current_receivable_amount,0))/sum(receivable_amount_target) back_rate,
from csx_analyse.csx_analyse_fr_sap_customer_credit_forecast_collection_report_df  --  承华  预测回款金额-帆软
-- 注意日期每个月最后一天
where sdt in ('20250731','20250831','20250930')
	and channel_name not in ('项目供应商','前置仓') 
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
		--	sdt in ('20250930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
	where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
)
---------- 回款率汇总
select
	province_name as province_name,
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
    sum(if(receivable_amount_target<=0,0,(current_receivable_amount)))/sum(if(receivable_amount_target<=0,0,receivable_amount_target)) as receivable_rate -- 回款率
from tmp_sap
group by province_name,
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
	b.city_group_name,
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
-- 注意日期每个月最后一天
where sdt in ('20250731','20250831','20250930')
	and channel_name not in ('项目供应商','前置仓') 
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
		--	sdt in ('20250930')
			-- and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no
	where
	(b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null)
	-- and receivable_amount_target !=0
	; 


-- AB类客户按照等级销售经理A类客户 
-- AB类客户明细

-- A类客户按照等级销售经理A类客户 
-- A类客户明细
with   tmp_customer_level as (select
  region_name,
  province_name,
  city_group_name,
  customer_no,
  customer_name,
  first_category_name,
  second_category_name,
  last_order_date,			-- 最后一次下单时间 需要注意这个是全业务的，下季度要调整20250731
  work_no,
  sales_name,
  sales_value,
  profit,
  profit_rate,
  customer_large_level,
  customer_small_level,
  customer_level_tag,
  if_new_order_cus,
  first_order_date,
  quarter
from
    csx_analyse.csx_analyse_report_sale_customer_level_qf
where
  tag = '2'
  and quarter = '20252'
  and customer_large_level in ('A','B')
  ),
tmp_sale_detail as   ( 
    SELECT
        customer_code,
        MAX(sdt) AS max_sdt,
        DATEDIFF('2025-09-30', FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(sdt), 'yyyyMMdd'), 'yyyy-MM-dd')) AS diff_days
    FROM csx_dws.csx_dws_sale_detail_di
    WHERE sdt >= '20250701' AND sdt <= '20250930'
      AND business_type_code = '1'
      AND order_channel_code <> 4
    GROUP BY customer_code
)
,
tmp_terminate_info as 
(select customer_code,terminate_date,
row_number()over(partition by customer_code order by terminate_date desc ) as rn from 
(select customer_code,terminate_date from csx_dim.csx_dim_crm_terminate_customer  
    where sdt='current' 
        and business_attribute_code=1
        and status=2
 union all 
 select customer_code,terminate_date from csx_dim.csx_dim_crm_terminate_customer_attribute  
    where sdt='current'  
        and business_attribute_code=1
        and approval_status=2
) a 
)
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
		   count(customer_no) all_cn,
	   	   count(case when typeder='断约客户' then  customer_no end ) cust_cn
from 
(select  
	 a.*,
    coalesce(c.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(c.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(c.rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(c.rp_leader_name,'') as rp_leader_name,
	coalesce(c.rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(c.rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(c.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(c.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(c.fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(c.fl_leader_name,'') as fl_leader_name,
	coalesce(c.fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(c.fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(c.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(c.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(c.bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(c.bbc_leader_name,'') as bbc_leader_name,
	coalesce(c.bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(c.bbc_leader_city_name,'') as bbc_leader_city_name,
     
    IF(d.customer_code IS not  NULL and b.max_sdt is not null , '断约客户', NULL) AS typeder
from tmp_customer_level a 
left join tmp_sale_detail b on a.customer_no=b.customer_code
left join tmp_terminate_info d on a.customer_no=d.customer_code
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
			sdt in ('20250930')
		) c on a.customer_no=c.customer_no
	)a group by province_name,
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

-- A类客户按照等级销售经理A类客户 
-- A类客户明细

-- A类客户按照等级销售经理A类客户 
-- A类客户明细
with   tmp_customer_level as (select
  region_name,
  province_name,
  city_group_name,
  customer_no,
  customer_name,
  first_category_name,
  second_category_name,
  last_order_date,			-- 最后一次下单时间 需要注意这个是全业务的，下季度要调整20250731
  work_no,
  sales_name,
  sales_value,
  profit,
  profit_rate,
  customer_large_level,
  customer_small_level,
  customer_level_tag,
  if_new_order_cus,
  first_order_date,
  quarter
from
    csx_analyse.csx_analyse_report_sale_customer_level_qf
where
  tag = '2'
  and quarter = '20252'
  and customer_large_level in ('A','B')
  ),
tmp_sale_detail as   ( 
    SELECT
        customer_code,
        MAX(sdt) AS max_sdt,
        DATEDIFF('2025-09-30', FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(sdt), 'yyyyMMdd'), 'yyyy-MM-dd')) AS diff_days
    FROM csx_dws.csx_dws_sale_detail_di
    WHERE sdt >= '20250701' AND sdt <= '20250930'
      AND business_type_code = '1'
      AND order_channel_code <> 4
    GROUP BY customer_code
)
,
tmp_terminate_info as 
(select customer_code,terminate_date,
row_number()over(partition by customer_code order by terminate_date desc ) as rn from 
(select customer_code,terminate_date from csx_dim.csx_dim_crm_terminate_customer  
    where sdt='current' 
        and business_attribute_code=1
        and status=2
 union all 
 select customer_code,terminate_date from csx_dim.csx_dim_crm_terminate_customer_attribute  
    where sdt='current'  
        and business_attribute_code=1
        and approval_status=2
) a 
)
select  
	 a.*,
    coalesce(c.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(c.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(c.rp_leader_user_number,'') as rp_leader_user_number,
	coalesce(c.rp_leader_name,'') as rp_leader_name,
	coalesce(c.rp_leader_city_user_number,'') as rp_leader_city_user_number,
    coalesce(c.rp_leader_city_name,'') as rp_leader_city_name,
	coalesce(c.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(c.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(c.fl_leader_user_number,'') as fl_leader_user_number,
	coalesce(c.fl_leader_name,'') as fl_leader_name,
	coalesce(c.fl_leader_city_user_number,'') as fl_leader_city_user_number,
    coalesce(c.fl_leader_city_name,'') as fl_leader_city_name,
	coalesce(c.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(c.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	coalesce(c.bbc_leader_user_number,'') as bbc_leader_user_number,
	coalesce(c.bbc_leader_name,'') as bbc_leader_name,
	coalesce(c.bbc_leader_city_user_number,'') as bbc_leader_city_user_number,
    coalesce(c.bbc_leader_city_name,'') as bbc_leader_city_name,
    b.max_sdt,
    d.terminate_date,
    IF(d.customer_code IS not  NULL and b.max_sdt is not null , '断约客户', NULL) AS typeder
from tmp_customer_level a 
left join tmp_sale_detail b on a.customer_no=b.customer_code
left join tmp_terminate_info d on a.customer_no=d.customer_code
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
			sdt in ('20250930')
		) c on a.customer_no=c.customer_no
  

