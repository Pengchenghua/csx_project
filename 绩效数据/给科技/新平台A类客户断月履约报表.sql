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