

drop table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale;
create  table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale
as
select
	a.performance_province_name,
	substr(a.sdt,1,6) as smonth,
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code,
	sum(sale_amt) as sale_amt
from
	(
	select
		performance_province_name,business_type_name,customer_code,inventory_dc_code,delivery_type_name,sale_amt,sdt,order_code
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230701' and sdt<='20230731'
		and channel_code  in ('1','7','9')
		and business_type_code =1
		and delivery_type_code in (2) -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and performance_region_name in ('华北大区')
		and customer_code in ('106775','128359','116383','130239','120771','111608','103207','223402','223283','127634','226207','224684','105696','130215','131427',
		'108734','107428','170451','226241','130504','127391','113980','106287','129671','129581','126377','130061','105695','178615','129695'
)
	) a 	
    left join 
		( 
        select
            distinct shop_code 
        from 
			csx_dim.csx_dim_shop 
        where 
			sdt='current' 
			and shop_low_profit_flag=1  
        )c on a.inventory_dc_code = c.shop_code
	left join  
	   (
		select
			customer_code,customer_name
		from  
			csx_dim.csx_dim_crm_customer_info 
		where 
			sdt='current'               
		)d on d.customer_code=a.customer_code
where
	c.shop_code is null 
group by 
	a.performance_province_name,
	substr(a.sdt,1,6),
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code
;

select * from csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale
	

drop table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale_2;
create  table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale_2
as
select
	a.performance_province_name,
	substr(a.sdt,1,6) as smonth,
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code,
	sum(sale_amt) as sale_amt
from
	(
	select
		performance_province_name,business_type_name,customer_code,inventory_dc_code,delivery_type_name,sale_amt,sdt,order_code
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230601' and sdt<='20230731'
		and channel_code  in ('1','7','9')
		and business_type_code =1
		and order_channel_code=6
		and performance_region_name in ('华北大区')
		and customer_code in ('128359','130928','120054','118318','129695','126377','122471','130504','106775','129022','176574','127634','105687','106287','226207','223402',
		'105628','114494','105575'
		)
	) a 	
    left join 
		( 
        select
            distinct shop_code 
        from 
			csx_dim.csx_dim_shop 
        where 
			sdt='current' 
			and shop_low_profit_flag=1  
        )c on a.inventory_dc_code = c.shop_code
	left join  
	   (
		select
			customer_code,customer_name
		from  
			csx_dim.csx_dim_crm_customer_info 
		where 
			sdt='current'               
		)d on d.customer_code=a.customer_code
where
	c.shop_code is null 
group by 
	a.performance_province_name,
	substr(a.sdt,1,6),
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code
;

select * from csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale_2

-- 河北
drop table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale;
create  table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale
as
select
	a.performance_province_name,
	substr(a.sdt,1,6) as smonth,
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code,
	a.goods_code,
	a.goods_name,
	sum(sale_amt) as sale_amt
from
	(
	select
		performance_province_name,business_type_name,customer_code,inventory_dc_code,delivery_type_name,sale_amt,sdt,order_code,goods_code,goods_name
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230501' and sdt<='20230731'
		and channel_code  in ('1','7','9')
		and business_type_code =1
		and delivery_type_code in (2) -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and performance_region_name in ('华北大区')
		and customer_code in ('131187','131162','131129','123035','129008','128631','112024','125854','131146','130674','166952','128587','229846','129762','127266'
)
	) a 	
    left join 
		( 
        select
            distinct shop_code 
        from 
			csx_dim.csx_dim_shop 
        where 
			sdt='current' 
			and shop_low_profit_flag=1  
        )c on a.inventory_dc_code = c.shop_code
	left join  
	   (
		select
			customer_code,customer_name
		from  
			csx_dim.csx_dim_crm_customer_info 
		where 
			sdt='current'               
		)d on d.customer_code=a.customer_code
where
	c.shop_code is null 
group by 
	a.performance_province_name,
	substr(a.sdt,1,6),
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code,
	a.goods_code,
	a.goods_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale
	

drop table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale_2;
create  table csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale_2
as
select
	a.performance_province_name,
	substr(a.sdt,1,6) as smonth,
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code,
	sum(sale_amt) as sale_amt
from
	(
	select
		performance_province_name,business_type_name,customer_code,inventory_dc_code,delivery_type_name,sale_amt,sdt,order_code
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230601' and sdt<='20230731'
		and channel_code  in ('1','7','9')
		and business_type_code =1
		and order_channel_code=6
		and performance_region_name in ('华北大区')
		and customer_code in ('128359','130928','120054','118318','129695','126377','122471','130504','106775','129022','176574','127634','105687','106287','226207','223402',
		'105628','114494','105575'
		)
	) a 	
    left join 
		( 
        select
            distinct shop_code 
        from 
			csx_dim.csx_dim_shop 
        where 
			sdt='current' 
			and shop_low_profit_flag=1  
        )c on a.inventory_dc_code = c.shop_code
	left join  
	   (
		select
			customer_code,customer_name
		from  
			csx_dim.csx_dim_crm_customer_info 
		where 
			sdt='current'               
		)d on d.customer_code=a.customer_code
where
	c.shop_code is null 
group by 
	a.performance_province_name,
	substr(a.sdt,1,6),
	a.business_type_name,
	a.delivery_type_name,
	a.inventory_dc_code,
	a.customer_code,
	d.customer_name,
	a.order_code
;

select * from csx_analyse_tmp.csx_analyse_tmp_zs_customer_order_sale_2
















		
直送原因
RD/ZZ
客户临时加单
客户指定供应商
供应商到货质量差
供应商缺货
供应商迟到
对账差异
接单漏单
其他（这个需要写下备注）


调价原因
报价错误-报价失误
报价错误-报价客户不认可
发货后报价类型
客户对账差异-其他
客户对账差异-税率调整
后端履约问题-其他
后端履约问题-商品等级/规格未达要求
后端履约问题-商品质量问题折扣处理
其他-手动备注		
		
select
   a.smonth,
        a.province_name,
        a.city_group_name,
        a.business_type_name,
        a.customer_code,
        d.customer_name,
        d.second_category_name,
    b.classify_middle_code,
    b.classify_middle_name,
        sales_type,
        fanli_type,
        delivery_type_name,
        types,
        c.first_sales_date,
        if(substr(c.first_sales_date,1,6)=a.smonth,'新客','老客') as xinlaok,
        sum(a.sales_value) by_sales_value,
        sum(a.sale_qty) by_sale_qty,
        sum(a.profit) by_profit        
from 
  (
        select 
                performance_province_name province_name,
        performance_city_name city_group_name
           ,substr(sdt,1,6) smonth,

                business_type_name,a.business_type_code,
                customer_code,
                if(order_channel_code=6 ,'是','否') sales_type
                ,if(order_channel_code=4 ,'是','否') fanli_type
                ,if(delivery_type_name='直送','是','否') delivery_type_name
                ,goods_code,

                if( c.shop_code is null,'否','是') types,
                sum(sale_amt)as sales_value,
                sum(profit)as profit,                
                sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty
        from (
               select 
                     * 
               from csx_dws.csx_dws_sale_detail_di 
               where 
                  sdt >='20220101' and sdt<='20230731'
                  and channel_code in('1','7','9')
                  ) a
    left join ( 
                    select
                   distinct shop_code 
                                from csx_dim.csx_dim_shop 
                                where sdt='current' and shop_low_profit_flag=1  
                          )c
               on a.inventory_dc_code = c.shop_code
    group by performance_province_name,
             performance_city_name,
                     substr(sdt,1,6),
                         business_type_name,business_type_code,
                     customer_code,

                     if( c.shop_code is null,'否','是')
                     ,if(order_channel_code=6 ,'是','否')
                         ,if(order_channel_code=4 ,'是','否')
                     ,delivery_type_name,
                     goods_code
 )a  
left join 
 (
   select 
     *  
   from  csx_dim.csx_dim_basic_goods 
   where sdt = 'current'
 ) b on b.goods_code = a.goods_code 
left join  -- 首单日期
(
  select 
    customer_code,
        business_type_code,
        min(first_business_sale_date) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' -- and         business_type_code in (1,2)
  group by customer_code,
           business_type_code
)c on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code
left join  
   (
         select
                customer_code,
                customer_name,second_category_name
         from  csx_dim.csx_dim_crm_customer_info 
         where sdt='current'               
        )d on d.customer_code=a.customer_code
group by    a.smonth,a.province_name,
        a.city_group_name,
        a.business_type_name,
        a.customer_code,
        d.customer_name,
        d.second_category_name,
    b.classify_middle_code,
    b.classify_middle_name,
        sales_type,
        fanli_type,
        delivery_type_name,
        types,












