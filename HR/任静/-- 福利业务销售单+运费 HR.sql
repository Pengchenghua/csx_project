-- 福利BBC业务销售单+运费 HR
create table csx_analyse_tmp.csx_analyse_tmp_sale_order as 
select sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name, 
    order_code_new,
    business_type_code,
    business_type_name,
    customer_code,
    customer_name,
    sale_amt,
    profit,
    sale_cost
from (
        select
            sale_month,
            performance_region_name,
            performance_province_name,
            performance_city_name, 
            order_code_new,
            business_type_code,
            business_type_name,
            customer_code,
            customer_name,
            (sale_amt) as sale_amt,
            (profit) as profit,
            (sale_cost) as sale_cost,
            row_number() over(
                partition by order_code_new
                order by customer_code,
                    business_type_code asc
            ) as rank_num
        from (
                select -- order_code,
                    performance_region_name,
                    performance_province_name,
                    performance_city_name,
                    substr(sdt,1,6) as sale_month,
                    case
                        when business_type_code = '6'
                        and substr(split(order_code, '-') [0], 1, 1) = 'B'
                        and substr(split(order_code, '-') [0], -1, 1) in ('A', 'B', 'C', 'D', 'E') then substr(
                            split(order_code, '-') [0],
                            2,
                            length(split(order_code, '-') [0]) -2
                        )
                        when business_type_code = '6'
                        and substr(split(order_code, '-') [0], 1, 1) = 'B'
                        and substr(split(order_code, '-') [0], -1, 1) not in ('A', 'B', 'C', 'D', 'E') then substr(
                            split(order_code, '-') [0],
                            2,
                            length(split(order_code, '-') [0]) -1
                        )
                        else split(order_code, '-') [0]
                    end as order_code_new,
                    business_type_code,
                    business_type_name,
                    customer_code,
                    customer_name,
                    sum(sale_amt) as sale_amt,
                    sum(profit ) as profit,
                    sum(sale_cost) as sale_cost
                from csx_dws.csx_dws_sale_detail_di
                where sdt >= '20240101'
                    and business_type_code in('2','6')
                group by performance_region_name,
                    performance_province_name,
                    performance_city_name,
                    substr(sdt,1,6),
                    case
                        when business_type_code = '6'
                        and substr(split(order_code, '-') [0], 1, 1) = 'B'
                        and substr(split(order_code, '-') [0], -1, 1) in ('A', 'B', 'C', 'D', 'E') then substr(
                            split(order_code, '-') [0],
                            2,
                            length(split(order_code, '-') [0]) -2
                        )
                        when business_type_code = '6'
                        and substr(split(order_code, '-') [0], 1, 1) = 'B'
                        and substr(split(order_code, '-') [0], -1, 1) not in ('A', 'B', 'C', 'D', 'E') then substr(
                            split(order_code, '-') [0],
                            2,
                            length(split(order_code, '-') [0]) -1
                        )
                        else split(order_code, '-') [0]
                    end ,
                    business_type_code,
                    business_type_name,
                    customer_code,
                    customer_name
            ) a
    ) a
where rank_num = 1

;
create table csx_analyse_tmp.csx_analyse_tmp_sale_order_transport as  

		select regexp_replace(send_date,'-','') as sdt,
			customer_code as customer_no, -- dc_code,
			access_caliber,
			shipped_order_code,
			shipped_type_code,
			sum(total_amount_tax_encluded) as transport_amount      -- 未税运费
		from csx_dws.csx_dws_tms_entrucking_order_detail_di a
		left semi
		join 
			(
			select * 
			from csx_dwd.csx_dwd_tms_entrucking_order_di
			where status_code != 100
			)b on a.entrucking_code = b.entrucking_code             
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
		and regexp_replace(send_date,'-','')>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
		and regexp_replace(send_date,'-','')<='${sdt_yes}'
		and access_caliber<>3
		and customer_code<>''
		group by regexp_replace(send_date,'-',''),customer_code,access_caliber,shipped_order_code,shipped_type_code
    
	;
drop table csx_analyse_tmp.csx_analyse_tmp_sale_transport ;
-- 福利BBC运费
create table csx_analyse_tmp.csx_analyse_tmp_sale_transport as  

		select regexp_replace(send_date,'-','') as sdt,
			customer_code as customer_no, -- dc_code,
			shipped_order_code,
            sum( transport_amount ) as transport_amount,                -- 订单含税运费
			sum( excluding_tax_avg_transport_amount ) as no_tax_transport_amount      -- 订单未税运费

		from csx_dws.csx_dws_tms_entrucking_order_detail_di a
		left semi
		join 
			(
			select entrucking_code
			from csx_dwd.csx_dwd_tms_entrucking_order_di
			where status_code != 100
            GROUP BY
                     entrucking_code
			)b on a.entrucking_code = b.entrucking_code             
		where sdt>='20240101'
		and access_caliber<>3
		group by regexp_replace(send_date,'-',''),customer_code,access_caliber,shipped_order_code,shipped_type_code

		-- 客户上月至今每日的运费 BBC  签收时间signing_time改 	账单所属期间结束bill_belongs_end
	  union all
	  select 
         regexp_replace(substr(bill_belongs_end,1,10),'-','') as sdt,
          customer_code customer_no,
          merchant_order_number shipped_order_code,
	      sum(cast(settlement_amount as decimal(20,6))) as transport_amount,      -- 含税运费	结算金额
		  sum(cast(settlement_amount_no_tax as decimal(20,6))) as no_tax_transport_amount     -- bbc 未税字段
	  from csx_report.csx_report_tms_transport_bbc_expense_detail
	  where sdt='20240710'
	  group by regexp_replace(substr(bill_belongs_end,1,10),'-','')  ,
          customer_code ,
         merchant_order_number 
        
-- 	  and regexp_replace(substr(bill_belongs_end,1,10),'-','')>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
-- 	  and regexp_replace(substr(bill_belongs_end,1,10),'-','')<='${sdt_yes}'
;

	'OM24012400004847'

	select  sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name, 
    order_code_new,
    business_type_code,
    business_type_name,
    customer_code,
    customer_name,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(sale_cost) sale_cost,
    -- shipped_order_code,
    -- shipped_type_code,
    SUM(transport_amount)transport_amount,
    sum(no_tax_transport_amount) as no_tax_transport_amount 
    from csx_analyse_tmp.csx_analyse_tmp_sale_order  a 
	left join 
	  csx_analyse_tmp.csx_analyse_tmp_sale_transport b on a.order_code_new=b.shipped_order_code
	where sale_month>='202401'
	 and  customer_no <>''
	GROUP BY  sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name, 
    order_code_new,
    business_type_code,
    business_type_name,
    customer_code,
    customer_name