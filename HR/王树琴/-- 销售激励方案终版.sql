-- 销售激励方案终版王树琴
      select
        
        substr(sdt, 1, 6) month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
         business_type_name,
        customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        rp_service_user_work_no,		
	rp_service_user_name,
        goods_code,
        goods_name,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit
      from
         csx_dws.csx_dws_sale_detail_di a 
        left join 
        (select customer_no,rp_service_user_work_no,		
	rp_service_user_name
	from  csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi 
	where smt='202509') b on a.customer_code=b.customer_no
      where
        sdt >= '20250901'
        and sdt <= '20250930'
        -- and channel_code in ('1', '7', '9')
        and business_type_code in ('1') 
        and goods_code in ('2059618',
                            '2060343',
                            '2074890',
                            '2068674',
                            '2070673',
                            '2068674',
                            '2070673',
                            '1191891',
                            '1527939',
                            '2080079',
                            '2083482',
                            '2058632',
                            '2058691',
                            '2058467',
                            '2058373'
                            )--  and performance_region_name in ('华南大区', '华北大区', '华西大区', '华东大区', '华中大区')
      group by
        substr(sdt, 1, 6),
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        goods_code,
        goods_name,
        business_type_name,rp_service_user_work_no,		
	rp_service_user_name
        ;

      select
        
        substr(sdt, 1, 6) month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
         business_type_name,
        customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
--         rp_service_user_work_no,		
-- 	rp_service_user_name,
        goods_code,
        goods_name,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit
      from
         csx_dws.csx_dws_sale_detail_di a 
        left join 
        (select customer_no,rp_service_user_work_no,		
	rp_service_user_name
	from  csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi 
	where smt='202509') b on a.customer_code=b.customer_no
      where
        sdt >= '20250901'
        and sdt <= '20250930'
        -- and channel_code in ('1', '7', '9')
        and business_type_code in ('2','6','10') 
        and goods_code in ('2080079',
'2083482',
'2057285',
'833280',
'903539',
'2051943',
'2053838',
'2071196',
'2086441',
'2086452',
'2086472',
'2086453',
'2086276',
'2086369',
'2086316',
'2063394',
'2068083',
'2067288',
'2067963',
'2068281',
'570987'    )--  and performance_region_name in ('华南大区', '华北大区', '华西大区', '华东大区', '华中大区')
      group by
        substr(sdt, 1, 6),
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        goods_code,
        goods_name,
        business_type_name
        -- rp_service_user_work_no,		
-- 	rp_service_user_name