--华西低毛利商品
-- 蔬菜低于5% 牛羊低于2% 猪肉低于5% 目前可以先按照这三类来出
with sale AS (
select performance_province_name,
    performance_city_name,
    classify_middle_name,
    customer_code,
    customer_name,
    goods_code,
    goods_name,
    sum(sale_qty) sale_qty,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(profit)/sum(sale_amt) profit_rate
from   csx_dws.csx_dws_sale_detail_di a
where business_type_code = 1
    and sdt='20240509'
    and performance_region_name = '华西大区'
    and delivery_type_code<>2 -- 剔除直送
    and order_channel_code=1  -- 取B端面，剔除返利、调价、补救
    and refund_order_flag=0   -- 取正向单
GROUP BY performance_province_name,
    performance_city_name,
    classify_middle_name,
    customer_code,
    customer_name,
    goods_code,
    goods_name
)
select a.performance_province_name,
    performance_city_name,
    classify_middle_name,
    customer_code,
    customer_name,
    b.rp_service_user_name_new
    goods_code,
    goods_name,
    sale_qty,
    sale_amt,
    profit,
    profit_rate
from sale a
    left join (
        select customer_no,
            rp_service_user_name_new
        from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
        where sdt = '20240509'
    )b on a.customer_code=b.customer_no
    where ( classify_middle_name='蔬菜' and profit_rate<0.05)
    or (classify_middle_name='牛羊' and profit_rate<0.02)
    or (classify_middle_name='猪肉' and profit_rate<0.05)