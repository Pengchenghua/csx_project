-- --------------------------------------------------------------------------------------------------
-- -------------------------------------
-- ------客户商品目前生效价格
drop table if exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month as 
select 
    a.* 
from 
    (select 
        warehouse_code as dc_code,
        customer_code,
        product_code as goods_code,
        customer_price,
        (case when price_type=1 then '建议售价' 
             when price_type=2 then '对标对象' 
             when price_type=3 then '销售成本价' 
             when price_type=4 then '上一周价格' 
             when price_type=5 then '售价' 
             when price_type=6 then '采购/库存成本价' 
             when price_type=7 then '上期价格' 
        else price_type end) as price_type,
        row_number()over(partition by warehouse_code,customer_code,product_code order by create_time desc) as pm  
    from csx_dwd.csx_dwd_price_customer_price_guide_di 
    where effective='1' 
    and length(sub_customer_code)=0 
    -- and warehouse_code in ('W0A3','W0R9') 
    ) a 
where a.pm=1 
;

-- --------------------------------------------------------------------------------------------------
-- -------------------------------------
-- ------子客户商品目前生效价格
drop table if exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month_sub;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_month_now_customer_price_month_sub as 
select 
    a.* 
from 
    (select 
        warehouse_code as dc_code,
        customer_code,
        sub_customer_code,
        product_code as goods_code,
        customer_price,
        (case when price_type=1 then '建议售价' 
             when price_type=2 then '对标对象' 
             when price_type=3 then '销售成本价' 
             when price_type=4 then '上一周价格' 
             when price_type=5 then '售价' 
             when price_type=6 then '采购/库存成本价' 
             when price_type=7 then '上期价格' 
        else price_type end) as price_type,
        row_number()over(partition by warehouse_code,customer_code,sub_customer_code,product_code order by create_time desc) as pm  
    from csx_dwd.csx_dwd_price_customer_price_guide_di 
    where effective='1' 
    and length(sub_customer_code)>0 
    ) a 
where a.pm=1 
-- and warehouse_code in ('W0A3','W0R9') 
;