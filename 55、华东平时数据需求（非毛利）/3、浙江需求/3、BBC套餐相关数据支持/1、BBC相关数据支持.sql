select 
    a.customer_code as `客户编码`,
    max(a.customer_name) as `客户名称`,
    a.user_name as `下单客户名称`,
    a.user_telephone as `下单客户手机号`,
    a.assemble_goods_code as `套餐编码`,
    a.assemble_goods_name as `套餐名称`,
    a.goods_code as `商品编码`,
    b.goods_name as `商品名称`,
    sum(a.sale_amt) as `销售额`,
    sum(a.sale_qty) as `销量` 
from 
(select * 
from csx_dws.csx_dws_bbc_sale_detail_di  
where sdt>='20241001' 
and sdt<='20250228' 
and customer_code='236634'  
and length(assemble_goods_code)>0 
) a 
left join 
(select * 
from csx_dim.csx_dim_basic_goods 
where sdt='current'
) b 
on a.goods_code=b.goods_code 
group by 
    a.customer_code,
    a.user_name,
    a.user_telephone,
    a.assemble_goods_code,
    a.assemble_goods_name,
    a.goods_code,
    b.goods_name