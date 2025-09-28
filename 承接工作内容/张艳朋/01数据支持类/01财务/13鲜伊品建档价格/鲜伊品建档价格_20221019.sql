select
    substr(sdt,1,6) as smonth,
    goods_code,
    goods_name,
    sum(excluding_tax_sales)
from
    csx_dw.dws_sale_r_d_detail
where 
	sdt>='20220601' and sdt<='20220930'
	and channel_code in('1','7','9')
	and goods_code in ('1505791','1505804','1505745','1505626','1505772','1505840','1505754','1505744','1505850')
group by 
    1,2,3
