-- TOP客户TOP品类毛利率
select customer_code,concat_ws(',',collect_set(top3)) top3
from (
select customer_code,
	classify_middle_code,
	classify_middle_name,
	concat('Top',mclass_rank,classify_middle_name,' 毛利率',	concat( round(profit/sale_amt*100,2) ,'%')) as top3,
	profit/sale_amt profit_rate,
	mclass_rank
from 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	sum(sale_amt) sale_amt,
	sum(profit) profit,	
	--  客户品类销售排名
	row_number() over(partition by customer_code order by sum(sale_amt) desc) as mclass_rank	
from csx_analyse.csx_analyse_bi_sale_detail_di a
where sdt >= '20231101'
and sdt <= '20240226' 
and a.business_type_code=1 
and shop_low_profit_flag !=1 
and  inventory_dc_code not in('W0J2','W0K9','W0G6','W0AJ','W0AL','WB71','W0AX','W0BD','W0T0')  
group by 
	customer_code,
	classify_middle_code,
	classify_middle_name
)a  where mclass_rank<4
)a group by customer_code