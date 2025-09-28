议题一：客户建议售价样本定义
    在2021/1/1之后的的销售数据中，按以下规则筛选样本，后续再分析讨论样本数量/样本覆盖率/筛选条件是否需要调整
        筛选 group by【日期+同个省区+同客户三级行业分类+sku】加点中位数报价以上 
        加点中位数按照销量加权先计算单个客户在周期内的加点加权平均数，再取group by范围内同一客户分类+sku的加点中位数 
        二级分类也可单独筛选一版看看
        奇数偶数逻辑：按照品类、sku分类聚合取中位数后，统计中位数的个数，个数为奇数的取中间，偶数取中间偏小的那个
        样本分析：取出来的样本，观察取到/没取到的客户&品类销量占比，毛利率，下单频率，行业等维度进行对比分析；

--20210823更新
1、加销售额占比 降序排列
2、剔除退货对应原单
3、加订单+商品的占比 （数据量）
4、中位数价格下浮0.5%

-- 昨日、上月昨日
--select ${hiveconf:current_day},${hiveconf:before1_last_mon};
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set before1_last_mon =regexp_replace(add_months(date_sub(current_date,1),-1),'-','');



--临时表1：各省区客户三级行业-商品的售价中位数
drop table csx_tmp.tmp_third_category_sales_price_percentile;
create temporary table csx_tmp.tmp_third_category_sales_price_percentile
as
select concat_ws('-',province_name,second_category_code,third_category_code,goods_code) as id,
  region_name,province_name,second_category_code,third_category_code,goods_code,sales_price*0.995 as sales_price,cnt
from 
(
  select 
    concat_ws('-',a.province_name,a.customer_no,b.second_category_code,b.third_category_code,a.goods_code) as id,
    --regexp_replace(split(shipped_time, ' ')[0], '-', '') shipped_date,
	a.region_name,a.province_name,a.customer_no,b.second_category_code,b.third_category_code,a.goods_code,
    cast(sum(a.sales_price*a.sales_qty)/sum(a.sales_qty) as decimal(30,6)) sales_price,   --含税售价 客户商品加权平均价
	row_number() over(partition by a.province_name,b.second_category_code,b.third_category_code,a.goods_code order by cast(sum(a.sales_price*a.sales_qty)/sum(a.sales_qty) as decimal(30,6)) ) num,
	count(*) over(partition by a.province_name,b.second_category_code,b.third_category_code,a.goods_code ) cnt
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
	and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )a 
  left join 
  (
    select customer_no,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no
  left join   --退货单的原单号
  (
    select distinct origin_order_no,order_no
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and return_flag='X'
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )c on a.order_no=c.origin_order_no 
  where c.order_no is null
  group by concat_ws('-',a.province_name,a.customer_no,b.second_category_code,b.third_category_code,a.goods_code),
  a.region_name,a.province_name,a.customer_no,b.second_category_code,b.third_category_code,a.goods_code
)a
--如果是奇数，取排序中间的，如果是偶数，取取中间偏小的那个 
where if(cnt%2=0,num=cnt/2,num=(cnt+1)/2);


--临时表2：各省区客户二级行业-商品的售价中位数
drop table csx_tmp.tmp_second_category_sales_price_percentile;
create temporary table csx_tmp.tmp_second_category_sales_price_percentile
as
select concat_ws('-',province_name,second_category_code,goods_code) as id,
  region_name,province_name,second_category_code,goods_code,sales_price*0.995 as sales_price,cnt
from 
(
  select 
    concat_ws('-',a.province_name,a.customer_no,b.second_category_code,a.goods_code) as id,
    --regexp_replace(split(shipped_time, ' ')[0], '-', '') shipped_date,
	a.region_name,a.province_name,a.customer_no,b.second_category_code,a.goods_code,
    cast(sum(a.sales_price*a.sales_qty)/sum(a.sales_qty) as decimal(30,6)) sales_price,   --含税售价 客户商品加权平均价
	row_number() over(partition by a.province_name,b.second_category_code,a.goods_code order by cast(sum(a.sales_price*a.sales_qty)/sum(a.sales_qty) as decimal(30,6)) ) num,
	count(*) over(partition by a.province_name,b.second_category_code,a.goods_code ) cnt
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
	and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )a 
  left join 
  (
    select customer_no,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no
  left join   --退货单的原单号
  (
    select distinct origin_order_no,order_no
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and return_flag='X'
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )c on a.order_no=c.origin_order_no 
  where c.order_no is null  
  group by concat_ws('-',a.province_name,a.customer_no,b.second_category_code,a.goods_code),
  a.region_name,a.province_name,a.customer_no,b.second_category_code,a.goods_code
)a
--如果是奇数，取排序中间的，如果是偶数，取两个中间的均值 where if(cnt%2=0,num in(cnt/2,cnt/2+1),num=(cnt+1)/2)
--如果是奇数，取排序中间的，如果是偶数，取取中间偏小的那个 
where if(cnt%2=0,num=cnt/2,num=(cnt+1)/2);

--结果表1：客户+商品明细数据
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select a.region_name,a.province_name,b.classify_middle_code,b.classify_middle_name,
  a.customer_no,a.customer_name,a.second_category_code,a.second_category_name,a.third_category_code,a.third_category_name,a.goods_code,b.goods_name,
  a.sales_qty,a.sales_price,
  c.sales_price as second_median_sales_price,d.sales_price as third_median_sales_price
from
(
  select 
	a.region_name,a.province_name,a.customer_no,b.customer_name,
	b.second_category_code,b.second_category_name,b.third_category_code,b.third_category_name,a.goods_code,
	sum(a.sales_qty) sales_qty,
    cast(sum(a.sales_price*a.sales_qty)/sum(a.sales_qty) as decimal(30,6)) sales_price   --含税售价 客户商品加权平均价
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
	and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )a 
  left join 
  (
    select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no
  left join   --退货单的原单号
  (
    select distinct origin_order_no,order_no
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and return_flag='X'
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )c on a.order_no=c.origin_order_no 
  where c.order_no is null  
  group by 
  a.region_name,a.province_name,a.customer_no,b.customer_name,
  b.second_category_code,b.second_category_name,b.third_category_code,b.third_category_name,a.goods_code
)a 
left join 
(
  select goods_id,regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    department_id,department_name,classify_middle_code,classify_middle_name		 
  from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
)b on b.goods_id=a.goods_code
left join csx_tmp.tmp_second_category_sales_price_percentile c 
  on c.province_name=a.province_name and c.second_category_code=a.second_category_code and c.goods_code=a.goods_code
left join csx_tmp.tmp_third_category_sales_price_percentile d 
  on d.province_name=a.province_name and d.second_category_code=a.second_category_code and d.third_category_code=a.third_category_code and d.goods_code=a.goods_code
;


--样本分析：取出来的样本，观察取到/没取到的客户&品类销量占比，毛利率，下单频率，行业等维度进行对比分析；
--结果表2：省区+品类 中位数售价以上的
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t'
select a.region_name,a.province_name,b.classify_middle_code,b.classify_middle_name,
  a.second_category_code,a.second_category_name,a.third_category_code,a.third_category_name,
  count(1) counts,  --数据量
  count(distinct a.customer_no) count_cust,
  sum(a.sales_value) sales_value,
  sum(a.profit) profit,
  sum(a.front_profit) front_profit,
  
  count(case when a.sales_price>=c.sales_price then a.customer_no end) counts_second,
  count(distinct case when a.sales_price>=c.sales_price then a.customer_no end) count_cust_second,
  sum(case when a.sales_price>=c.sales_price then a.sales_value end) sales_value_second,
  sum(case when a.sales_price>=c.sales_price then a.profit end) profit_second,
  sum(case when a.sales_price>=c.sales_price then a.front_profit end) front_profit_second,
  
  count(case when a.sales_price>=d.sales_price then a.customer_no end) counts_third,
  count(distinct case when a.sales_price>=d.sales_price then a.customer_no end) count_cust_third,
  sum(case when a.sales_price>=d.sales_price then a.sales_value end) sales_value_third,
  sum(case when a.sales_price>=d.sales_price then a.profit end) profit_third,
  sum(case when a.sales_price>=d.sales_price then a.front_profit end) front_profit_third
from
(
  select regexp_replace(split(shipped_time, ' ')[0], '-', '') shipped_date,
	a.region_name,a.province_name,a.customer_no,b.customer_name,
	b.second_category_code,b.second_category_name,b.third_category_code,b.third_category_name,a.goods_code,
	a.sales_qty,a.sales_price,a.sales_value,a.profit,a.front_profit
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
	and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )a 
  left join 
  (
    select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no	
  left join   --退货单的原单号
  (
    select distinct origin_order_no,order_no
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and return_flag='X'
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )c on a.order_no=c.origin_order_no 
  where c.order_no is null  
)a 
left join 
(
  select goods_id,regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    department_id,department_name,classify_middle_code,classify_middle_name		 
  from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
)b on b.goods_id=a.goods_code
left join csx_tmp.tmp_second_category_sales_price_percentile c 
  on c.province_name=a.province_name and c.second_category_code=a.second_category_code and c.goods_code=a.goods_code
left join csx_tmp.tmp_third_category_sales_price_percentile d 
  on d.province_name=a.province_name and d.second_category_code=a.second_category_code and d.third_category_code=a.third_category_code and d.goods_code=a.goods_code
group by a.region_name,a.province_name,b.classify_middle_code,b.classify_middle_name,
a.second_category_code,a.second_category_name,a.third_category_code,a.third_category_name
;


--结果表3：省区+2级行业 中位数售价以上的
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t'
select a.region_name,a.province_name,
  a.second_category_code,a.second_category_name,
  count(1) counts,  --数据量
  count(distinct a.customer_no) count_cust,
  sum(a.sales_value) sales_value,
  sum(a.profit) profit,
  sum(a.front_profit) front_profit,
  
  count(case when a.sales_price>=c.sales_price then a.customer_no end) counts_second,
  count(distinct case when a.sales_price>=c.sales_price then a.customer_no end) count_cust_second,
  sum(case when a.sales_price>=c.sales_price then a.sales_value end) sales_value_second,
  sum(case when a.sales_price>=c.sales_price then a.profit end) profit_second,
  sum(case when a.sales_price>=c.sales_price then a.front_profit end) front_profit_second
from
(
  select regexp_replace(split(shipped_time, ' ')[0], '-', '') shipped_date,
	a.region_name,a.province_name,a.customer_no,b.customer_name,
	b.second_category_code,b.second_category_name,b.third_category_code,b.third_category_name,a.goods_code,
	a.sales_qty,a.sales_price,a.sales_value,a.profit,a.front_profit
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
	and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )a 
  left join 
  (
    select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no	
  left join   --退货单的原单号
  (
    select distinct origin_order_no,order_no
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and return_flag='X'
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )c on a.order_no=c.origin_order_no 
  where c.order_no is null  
)a 
--left join 
--(
--  select goods_id,regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
--    department_id,department_name,classify_middle_code,classify_middle_name		 
--  from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
--)b on b.goods_id=a.goods_code
left join csx_tmp.tmp_second_category_sales_price_percentile c 
  on c.province_name=a.province_name and c.second_category_code=a.second_category_code and c.goods_code=a.goods_code
--left join csx_tmp.tmp_third_category_sales_price_percentile d 
--  on d.province_name=a.province_name and d.second_category_code=a.second_category_code and d.third_category_code=a.third_category_code and d.goods_code=a.goods_code
group by a.region_name,a.province_name,
a.second_category_code,a.second_category_name
;

--结果表4：省区+3级行业 中位数售价以上的
insert overwrite directory '/tmp/raoyanhua/linshi04' row format delimited fields terminated by '\t'
select a.region_name,a.province_name,
  a.second_category_code,a.second_category_name,a.third_category_code,a.third_category_name,
  count(1) counts,  --数据量
  count(distinct a.customer_no) count_cust,
  sum(a.sales_value) sales_value,
  sum(a.profit) profit,
  sum(a.front_profit) front_profit,
  
  count(case when a.sales_price>=d.sales_price then a.customer_no end) counts_third,
  count(distinct case when a.sales_price>=d.sales_price then a.customer_no end) count_cust_third,
  sum(case when a.sales_price>=d.sales_price then a.sales_value end) sales_value_third,
  sum(case when a.sales_price>=d.sales_price then a.profit end) profit_third,
  sum(case when a.sales_price>=d.sales_price then a.front_profit end) front_profit_third
from
(
  select regexp_replace(split(shipped_time, ' ')[0], '-', '') shipped_date,
	a.region_name,a.province_name,a.customer_no,b.customer_name,
	b.second_category_code,b.second_category_name,b.third_category_code,b.third_category_name,a.goods_code,
	a.sales_qty,a.sales_price,a.sales_value,a.profit,a.front_profit
  from 
  (
    select *
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and (return_flag<>'X' or return_flag is null)   --剔除退货单
	and substr(order_no,1,1)<>'R' --差异单补救单也会是R开头，剔除
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )a 
  left join 
  (
    select customer_no,customer_name,second_category_code,second_category_name,third_category_code,third_category_name
    from csx_dw.dws_crm_w_a_customer
    where sdt='current'
  )b on a.customer_no=b.customer_no	
  left join   --退货单的原单号
  (
    select distinct origin_order_no,order_no
    from csx_dw.dws_sale_r_d_detail
    where sdt>${hiveconf:before1_last_mon}
	and sdt<=${hiveconf:current_day}
    and channel_code in('1','7','9')
    and business_type_code='1'
    and return_flag='X'
    and sales_type<>'fanli'
    and customer_no not in ('103097', '103903','104842')
  )c on a.order_no=c.origin_order_no 
  where c.order_no is null  
)a 
--left join 
--(
--  select goods_id,regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
--    department_id,department_name,classify_middle_code,classify_middle_name		 
--  from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
--)b on b.goods_id=a.goods_code
--left join csx_tmp.tmp_second_category_sales_price_percentile c 
--  on c.province_name=a.province_name and c.second_category_code=a.second_category_code and c.goods_code=a.goods_code
left join csx_tmp.tmp_third_category_sales_price_percentile d 
  on d.province_name=a.province_name and d.second_category_code=a.second_category_code 
     and d.third_category_code=a.third_category_code and d.goods_code=a.goods_code
group by a.region_name,a.province_name,
a.second_category_code,a.second_category_name,a.third_category_code,a.third_category_name
;


