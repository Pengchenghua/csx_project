--==============================================================================================
set sdt='20210705';     -- 库存日期
set dc=('W0A6');        -- 需要稽核DC
 
 --临保商品、过保商品
--逻辑说明 ： 未次入库天数大于保质期天数
--商品最后一次入库天数>保质期天数（主数据保质期），保质期过四分之三且库存金额>500元以上
--商品最后一次入库天数>保质期天数（主数据保质期），保质期过了
select 
	sales_province_code,sales_province_name,dc_code,dc_name,a.goods_id,bar_code,--条码
    a.goods_name,a.unit_name,standard,classify_middle_code,classify_middle_name,department_id,
    department_name,final_amt,final_qty,
    case when extent_date>1 then 1 else extent_date end extent_date,--临保率
    qualitative_period,a.entry_days,a.entry_qty,a.entry_sdt,a.no_sale_days,a.max_sale_sdt
from
	(
	select 
		dc_code,dc_name,a.goods_id,bar_code,a.goods_name,a.unit_name,b.standard,
		classify_middle_code,--管理中类编号
		classify_middle_name,
		department_id,--课组编码
		department_name,
		final_amt,--期末库存额
		final_qty,--期末库存量
		qualitative_period,--保质期
		a.entry_days,--最近入库日期天数
		a.entry_qty,--最近入库数量
		a.entry_sdt,--最近入库日期
		coalesce(entry_days/qualitative_period ,0) as extent_date,--临保率
		a.no_sale_days,--未销售天数
		a.max_sale_sdt --最近一次销售日期
	from 
		csx_tmp.ads_wms_r_d_goods_turnover a --物流库存周转剔除客户直送、一件代发业务
	join
		(
		select 
			goods_id,bar_code,goods_name,standard,classify_large_code,classify_large_name,
			classify_middle_code,classify_middle_name,department_id,department_name,
			qualitative_period  ---保质期
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt='current'
		) b on a.goods_id=b.goods_id
	where 
		sdt=${hiveconf:sdt}
		--AND dc_code='W0A7'
		and business_division_code='12' --采购部编码:11生鲜采购部;12食品用品采购部;15易耗品
		--AND a.entry_days>b.qualitative_period
		and a.final_qty>0
	)a 
	join 
		(
		select 
			sales_province_code,sales_province_name,shop_id,shop_name
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt='current'
			and table_type=1
			and purpose IN ('01')  --地点用途： 01大客户仓库 02商超仓库 03工厂 04寄售门店 05彩食鲜小店 06合伙人物流 07BBC物流 08代加工工厂
			and sales_region_code='3'
		) c on a.dc_code=c.shop_id
where 
	round(extent_date,2)>0.75 ;


--==============================================================================================
-- 2.4、呆滞商品、未销售商品明细
-- 干货未销售天数>30天以上，生鲜其他课水果、蔬菜、肉禽未销售天数>5天以上，食品未销售天数>30天以上，用品类未销售天数>60天以上,以上商品入库天数>30天以上

set sdt='20210705'; 

drop table if exists csx_tmp.temp_no_sale_goods;
create table csx_tmp.temp_no_sale_goods as 
select 
	a.dist_code,a.dist_name,a.dc_code,a.dc_name,--a.division_code,--a.division_name,
    a.goods_id,b.bar_code,b.goods_name,b.unit_name,b.standard,classify_middle_code,
    classify_middle_name,a.dept_id,a.dept_name,a.final_qty,a.final_amt,a.no_sale_days,
    a.max_sale_sdt,a.entry_days,a.entry_qty,a.entry_sdt
from 
	csx_tmp.ads_wms_r_d_goods_turnover a
	join
		(
		select 
			goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,
			classify_middle_name,unit_name,standard,bar_code
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt='current'
		) b on a.goods_id=b.goods_id
	join
		(
		select 
			sales_province_code,sales_province_name,shop_id,shop_name
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt='current'
			and table_type=1
			and purpose IN ('01')
			-- AND shop_id in ${hiveconf:dc}
		) c on a.dc_code=c.shop_id
where 
	sdt=${hiveconf:sdt}
	and a.final_qty>a.entry_qty
	and (
        (category_large_code='1101'
        and (a.no_sale_days>30 or a.max_sale_sdt='' )--未销售天数
        )
       OR (dept_id IN ('H02','H03')
           and (a.no_sale_days>5 or a.max_sale_sdt='' )
           )
       OR (dept_id IN ('H04','H05','H06','H07','H08','H09','H10','H11')
            and (a.no_sale_days>30 or a.max_sale_sdt=''  )
           )
       OR (division_code ='12'
            and ( a.no_sale_days>30 or a.max_sale_sdt='' )
          )
       OR (division_code IN ('13','14','15')
           and (a.no_sale_days>60 or a.max_sale_sdt='' )
         )
      )
	and final_qty>0 
	and a.final_amt>2000
	and a.entry_days>30;
  

  
--高周转天数商品=低周转天数商品
--干货周转天数>45天以上且库存金额>3000元以上；水果、蔬菜周转天数>5天以上且库存金额>500元；
--生鲜其他课周转天数>15天以上且库存金额>2000元；食品部周转天数>45天以上且库存金额>2000元以上，
--用品类周转天数>60天以上且库存金额>3000 元以上
--入库天数>3天以上；未销售天数>7天以上

set sdt='20210705'; 

drop table if exists csx_tmp.tmp_hight_turn_goods ;
create temporary table  csx_tmp.tmp_hight_turn_goods as 
select 
	a.dist_code,a.dist_name,a.dc_code,a.dc_name,--a.division_code,--a.division_name,
    a.goods_id, b.bar_code,b.goods_name,b.unit_name,b.standard,classify_middle_code,
    classify_middle_name,a.dept_id,a.dept_name,coalesce(final_amt/final_qty) as cost,
    a.final_qty,a.final_amt,a.days_turnover_30,a.no_sale_days,a.max_sale_sdt,
    a.entry_days,a.entry_qty,a.entry_sdt
from 
	csx_tmp.ads_wms_r_d_goods_turnover a
	join
		(
		select 
			goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,
			classify_middle_name,unit_name,standard,bar_code
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt='current'
		) b on a.goods_id=b.goods_id
	join
		(
		select 
			sales_province_code,sales_province_name,shop_id,shop_name
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt='current'
			and table_type=1
			and purpose IN ('01')
			-- and shop_id in ${hiveconf:dc}
			-- and sales_region_code='3'
			--and sales_province_code='24'   --稽核省区编码
		) c on a.dc_code=c.shop_id
where    
    sdt=${hiveconf:sdt}             --更改查询日期
	and a.final_qty>a.entry_qty
	and ( (category_large_code='1101' and days_turnover_30>45 AND final_amt>3000)
    or (dept_id in ('H02','H03') and days_turnover_30>5 and a.final_amt>500 )
    OR (dept_id IN ('H04','H05','H06','H07','H08','H09','H10','H11') AND days_turnover_30>15 and a.final_amt>2000) 
    or (division_code ='12' and days_turnover_30>45 and final_amt>2000 )
    or (division_code in ('13','14')  and days_turnover_30>60 and final_amt>3000))
    and final_qty>0
    and a.entry_days>3
    and (a.no_sale_days>7 or no_sale_days='')
  ;
  
-- 关联未销售商品明细，剔除未销售商品
drop table if exists  csx_tmp.tmp_hight_turn_goods_01;

create temporary table csx_tmp.tmp_hight_turn_goods_01 as 
select 
	a.* 
from 
	csx_tmp.tmp_hight_turn_goods a 
	left join csx_tmp.temp_no_sale_goods b on a.dc_code=b.dc_code and a.goods_id=b.goods_id
where 
	b.goods_id is null ;
 
-- 高库存商品
-- 期末库存额>5000

set sdt='20210705'; 

drop table if exists csx_tmp.tmp_hight_stock ;
create temporary table  csx_tmp.tmp_hight_stock as 
select 
	a.* 
from 
	(
	select 
		a.dist_code,a.dist_name,a.dc_code,a.dc_name,--a.division_code,--a.division_name,
		a.goods_id,b.bar_code,b.goods_name,b.unit_name,b.standard,classify_middle_code,
		classify_middle_name,a.dept_id,a.dept_name,coalesce(final_amt/final_qty) as cost,
		a.final_qty,a.final_amt,a.days_turnover_30,a.no_sale_days,a.max_sale_sdt,
		a.entry_days,a.entry_qty,a.entry_sdt
	from 
		csx_tmp.ads_wms_r_d_goods_turnover a
		join
			(
			select 
				goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,
				classify_middle_name,unit_name,standard,bar_code
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt='current'
			) b ON a.goods_id=b.goods_id
		join
			(
			select 
				sales_province_code,sales_province_name,shop_id,shop_name
			from 
				csx_dw.dws_basic_w_a_csx_shop_m
			where 
				sdt='current'
				and table_type=1
				and purpose IN ('01')
				-- and shop_id in ${hiveconf:dc}
				-- and sales_region_code='3'
				and sales_province_name='重庆市' 
			) c on a.dc_code=c.shop_id
	where    
		sdt=${hiveconf:sdt}             --更改查询日期
    )a 
    left join 
		csx_tmp.temp_no_sale_goods b on a.dc_code=b.dc_code and a.goods_id=b.goods_id
    left join 
		csx_tmp.tmp_hight_turn_goods c  on a.dc_code=c.dc_code and a.goods_id=c.goods_id
where 
	b.goods_id is null 
	and c.goods_id is null 
    and a.final_amt>5000
order by 
	a.final_amt desc ;
	
  
-- 呆滞商品明细
  select * from  csx_tmp.temp_no_sale_goods where 1=1 and dc_code in #{hiveconf:dc};
-- 低周转商品明细
  select * from  csx_tmp.tmp_hight_turn_goods_01 where 1=1 and dc_code in #{hiveconf:dc};

-- 高库存商品明细
  select * from  csx_tmp.tmp_hight_stock where 1=1 and dc_code in #{hiveconf:dc};


--==============================================================================================

-- 2.8 收货单稽核——供应商入库金额TOP10单据
with temp01 as 
(
select 
	order_code,bd_id,receive_amt,row_number()over(partition by bd_id order by receive_amt desc) as aa
from 
	(
	select 
		order_code,
		case when division_code in ('12','13') then '12'  when division_code in ('11','10') then '10' end as bd_id,
		sum(price*receive_qty) as receive_amt
	from 
		csx_dw.dws_wms_r_d_entry_detail
	where 
		sdt>='20210601' and sdt<='20210704'
		and division_code in ('10','11','12','13')
		and receive_location_code='W0Q2'
		and province_name='贵州省'
		and business_type_name like '%供应商%'
		and super_class=1
    group by 
		order_code,
		case when division_code in ('12','13') then '12'  when division_code in ('11','10') then '10' end
    )a
),
temp02 as 
(
select 
	order_code,supplier_code,supplier_name,goods_code,goods_name,division_code,division_name,(price*receive_qty) as receive_amt,receive_qty
from 
	csx_dw.dws_wms_r_d_entry_detail
where 
	sdt>='20210601' and sdt<='20210704'
    and receive_location_code='W0Q2'
	and province_name='贵州省'
    and business_type_name like '%供应商%'
    and super_class=1
)

select 
	b.*,aa 
from 
	temp01 a
    join
		temp02 b on a.order_code=b.order_code
where a.aa<11
;



--==============================================================================================

--2.9 签收单稽核
--1、取逾期客户关联销售表，销售天数<逾期天数,且销售额大于0，随机抽取10个客户，
with temp01 as 
(
select 
	*,over_amt-(claim_amount-payment_amount_1) as overdue_amount 
from  
	csx_dw.report_sss_r_d_cust_receivable_amount
where 
	sdt=regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '')
	and province_name='重庆市'
	and (over_amt-(claim_amount-payment_amount_1))>0 
	and receivable_amount>0
	and smonth='小计'
),
  temp02 as 
(
select 
	customer_no,customer_name,order_no,business_type_name,sum(sales_value) sale,sdt,
    datediff(date_sub(current_timestamp(), 1),to_date(sales_time)) as sales_days,
	logistics_mode_name
from 
	csx_dw.dws_sale_r_d_detail
where sdt>='20210101' 
	and channel_code!='2'
	and business_type_name!='BBC'
group by 
	customer_no,customer_name,order_no,sdt,business_type_name,
	datediff(date_sub(current_timestamp(), 1),to_date(sales_time)),
	logistics_mode_name
)
 
select 
	* 
from 
	(
	select 
		distinct a.customer_no,a.customer_name,a.order_no,a.business_type_name,
		a.sale,a.sdt,a.sales_days,a.logistics_mode_name,
		b.overdue_amount,receivable_amount,max_over_days,payment_name
    from 
		temp02 a join temp01 b on a.customer_no=b.customer_no
	where 
		sales_days<max_over_days 
		and sale>0
    )a 
limit 15
  
  ;
    

--==============================================================================================
--3.1.1 工厂盘点差异稽核金额 >500或<-500
select 
	*
from
	(
	select 
		a.dc_code,a.dc_name,a.goods_code,a.goods_bar_code,a.goods_name,a.unit,a.reservoir_area_code,
		a.reservoir_area_name,store_location_code,a.store_location_name,
		sum(a.inventory_qty_diff) as diff_qty,
		sum(a.inventory_amount_diff) as diff_amt
	from 
		csx_dw.dwd_wms_r_d_inventory_product_detail a 
		join 
			(
			select 
				shop_id 
			from 
				csx_dw.dws_basic_w_a_csx_shop_m 
			where 
				sdt='current' 
				and purpose='03' 
				and sales_province_name='贵州省'
			) b on a.dc_code=b.shop_id
	where 
		sdt>='20210601' 
		and sdt<='20210630' 
	group by 
		store_location_code,a.store_location_name,a.dc_code,a.dc_name,a.goods_code,a.goods_bar_code,
		a.goods_name,a.unit,a.reservoir_area_code,a.reservoir_area_name
	) t1
where
	diff_amt>=500
	or diff_amt<=-500;
	
	

--3.1.2 工厂商品报损金额>500或<-500
select dc_code,dc_name,department_id,department_name,goods_code,goods_name,qty,amt,frmloss_type_name 
from 
(
select dc_code,dc_name,department_id,department_name,goods_code,goods_name,sum(qty)qty,sum(amt) as amt,frmloss_type_name 
from csx_dw.ads_wms_r_d_bs_detail_days
where sdt>='20210601' and sdt<='20210630'
and dc_code in('W079','W0A6')
group by dc_code,dc_name,department_id,department_name,goods_code,goods_name,frmloss_type_name 
)a where (amt>500 or amt<-500);  


---3.4 供应商预付款、扣款 调整逻辑规则取最后一笔余额
select
  a.company_code,
  a.company_name,
  a.supplier_code,
  a.supplier_name,
  account_group_name,
  business_no,
  (deduct_amount+not_deduct_amount) as resident_amount,
  update_time
 from 
 (
 select
    company_code,
    company_name,
    supplier_code,
    supplier_name,
    business_no, --付款款单
    business_amount,-- 扣款
    deduct_amount,--可用金额
    not_deduct_amount,--不可用金额
    total_amount, --总金额 预付款金额
    update_time,
    row_number() over(partition by supplier_code order by update_time desc ) as rank_1
  from csx_dw.dwd_pss_r_d_statement_prepayment_operation_record 
  where operation_type=2 and business_type=2
 )a 
 left join
(select supplier_code,supplier_name,reconciliation_tag,b.dic_value  as reconciliation_tag_name,account_group,c.dic_value as account_group_name from csx_ods.source_basic_w_a_md_supplier_info a 
 left join
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20210630' and dic_type='CONCILIATIONNFLAG' ) b on a.reconciliation_tag=b.dic_key 
 left join 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20210630' and dic_type='ZTERM' ) c on a.account_group=c.dic_key 
  where sdt='20210630'
 ) c on a.supplier_code=c.supplier_code
 where rank_1=1
 and company_code='2210'
 and (deduct_amount+not_deduct_amount)<>0;
 


-----------------------------------------------------------------------------------------------------------------------------------------------------------------
--备用代码 
--猪肉库存
select dc_code,
  dc_name,
  goods_code,
  goods_name,
  category_middle_code,
  category_middle_name,
  qty,
  amt
from csx_dw.dws_wms_r_d_accounting_stock_m
where dc_code = 'W079'
  AND department_id = 'H05'
  and sdt = '20210630'
  and qty > 0
  and reservoir_area_code not IN('PD01', 'PD02', 'TS01');    
  
--全国DC异常商品占比 统计数据导出
  select t1.dc_code,t1.dc_name,
        business_division_code,
        business_division_name, 
        sum(t1.final_amt) final_amt ,
        sum(period_inv_amt_30day)/sum(cost_30day) as turn_days,
        count(distinct t1.goods_id) as stock_sku,
        count(distinct t2.goods_id) as no_sale_sku,
        count(distinct t3.goods_id) as turn_sku,
        count(distinct t4.goods_id) as hight_sku
  from 
  (select dc_code,dc_name,
         business_division_code,
         business_division_name, 
         final_amt ,
         cost_30day,
         period_inv_amt_30day,
         goods_id
    from csx_tmp.ads_wms_r_d_goods_turnover  a
    join 
     (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
    AND purpose IN ('01')
    -- and shop_id in ${hiveconf:dc}
    -- AND sales_region_code='3'
    --and sales_province_code='24'   --稽核省区编码
    ) c ON a.dc_code=c.shop_id
  where sdt=${hiveconf:sdt}
   and a.final_qty>0
  ) t1
  left join 
  -- 未销售查询
   csx_tmp.temp_no_sale_goods as t2 on t1.dc_code=t2.dc_code and t1.goods_id=t2.goods_id 
  left join
  --低周转库存
   csx_tmp.tmp_hight_turn_goods_01 t3 on t1.dc_code=t3.dc_code and t1.goods_id=t3.goods_id 
  left join
  --高库存
   csx_tmp.tmp_hight_stock t4 on t1.dc_code=t4.dc_code and t1.goods_id=t4.goods_id 
   
   group by t1.dc_code,t1.dc_name,business_division_code,
         business_division_name;