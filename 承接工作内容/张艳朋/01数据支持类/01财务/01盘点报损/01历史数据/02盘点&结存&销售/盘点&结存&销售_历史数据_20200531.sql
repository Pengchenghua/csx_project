
--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_inv_stock_sales_day partition(sdt)

select
	d.province_name,
	d.city_name,
	coalesce(b.dc_code,c.dc_code,a.location_code) as location_code,
	coalesce(b.division_name,c.division_name,a.division_name) as division_name,
	coalesce(a.surplus_qty,'0') as surplus_qty,
	coalesce(a.surplus_amt,'0') as surplus_amt,
	coalesce(a.loss_qty,'0') as loss_qty,
	coalesce(a.loss_amt,'0') as loss_amt,
	coalesce(b.qty,'0') as qty,
	coalesce(b.amt,'0') as amt,
	coalesce(a.surplus_qty+a.loss_qty,'0') as surplus_loss_total_qty,
	coalesce(a.surplus_amt+a.loss_amt,'0') as surplus_loss_total_amt,
	coalesce(c.sales_value,'0') as sales_value,
	coalesce(c.sales_cost,'0') as sales_cost,
	coalesce(c.profit,'0') as profit,
	coalesce(c.profit/c.sales_value,'0') as profit_rate,
	coalesce((a.surplus_amt+a.loss_amt)/c.sales_cost,'0') as loss_rate,
	coalesce((a.surplus_amt+a.loss_amt+c.profit)/c.sales_value,'0') as inv_after_profit_rate,
	'20200531' as update_time,
	'202005' as sdt
from
	--盘盈亏
	(
	select
		location_code,
		division_name,
		sum(no_post_surplus_qty+post_surplus_qty) as surplus_qty,
		sum(no_post_surplus_amt+post_surplus_amt_no_tax) as surplus_amt,
		sum(no_post_loss_qty+post_loss_qty) as loss_qty,
		sum(no_post_loss_amt+post_loss_amt_no_tax) as loss_amt
	from
		(
		select
			location_code,
			if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品')) as division_name, --部类
			--业务盘点-盘盈
			if(substr(move_type,1,3) in ('111') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction='+',-1*qty,qty),0) as no_post_surplus_qty,   --业务盘点未过账 盘盈 数量，注意direction条件'+'
			if(substr(move_type,1,3) in ('111') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction='+',-1*amt,amt),0) as no_post_surplus_amt,   --业务盘点未过账 盘盈 含税金额，注意direction条件'+'
			--业务盘点-盘亏
			if(substr(move_type,1,3) in ('110') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction='+',-1*qty,qty),0) as no_post_loss_qty,   --业务盘点未过账 盘盈 数量，注意direction条件'+'
			if(substr(move_type,1,3) in ('110') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction='+',-1*amt,amt),0) as no_post_loss_amt,   --业务盘点未过账 盘盈 含税金额，注意direction条件'+'
			--财务盘点-盘盈
			if(substr(move_type,1,3) in ('115') and amt_no_tax<>'0',if(direction='-',-1*qty,qty),0) as post_surplus_qty,   --财务盘点过账不含税数量	
			if(substr(move_type,1,3) in ('115') and amt_no_tax<>'0',if(direction='-',-1*amt_no_tax,amt_no_tax),0) as post_surplus_amt_no_tax,   --财务盘点过账不含税金额
			--财务盘点-盘亏
			if(substr(move_type,1,3) in ('116') and amt_no_tax<>'0',if(direction='-',-1*qty,qty),0) as post_loss_qty,   --财务盘点过账不含税数量	
			if(substr(move_type,1,3) in ('116') and amt_no_tax<>'0',if(direction='-',-1*amt_no_tax,amt_no_tax),0) as post_loss_amt_no_tax  --财务盘点过账不含税金额	
		from 
			csx_tmp.source_cas_r_d_accounting_credential_item 
		where 
			sdt='19990101' 
			and to_date(posting_time) between '2020-05-01' and '2020-05-31'
			and substr(move_type,1,3) in ('110','111','115','116')
		) t1
	group by
		location_code,
		division_name
	) a
	-- 结存信息
	full join 
		(
		select
			dc_code,
			if(substr(department_id,1,1) in('U','H'),'生鲜',if(substr(department_id,1,1)='A','食百','易耗品')) division_name,   --部类
			sum(qty) as qty,
			sum(amt) as amt
		from
			csx_dw.dws_wms_r_d_accounting_stock_m
		where
			sdt='20200531'
			and substr(reservoir_area_code,1,2) not in ('PD','ZT') --盘点 在途
		group by
			dc_code,
			if(substr(department_id,1,1) in('U','H'),'生鲜',if(substr(department_id,1,1)='A','食百','易耗品'))
		) b on b.dc_code=a.location_code and b.division_name=a.division_name
	-- 销售信息
	full join 
		(
		select	
			dc_code,
			if(substr(department_code,1,1) in('U','H'),'生鲜',if(substr(department_code,1,1)='A','食百','易耗品')) division_name,   --部类
			sum(sales_value) as sales_value,
			sum(sales_cost) as sales_cost,
			sum(profit) as profit
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt between '20200501' and '20200531'
			and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
			and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)	
			and (dc_code !='W0B6' or (dc_code ='W0B6' and channel=7)) --销售额取全部 排除福建单独仓库W0B6的非BBC业绩		
		group by
			dc_code,
			if(substr(department_code,1,1) in('U','H'),'生鲜',if(substr(department_code,1,1)='A','食百','易耗品'))  --部类	
		) c on c.dc_code=b.dc_code and c.division_name=b.division_name
	-- 省区城市信息
	left join 
		(
		select 
			shop_id,shop_name,province_code,province_name,city_code,city_name
		from 
			csx_dw.dws_basic_w_a_csx_shop_m 
		where 
			sdt = 'current'
			and table_type=1
		) d on d.shop_id=coalesce(b.dc_code,c.dc_code,a.location_code)
		
	





