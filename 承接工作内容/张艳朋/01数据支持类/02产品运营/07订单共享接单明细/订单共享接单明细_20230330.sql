	
	
select
	count(*) from (
	
drop table csx_analyse_tmp.csx_analyse_tmp_recep_order_detail;
create table csx_analyse_tmp.csx_analyse_tmp_recep_order_detail
as	
	
select
	recep_order_user_number,recep_order_by,order_category_name,
	a.goods_code,a.goods_name,spec,unit_name,inventory_dc_code,inventory_dc_name,count(*) as order_cnt,
	b.classify_large_name,b.classify_middle_name,b.classify_small_name,b.category_large_name,b.category_middle_name,b.category_small_name
from
	(
	select
		customer_code, -- 客户编码
		customer_name, -- 客户名称
		sub_customer_code, -- 子客户编码
		sub_customer_name, -- 子客户名称
		recep_order_user_number, -- 接单员工号
		recep_order_by, -- 接单员名称
		-- regexp_replace(to_date(recep_order_time),'-','') as recep_order_date, -- 接单日期
		-- order_business_type, -- 订单类型(NORMAL-日配单,WELFARE-福利单,BIGAMOUNT_TRADE-大宗贸易,INNER-内购单)
		case when order_business_type = 'NORMAL' then '日配单' when order_business_type = 'WELFARE' then '福利单'
		  when order_business_type = 'BIGAMOUNT_TRADE' then '大宗贸易' when order_business_type = 'INNER' then '内购单' end as order_category_name, -- 订单类型名称
		order_date,
		-- order_code,
		goods_code,
		goods_name,
		spec,
		unit_name,
		inventory_dc_code,
		inventory_dc_name
	from 
		csx_dwd.csx_dwd_csms_yszx_order_detail_di
	where 
		sdt >= '20230201'  
		and regexp_replace(to_date(recep_order_time),'-','') >= '20230201'
		and order_status not in ('CREATED', 'PAID', 'CONFIRMED') 
		and item_status <> 0
		and order_business_type in ('NORMAL')
		and delivery_type_code =0 -- 订单模式：0-配送,1-直送，2-自提，3-直通
		and recep_order_user_number in ('80743251','80898491','80920120','80969607','81039983','81139164','81163119','81165277','81083211','81168290','81169280','81169278','81179834','81180535',
		'80780439','81165598','81165595','81169326','81179726','81169645','81165279','81170737','81180078','81165597','81166860','81030501','81175109','81127037','81167831',
		'81165970','80908503','81169329','81169549','81169860','81173246','81173455','81174397','81175121','81178639','80983545','81168281','81169325','81169548','81181638',
		'81182489','81183923','81204448','81209814','81209815','81205448','81169550','81165278','80289664','80086515','81175111','81179816','80007019','80005548','80006085',
		'80930656','80442860','80013180','80751680','80824893','80011086','80002037','80953985','80724296','80738588','80424955','81113722','80763627','81165600','81083392',
		'81165601','80902693','80220218','81169063','81169266','81169066','80841427','81169317','80875276','80800455','81170728','81172402','81175243','81172899','81176213',
		'81176948','81181698','81182500','81186368','81185882','81186370','81186371','81169072')
	) a 
	join -- 过滤外部城市服务商订单
		(
		select
			shop_code
		from 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current' 
			and purpose != '09'
		) e on e.shop_code=a.inventory_dc_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,category_large_name,category_middle_name,category_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) b on b.goods_code=a.goods_code
group by 
	recep_order_user_number,recep_order_by,order_category_name,
	a.goods_code,a.goods_name,spec,unit_name,inventory_dc_code,inventory_dc_name,
	b.classify_large_name,b.classify_middle_name,b.classify_small_name,b.category_large_name,b.category_middle_name,b.category_small_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_recep_order_detail	
) a 
	
;

drop table csx_analyse_tmp.csx_analyse_tmp_recep_order_detail_02;
create table csx_analyse_tmp.csx_analyse_tmp_recep_order_detail_02
as		
select
	customer_code,customer_name,recep_order_user_number,recep_order_by,order_category_name,order_date,goods_code,goods_name,spec,unit_name,inventory_dc_code,inventory_dc_name,goods_remarks
from
	(
	select
		customer_code, -- 客户编码
		-- customer_name, -- 客户名称
		regexp_replace(customer_name,'\n|\t|\r|\,|\"|\\\\n','') as customer_name,
		recep_order_user_number, -- 接单员工号
		recep_order_by, -- 接单员名称
		case when order_business_type = 'NORMAL' then '日配单' when order_business_type = 'WELFARE' then '福利单'
		  when order_business_type = 'BIGAMOUNT_TRADE' then '大宗贸易' when order_business_type = 'INNER' then '内购单' end as order_category_name, -- 订单类型名称
		order_date,
		goods_code,
		-- goods_name,
		regexp_replace(goods_name,'\n|\t|\r|\,|\"|\\\\n','') as goods_name,
		--spec,
		regexp_replace(spec,'\n|\t|\r|\,|\"|\\\\n','') as spec,
		-- unit_name,
		regexp_replace(unit_name,'\n|\t|\r|\,|\"|\\\\n','') as unit_name,
		inventory_dc_code,
		-- inventory_dc_name,
		regexp_replace(inventory_dc_name,'\n|\t|\r|\,|\"|\\\\n','') as inventory_dc_name,
		-- goods_remarks
		regexp_replace(goods_remarks,'\n|\t|\r|\,|\"|\\\\n','') as goods_remarks
	from 
		csx_dwd.csx_dwd_csms_yszx_order_detail_di
	where 
		sdt >= '20230201'  
		and regexp_replace(to_date(recep_order_time),'-','') >= '20230201'
		and order_status not in ('CREATED', 'PAID', 'CONFIRMED') 
		and item_status <> 0
		and order_business_type in ('NORMAL')
		and delivery_type_code =0 -- 订单模式：0-配送,1-直送，2-自提，3-直通
		and goods_remarks rlike '件|箱|瓶|个|条'
		and recep_order_user_number in ('80743251','80898491','80920120','80969607','81039983','81139164','81163119','81165277','81083211','81168290','81169280','81169278','81179834','81180535',
		'80780439','81165598','81165595','81169326','81179726','81169645','81165279','81170737','81180078','81165597','81166860','81030501','81175109','81127037','81167831',
		'81165970','80908503','81169329','81169549','81169860','81173246','81173455','81174397','81175121','81178639','80983545','81168281','81169325','81169548','81181638',
		'81182489','81183923','81204448','81209814','81209815','81205448','81169550','81165278','80289664','80086515','81175111','81179816','80007019','80005548','80006085',
		'80930656','80442860','80013180','80751680','80824893','80011086','80002037','80953985','80724296','80738588','80424955','81113722','80763627','81165600','81083392',
		'81165601','80902693','80220218','81169063','81169266','81169066','80841427','81169317','80875276','80800455','81170728','81172402','81175243','81172899','81176213',
		'81176948','81181698','81182500','81186368','81185882','81186370','81186371','81169072')
	) a 
	join -- 过滤外部城市服务商订单
		(
		select
			shop_code
		from 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current' 
			and purpose != '09'
		) e on e.shop_code=a.inventory_dc_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_recep_order_detail_02
