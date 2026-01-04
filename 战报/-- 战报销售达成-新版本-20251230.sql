-- 战报销售达成
select
  case when a.performance_region_name like '%大区' then '省区' else '平台' end performance_region_name1,
 a.performance_region_name, 	
 a.performance_province_name,		
 a.performance_province_manager_employee_name,
 a.performance_city_name,
-- 北京、浙江、福建、重庆省区展示城市
a.performance_city_name  change_performance_city_name,
 a.performance_city_manager_employee_name,  
  a.change_channel_code,
  case when a.change_channel_name='大客户' then 'B端(日配+大福利)'
       when a.change_channel_name='商超' then 'M端'
    else a.change_channel_name end change_channel_name,
  c.target_sale_amt,
  c.target_profit,
  c.zy_target_sale_amt,
  c.zy_target_profit,
  b.d_sale_amt,
  b.sale_amt,
  b.profit,
  b.new_normal_sale_amt,    -- 日配销售管理销售额
  b.new_normal_profit,      -- 日配销售管理毛利额
  b.include_direct_normal_sale_amt, -- 日配销售额
  b.include_direct_normal_profit,   -- 日配毛利额
  -- 日配采购参与销售额
  b.include_direct_normal_sale_amt-b.new_normal_sale_amt as rp_purchse_sale_amt,    -- 日配采购参与销售额
  b.include_direct_normal_profit -b.new_normal_profit as rp_purchse_profit,         -- 日配采购参与毛利额
 
  -- 福利销售
  c.welfare_target_sale,
  c.welfare_target_profit,
  b.welfare_sale_amt,
  b.welfare_profit, 
  b.new_welfare_sale_amt,   -- 福利销售管理销售额
  b.new_welfare_profit, -- 福利销售管理毛利额

  c.bbc_target_sale,
  c.bbc_target_profit,
  b.bbc_sale_amt,
  b.bbc_profit,   
  b.mom_sale_amt,
  b.mom_profit,
  b.mom_normal_sale_amt,
  b.mom_normal_profit,
  b.mom_include_direct_normal_sale_amt,
  b.mom_include_direct_normal_profit,
  b.mom_welfare_sale_amt,
  b.mom_welfare_profit,
  b.mom_new_welfare_sale_amt,
  b.mom_new_welfare_profit,
  b.mom_bbc_sale_amt,
  b.mom_bbc_profit,  
  b.yom_sale_amt,
  b.yom_profit,
  b.yom_normal_sale_amt,
  b.yom_normal_profit,
  b.yom_include_direct_normal_sale_amt,
  b.yom_include_direct_normal_profit,
  b.yom_welfare_sale_amt,
  b.yom_welfare_profit,
  b.yom_new_welfare_sale_amt,
  b.yom_new_welfare_profit,
  b.yom_bbc_sale_amt,
  b.yom_bbc_profit,
    -- 福利采购参与销售额
  b.welfare_sale_amt-b.new_welfare_sale_amt as welfare_purchse_sale_amt, -- 福利采购参与销售额
  b.welfare_profit-b.new_welfare_profit as welfare_purchse_profit,  -- 福利采购参与毛利额
  b.mom_welfare_sale_amt- b.mom_new_welfare_sale_amt as mom_welfare_purchse_sale_amt,
  b.mom_welfare_profit- b.mom_new_welfare_profit as mom_welfare_purchse_profit,
  b.yom_welfare_sale_amt - b.yom_new_welfare_sale_amt as yom_welfare_purchse_sale_amt,
  b.yom_welfare_profit - b.yom_new_welfare_profit as yom_welfare_purchse_profit,

   -- 大福利=福利+BBC
  c.welfare_target_sale+c.bbc_target_sale as big_welfare_target_sale_amt,
  c.welfare_target_profit + c.bbc_target_profit as big_welfare_target_profit    ,
  b.welfare_sale_amt + b.bbc_sale_amt as big_welfare_sale_amt,
  b.welfare_profit + b.bbc_profit as big_welfare_profit,
  b.mom_welfare_sale_amt + b.mom_bbc_sale_amt as mom_big_welfare_sale_amt,
  b.mom_welfare_profit + b.mom_bbc_profit as mom_big_welfare_profit,
  b.yom_welfare_sale_amt + b.yom_bbc_sale_amt as yom_big_welfare_sale_amt,
  b.yom_welfare_profit + b.yom_bbc_profit as yom_big_welfare_profit,
  -- 大福利采购参与销售额 = 福利+BBC-福利销售管理-BBC销售管理
  b.welfare_sale_amt + b.bbc_sale_amt - (b.new_welfare_sale_amt + bbc_manage_sale_amt) as big_welfare_purchse_sale_amt,
  b.welfare_profit + b.bbc_profit - (b.new_welfare_profit +bbc_manage_profit) as big_welfare_purchse_profit,
  b.mom_welfare_sale_amt + b.mom_bbc_sale_amt - (b.mom_new_welfare_sale_amt + mom_bbc_manage_sale_amt) as mom_big_welfare_purchse_sale_amt,
  b.mom_welfare_profit + b.mom_bbc_profit - (b.mom_new_welfare_profit + mom_bbc_manage_profit) as mom_big_welfare_purchse_profit,
  b.yom_welfare_sale_amt + b.yom_bbc_sale_amt - (b.yom_new_welfare_sale_amt + yom_bbc_manage_sale_amt) as yom_big_welfare_purchse_sale_amt,
  b.yom_welfare_profit + b.yom_bbc_profit - (b.yom_new_welfare_profit + yom_bbc_manage_profit) as yom_big_welfare_purchse_profit,
  -- 大福利销售管理
  b.new_welfare_sale_amt + bbc_manage_sale_amt as  big_welfare_manage_sale_amt,
  b.new_welfare_profit +bbc_manage_profit as big_welfare_manage_profit,
  b.mom_new_welfare_sale_amt + mom_bbc_manage_sale_amt as mom_big_welfare_manage_sale_amt,
  b.mom_new_welfare_profit + mom_bbc_manage_profit as mom_big_welfare_manage_profit,
  b.yom_new_welfare_sale_amt + yom_bbc_manage_sale_amt as yom_big_welfare_manage_sale_amt,
  b.yom_new_welfare_profit + yom_bbc_manage_profit as yom_big_welfare_manage_profit
  
  
from
( -- 省区没有业绩时也要显示出来
	select 
		t1.*,
		change_channel_code,
		change_channel_name
	from
	(
		select distinct
       city_group_code            as performance_city_code,
       city_group_name            as performance_city_name,
       city_group_manager_id      as performance_city_manager_id,
       city_group_manager_name    as performance_city_manager_employee_name,
       city_group_manager_work_no as performance_city_manager_employee_code,
       province_code              as performance_province_code,
       province_name              as performance_province_name,
       province_manager_id        as performance_province_manager_id,
       province_manager_name      as performance_province_manager_employee_name,
       province_manager_work_no   as performance_province_manager_employee_code,
       region_code                as performance_region_code,
       region_name                as performance_region_name,
       region_manager_id          as performance_region_manager_id,
       region_manager_name        as performance_region_manager_employee_name,
       region_manager_work_no     as performance_region_manager_employee_code
-- 		from csx_data_config.sales_area_belong_mapping_config	-- 正式表需要切回
       from csx_data_market.sales_area_belong_mapping_config       
		where  region_code in ('1','2','3','4','5','6','8','100') -- 100其他：平台-B、平台-酒水；5大宗 6供应链 7BBC
		and province_code not in ('37','40','41') -- 100其他：其他、大客户平台、商超平台
		and city_group_name not in ('漳州市','四平市','上海市','新乡市','')
	) t1
	join
	(
		select '1'as change_channel_code, '大客户'as change_channel_name
		union all
		select '2'as change_channel_code, '商超'as change_channel_name
	) t2 on 1 = 1
) a 
left join 
( select -- performance_province_code, performance_city_code,  
-- 解决江西同比问题：之前江西归福建，后独立   
case when performance_province_code ='15' and performance_city_code ='37' then '14' else performance_province_code end performance_province_code,
case when performance_province_code ='15' and performance_city_code ='37' then '39' else  performance_city_code end  performance_city_code,    
    change_channel_code,
    sum(case when sdt='${EDATE}' then sale_amt end)/10000 d_sale_amt,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then sale_amt end)/10000 sale_amt,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then profit end)/10000 profit,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then new_normal_sale_amt end)/10000  new_normal_sale_amt,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then new_normal_profit end)/10000  new_normal_profit, 
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then include_direct_normal_sale_amt end)/10000  include_direct_normal_sale_amt,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then include_direct_normal_profit end)/10000  include_direct_normal_profit,  
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then welfare_sale_amt end)/10000  welfare_sale_amt,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then welfare_profit end)/10000  welfare_profit,  
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then bbc_sale_amt end)/10000  bbc_sale_amt,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then bbc_profit end)/10000  bbc_profit,  
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then new_welfare_sale_amt end)/10000 new_welfare_sale_amt,
    sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then new_welfare_profit end)/10000 new_welfare_profit,
	-- BBC销售管理
	sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then new_bbc_sale_amt end)/10000 bbc_manage_sale_amt	,
	sum(case when sdt>='${SDATE}' and  sdt<='${EDATE}' then new_bbc_profit end)/10000 bbc_manage_profit	 ,
    -- 环比数据	
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then sale_amt end)/10000 mom_sale_amt,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then profit end)/10000 mom_profit,
    sum(case when '${EDATE}'>='20250101' AND '${EDATE}'<'20250201' THEN 0 when  sdt>='${SBJ}' and  sdt<='${EBJ}' then new_normal_sale_amt end)/10000 mom_normal_sale_amt,
    sum(case when '${EDATE}'>='20250101' AND '${EDATE}'<'20250201' THEN 0 when sdt>='${SBJ}' and  sdt<='${EBJ}' then new_normal_profit end)/10000 mom_normal_profit,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then include_direct_normal_sale_amt end)/10000 mom_include_direct_normal_sale_amt,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then include_direct_normal_profit end)/10000 mom_include_direct_normal_profit,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then welfare_sale_amt end)/10000 mom_welfare_sale_amt,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then welfare_profit end)/10000 mom_welfare_profit,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then bbc_sale_amt end)/10000 mom_bbc_sale_amt,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then bbc_profit end)/10000 mom_bbc_profit,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then new_welfare_sale_amt end)/10000 mom_new_welfare_sale_amt,
    sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then new_welfare_profit end)/10000 mom_new_welfare_profit,
	-- BBC销售管理
	sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then new_bbc_sale_amt end)/10000 mom_bbc_manage_sale_amt	,
	sum(case when sdt>='${SBJ}' and  sdt<='${EBJ}' then new_bbc_profit end)/10000 mom_bbc_manage_profit	 ,
    -- 同比数据		
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then sale_amt end)/10000 yom_sale_amt,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then profit end)/10000 yom_profit,
    sum(case when '${EDATE}'>='20250101' AND '${EDATE}'<'20250201' THEN 0 when sdt>='${QBJ}' and  sdt<='${TBJ}' then new_normal_sale_amt end)/10000 yom_normal_sale_amt,
    sum(case when '${EDATE}'>='20250101' AND '${EDATE}'<'20250201' THEN 0 when sdt>='${QBJ}' and  sdt<='${TBJ}' then new_normal_profit end)/10000 yom_normal_profit,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then include_direct_normal_sale_amt end)/10000 yom_include_direct_normal_sale_amt,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then include_direct_normal_profit end)/10000 yom_include_direct_normal_profit,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then welfare_sale_amt end)/10000 yom_welfare_sale_amt,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then welfare_profit end)/10000 yom_welfare_profit,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then bbc_sale_amt end)/10000 yom_bbc_sale_amt,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then bbc_profit end)/10000 yom_bbc_profit,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then new_welfare_sale_amt end)/10000 yom_new_welfare_sale_amt,
    sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then new_welfare_profit end)/10000 yom_new_welfare_profit,
	-- BBC销售管理
	sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then new_bbc_sale_amt end)/10000 yom_bbc_manage_sale_amt	,
	sum(case when sdt>='${QBJ}' and  sdt<='${TBJ}' then new_bbc_profit end)/10000 yom_bbc_manage_profit	 
  from csx_data_market.ads_sale_performance_daily_paper_di
  where((sdt>= '${SBJ}' and sdt<='${EBJ}') or (sdt>= '${SDATE}' and sdt<='${EDATE}') or (sdt>= '${QBJ}' and sdt<='${TBJ}'))
  group by 
		case when performance_province_code ='15' and performance_city_code ='37' then '14' else performance_province_code end,
		case when performance_province_code ='15' and performance_city_code ='37' then '39' else  performance_city_code end,  
		   change_channel_code
) b on a.performance_province_code=b.performance_province_code
    and a.performance_city_code = b.performance_city_code
    and a.change_channel_code= b.change_channel_code
left join 
 (select 
    province_code,city_group_code,case when channel_code in (1,7) then 1 else channel_code end change_channel_code,    
    sum(sales_value)/10000 as target_sale_amt,
    sum(profit)/10000 as target_profit,
    sum(case when business_type_code in (1,2,6,10) then sales_value/10000  end ) zy_target_sale_amt,
    sum(case when business_type_code in (1,2,6,10) then profit/10000  end ) zy_target_profit,
    sum(if(business_type_code in (2,10),sales_value/10000,0)) welfare_target_sale,
    sum(if(business_type_code in (2,10),profit/10000,0)) welfare_target_profit,
    sum(if(business_type_code in (6),sales_value/10000,0)) bbc_target_sale,
    sum(if(business_type_code in (6),profit/10000,0)) bbc_target_profit
   from csx_data_market.dws_basic_w_a_business_target_manage	
	where month =left('${EDATE}',6) and date_mark = 'M'
	group by province_code,
		city_group_code,
		case when channel_code in (1,7) then 1 else channel_code end
 ) c  on a.performance_province_code=c.province_code
    and a.performance_city_code = c.city_group_code
    and a.change_channel_code= c.change_channel_code
  where a.performance_city_name!='天津市'
   order by  case when a.change_channel_name='大客户' then 1
       when a.change_channel_name='商超' then 2
    else 3 end,
     -- 按照省区排序
     FIELD(a.performance_province_name,'福建省','江西省','广东广州','广东深圳','北京市','河北省','陕西省','河南省','东北','重庆市','四川省','贵州省','上海','江苏南京','浙江省','安徽省','湖北省','其他','供应链','大宗'),
  FIELD(a.performance_city_name,'福州市','厦门市','泉州市','莆田市','南平市','三明市','宁德市','龙岩市','北京市','石家庄市','西安市','大连','哈尔滨市','东丰','重庆主城','万州区','黔江区','永川区','石柱县','成都市','宜宾','上海松江','江苏苏州','南京主城','江苏盐城','杭州市','宁波市','台州市','合肥市','阜阳市','武汉市')