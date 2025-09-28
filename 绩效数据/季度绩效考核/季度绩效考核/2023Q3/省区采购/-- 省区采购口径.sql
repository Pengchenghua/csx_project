-- 销售总监经理主管指标口径 
-- 后台收入剔除仓-待确定
select
  settlement_dc_code,
  settlement_dc_name,
  company_code,
  company_name
from
  csx_dwd.csx_dwd_pss_settle_settle_bill_di
where
  (settlement_dc_name not like '%项目供应商%'
  and settlement_dc_name not like '%福利%'
  and settlement_dc_name not like '%BBC%'
  and settlement_dc_name not like '%全国%'
  and settlement_dc_name not like '%合伙人%'
  and settlement_dc_name not like '%服务商%'
  and settlement_dc_name not like '%直送%'
  )
  
group by
  settlement_dc_code,
  settlement_dc_name,
  company_code,
  company_name


----------- 采购部门绩效考核：采购占比2023Q4开始使用  全国采购占比DC
----------- 生鲜全国采购占比
select
   a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
   a.classify_middle_name,
   dc_code,
   nvl(amtfenzi,0),
   receive_amt,
   if(receive_amt=0,0,nvl(amtfenzi,0)/receive_amt) zhanbi
from (
select
   a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
   a.classify_middle_name,
     a.dc_code,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,performance_city_name,
   dc_code,
   case when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊','米','蛋','熟食烘焙','干货加工') then classify_middle_name
	 else '食百' end  as classify_middle_name,
   csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
   is_central_tag, --品类+供应商集采  
   sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20230701' and sdt<='20230930'
  AND order_code like 'IN%'
group by performance_region_name,
   performance_province_name,performance_city_name,
   dc_code,
   -- classify_middle_code,
   case when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊','米','蛋','熟食烘焙','干货加工') then classify_middle_name
	 else '食百' end,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag

)a 
left join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
where a.classify_middle_name<>'食百'
group by a.performance_region_name,
   a.performance_province_name,a.performance_city_name,
  a.dc_code,
   a.classify_middle_name
union all   
 -------------  食百
 -- 1.采购占比食百修改
 select
   a.performance_region_name,
   a.performance_province_name,
   a.performance_city_name,
  a.dc_code,
 --  a.classify_middle_code,
   a.classify_middle_name,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,
   performance_city_name,
   dc_code,
   'B00'  as classify_middle_code,
   '食百'  as classify_middle_name,
   csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
   if(supplier_code in  
          ('20054206','20058482','20055149','20058420','20054478','20057039','20055847','20057161','20028053','20054206','20055149',
          '20058420','20054478','20057039','20039468','20027660','20059618','20049739','20059006','20035903','20056817','20032305',
          '20042204','20054206','20055149','20058420','20054478','20057039','20016117','20040455','20025727','20034091','20030254',
          '20034539','20051022','20059017','20039079','20059123','20035648','20029968','20022637','20051729','200428','20032963',
          '20050591','20049986','20034257','20050612','20038485','125526FZ','20034929','20036699','20023896','20035092','20046667',
          '20024998','20026373','20020561','20035723','20046873','20032995','20032779','20017602','20035727','20056045','20051435',
          '20059131','20053435','X00016','20041413','20044104','20035749','20032958','20055689','20044532','20034040','201538','X00667',
          '20048472','20029808','20051055','20035641','20054206','20055149','20058420','20054478','20057039','20058964','20055789',
          '20055793','20054206','20055149','20058420','20054478','20057039','20060600','20060936','20060131','20055800','20054206',
          '20055149','20058420','20054478','20057039','20057555','20056056','20059481','20056009','20051682','20054206','20055149',
          '20058420','20054478','20057039','20060297','20056195','20053968','20054206','20055149','20058420','20054478','20057039',
          '20058882','20055624','20055687','20058482','20054206','20055149','20058420','20054478','20057039','20052108','20043433',
          '20056766','20060590','20043203','20054594','20058482','20054206','20055149','20054478','20057039','20015198','20058483',
          '20040569','20043620','20051102','20042440','20042593','20042858','20038363','20054501','20032908','20060131','20056731',
          '20041373','20040910','20020331','20051090','20043203','20054594','20054206','20055149','20058420','20054478','20057039',
          '20054206','20055149','20058420','20054478','20057039','20043747','20045257','20052997','20051075','20055950','20006126',
          '20042553','20046634','20058482','20054206','20055149','20058420','20054478','20057039','20015198','20041536','20056731',
          '20006870','20056465','20058345','20058420','20054206','20055149','20054478','20057039','20050477','20047689','20050317',
          '20060648','20055750','20047878','20060682','20051410','20046854','20042176','20029976','20055891','20045573','20058482',
          '20055149','20058420','20054478','20057039','20015198','20047725','20041311','20058323','20058751','20056311','20056359',
          '20053074','20043965','20057951','20047739','20060424','20044770','20055284','20060425','20040892','20041228','20038251',
          '20055311','20058872','20058817','20045251','20041365','20060277','20013388','20054206','20054206','20051865','20016978',
          '20055149','20058420','20054478','20057039','20044284','20047210','20038363','20055111','20043753','20060633','20055653',
          '20017531','20057187','20055827','20024248','20055827','20055430','20038484','20055908','20016994','20043687','20034350',
          '20056540','20045301','20035724','20042327','20050894','20054721','20042321'),'1','0')
      as is_central_tag, -- 品类+供应商集采
     sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20230701' and sdt<='20230930'
  AND order_code like 'IN%'
 and classify_middle_name in 
    ('酒','香烟饮料','休闲食品','面类/米粉类','调味品类','食用油类','罐头小菜','早餐冲调','常温乳品饮料','冷藏冷冻食品','清洁用品','纺织用品','家电','文体用品','家庭用品','易耗品','服装')
group by performance_region_name,
   performance_province_name,performance_city_name,
   dc_code,
   classify_middle_code,
   classify_middle_name,
   csx_purchase_level_code,
   order_business_type,
   if(supplier_code in  
          ('20054206','20058482','20055149','20058420','20054478','20057039','20055847','20057161','20028053','20054206','20055149',
          '20058420','20054478','20057039','20039468','20027660','20059618','20049739','20059006','20035903','20056817','20032305',
          '20042204','20054206','20055149','20058420','20054478','20057039','20016117','20040455','20025727','20034091','20030254',
          '20034539','20051022','20059017','20039079','20059123','20035648','20029968','20022637','20051729','200428','20032963',
          '20050591','20049986','20034257','20050612','20038485','125526FZ','20034929','20036699','20023896','20035092','20046667',
          '20024998','20026373','20020561','20035723','20046873','20032995','20032779','20017602','20035727','20056045','20051435',
          '20059131','20053435','X00016','20041413','20044104','20035749','20032958','20055689','20044532','20034040','201538','X00667',
          '20048472','20029808','20051055','20035641','20054206','20055149','20058420','20054478','20057039','20058964','20055789',
          '20055793','20054206','20055149','20058420','20054478','20057039','20060600','20060936','20060131','20055800','20054206',
          '20055149','20058420','20054478','20057039','20057555','20056056','20059481','20056009','20051682','20054206','20055149',
          '20058420','20054478','20057039','20060297','20056195','20053968','20054206','20055149','20058420','20054478','20057039',
          '20058882','20055624','20055687','20058482','20054206','20055149','20058420','20054478','20057039','20052108','20043433',
          '20056766','20060590','20043203','20054594','20058482','20054206','20055149','20054478','20057039','20015198','20058483',
          '20040569','20043620','20051102','20042440','20042593','20042858','20038363','20054501','20032908','20060131','20056731',
          '20041373','20040910','20020331','20051090','20043203','20054594','20054206','20055149','20058420','20054478','20057039',
          '20054206','20055149','20058420','20054478','20057039','20043747','20045257','20052997','20051075','20055950','20006126',
          '20042553','20046634','20058482','20054206','20055149','20058420','20054478','20057039','20015198','20041536','20056731',
          '20006870','20056465','20058345','20058420','20054206','20055149','20054478','20057039','20050477','20047689','20050317',
          '20060648','20055750','20047878','20060682','20051410','20046854','20042176','20029976','20055891','20045573','20058482',
          '20055149','20058420','20054478','20057039','20015198','20047725','20041311','20058323','20058751','20056311','20056359',
          '20053074','20043965','20057951','20047739','20060424','20044770','20055284','20060425','20040892','20041228','20038251',
          '20055311','20058872','20058817','20045251','20041365','20060277','20013388','20054206','20054206','20051865','20016978',
          '20055149','20058420','20054478','20057039','20044284','20047210','20038363','20055111','20043753','20060633','20055653',
          '20017531','20057187','20055827','20024248','20055827','20055430','20038484','20055908','20016994','20043687','20034350',
          '20056540','20045301','20035724','20042327','20050894','20054721','20042321'),'1','0')
)a 
 join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
group by a.performance_region_name,
   a.performance_province_name,a.performance_city_name,
   a.classify_middle_name,a.dc_code) a ;
		 
-- -- -- -- -- -- -- 客诉率		 
select *,
  nvl(kesu,0)/nvl(a.skuall,0)  kesu_lv
from (select
*,
sum(sku) over(partition by smonth,performance_region_name,performance_province_name,performance_city_name) skuall
from 
(  
select
  a.smonth,
  --  a.performance_region_code,
  a.performance_region_name,
  --  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
    a.inventory_dc_code,
  --  a.inventory_dc_name,
  if(a.classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货'),'生鲜','食百') as  classify_name,
  a.classify_middle_name,
  sum(nvl(b.kesu,0)) kesu,
  sum(nvl(a.sku,0)) sku
from 
(
select
  smonth,
  --  performance_region_code,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  --  IF(performance_province_name='浙江省',performance_city_name,performance_province_name) performance_province_name,
   inventory_dc_code,inventory_dc_name,
  classify_middle_code,
  classify_middle_name,
  sum(sku) sku
from (
select 
  substr(sdt,1,6) smonth,
 performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,inventory_dc_name,
  a.classify_middle_code,
  c.classify_middle_name,
  a.order_code,
  count(distinct a.goods_code) sku
from csx_dws.csx_dws_sale_detail_di a
left join
  (
    select
      goods_code,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
   ) c on c.goods_code = a.goods_code
where  sdt>='20230701' and sdt<='20230930'
      	and channel_code in('1','7','9')
		and business_type_code not in(4,6) --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 --  1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 --  退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
group by substr(sdt,1,6) ,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,inventory_dc_name,
  a.classify_middle_code,
  c.classify_middle_name,
  a.order_code
  )a 
group by smonth,
 --  performance_region_code,
  performance_region_name,
 --  performance_province_code,
 performance_province_name,
 performance_city_name,
   --  IF(performance_province_name='浙江省',performance_city_name,performance_province_name),
   inventory_dc_code,inventory_dc_name,
  classify_middle_code,classify_middle_name	
	) a
left join 
(
  select
    substr(sdt,1,6) smonth,        --  客诉日期
	performance_province_name,
      inventory_dc_code,
    if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code) classify_middle_code,
    count(distinct complaint_code)  kesu -- - 客诉单量
  from csx_analyse.csx_analyse_fr_oms_complaint_detail_di
  where  sdt>='20230701' and sdt<='20230930'
        and complaint_status_code in (20,30)  --  客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
		and complaint_deal_status in (10,40) --  责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and second_level_department_name !=''
		and first_level_department_name='采购'
  group by  substr(sdt,1,6),        --  客诉日期
          inventory_dc_code,
         if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code),performance_province_name
  )b on a.inventory_dc_code=b.inventory_dc_code and a.smonth=b.smonth and a.classify_middle_code=b.classify_middle_code 
        and a.performance_province_name=b.performance_province_name
group by  a.smonth,
  --  a.performance_region_code,
  a.performance_region_name,
  --  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
    a.inventory_dc_code,
  --  a.inventory_dc_name,
  if(a.classify_middle_name in ('水果','蔬菜','水产','牛羊','调理预制品','猪肉','家禽','米','蛋','熟食烘焙','干货'),'生鲜','食百'),
  a.classify_middle_name)a 
  )a
;