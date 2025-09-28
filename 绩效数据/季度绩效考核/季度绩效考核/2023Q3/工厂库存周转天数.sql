select 
performance_province_name,
performance_city_name,
sum(fact_values)/3  fact_cnt
from (
-- (月初库存金额+月末库存金额)/2/总领用金额*30
select
a.months,
a.performance_province_name,
a.performance_city_name,
if(a.requisition_amt=0,0,(a.qm_amt+b.qm_amt)/2/a.requisition_amt*30) fact_values
from ( 
select
       substr(sdt,1,6) months,
        c.performance_province_name,
		c.performance_city_name,
		sum(requisition_amt) requisition_amt,
		sum(qm_amt) qm_amt
		--  sum(begin_inventoty_amt+qm_amt)/2/sum(use_amt)*30 fact_values
   from csx_ads.csx_ads_wms_goods_turnover_df a
     left join (select  shop_code,performance_province_name,performance_city_name
				from csx_dim.csx_dim_shop 
				where sdt='20230930'
				)c
        on a.dc_code = c.shop_code
where a.sdt in ('20230731','20230831','20230930')  
and a.division_name='易耗品'
and a.dc_code in 
   (
'W0T5',
'W080',
'W048',
'W053',
'WB03',
'W0T3',
'W0Q3',
'W0Q4',
'W0Q1',
'W0T6',
'W0Q8',
'W088',
'W0R8',
'W0R9',
'W0P6',
'W0AS',
'W0AR',
'W0A6',
'W079',
'W0M6',
'W0P3',
'WB01',
'W039',
'W0T7',
'W0AZ'
)
	    and a.goods_code not in 
		('150140','904544','904488','81676','1232005','614996','1234069','1263534','195740','437148','452453','407010','360855','436691','452462','360900',
		'912340','926110','930553','929543','902220','444698','408725','435984','902298','406937','902343','407070','845790','1234034','741211','1210073',
		'1295393','1284067','1253936','450944','902324','475492','841823','447477','447612','447667','361998','508379','1250473','902317','902296','1235578',
		'1235612','1235577','403141','1188543','409738','850572','706239','721250','704366','346672','1238532','362238','873996','443429','873997','1093155',
		'1186407','1198595','1056043','235783','1268580','1256215','902305','437149','437150','1260353','406950','407039','407026','407015','408646','1282161',
		'1256369','1370102','440414','1256368','1420060','1256367','1407884','417979','1256366','1256365','761397','457658','1235625','1235559','902225',
		'902330','406976','1237048','1185412','858860','678884','519143','901873','1465218','942974','360852','407025','440301','452457','406974','1428096',
		'1428163','717195','408667','447637','452352','910247','850641','437154','678885','362031','519142','407041','1068062','1274251','440413','528417',
		'1371169','1313392','1313397','153304','1273659','444244','452447','360867','766273','1067931','1250245','1221784','361547','1221383','361540','361538',
		'361530','361375','361417','361425','361529','361528','361463','361938','1227333','362001','362012','408711','196194','6624','904493','360838','444760',
		'407044','1212586','360869','360864','440280','850107','1422474','360884','453278','436642','436653','440284','440317','440342','1063715','440362',
		'556397','436667','360938','837212','1253564','364128','447636','406977','997594','872507','873985','873986','441960','441973','889814','858854',
		'872544','872510','872515','872516','438979','611437','611438','437812','433059','433048','433437','433484','433044','435962','433033','433026',
		'918996','603619','585579','585578','436575','872539','362009','441991','436955','448769','448768','441965','441966','585568','441972','441978',
		'443428','441983','441982','873998','873988','873982','873980','441964','441962','873975','873976','873978','1250887','1438064','1446866','1446289',
		'1446948','1382006','440343','1426746','1195031','1432691','1423800','453276','407059','1458170','1461000','1448939','436650','436657','360874',
		'1445428','1425675','1425676','1446966','437158','961617','1461773','1488877','902287','408684','360932','1438645','324426','1434686','362015',
		'850557','407020','1433180','1246864','1246865','651935','451845','1424270','1480737','324660','1448605','391329','1468869','763150','324388',
		'324395','437167','440339','1360294','408733','436718','1466565','440252','437143','1471858','1429375','1422815','1464495','1468916','1475837',
		'1455681','910270','1465582','910271','1420230','1448594','407065','1424818','360868','436670','437168','1428867','360851','437151','1371255',
		'360881','360890','440241','407061','407060','1460269','407057','1457822','910277','437161','993293','437155','1431848','440366','1381932','1482110',
		'941552','1454546','440312','1363380','324429','1437112','1437300','408691','906260','1403622','1460909','407027','406949','1415678','902282','1430491',
		'902284','436666','1368944','1470876','1441277','1424230','1467612','910267','1438006','360931','360934','360929','367509','1463445','778115','723248',
		'723252','1421597','766272','1455467','577405','1455551','407817','1015662','1211369','403231','434026','1416932','408640','872454','1424225','902224',
		'1472937','1472936','1449780','756017','1437640','1431976','1048752','1123881','1048761','1048753','1419502','1455437','1062628','1267348','904528',
		'994898','1054340','902329','360846','407028','440237','452454','736078','361277','1243380','1192102','1192103','1243381','413415','875743','822456',
		'276669','1468706','717933','436715','1479576','692922','1018135','556396','1500340','1378275','1501149','1505072','1501187','929760','452451',
		'1505521','1505458','1195038','1505503','436668','1511932','1508853','1498923','436627','408728','360936','1235956','243755')

    group by  months,
        c.performance_province_name,
		c.performance_city_name)a
left join (
 select
       case when substr(sdt,1,6)='202308' then '202309'
            when substr(sdt,1,6)='202307' then '202308'	 
             when substr(sdt,1,6)='202306' then '202307' end months,
        c.performance_province_name,
		c.performance_city_name,
		sum(qm_amt) qm_amt
		-- sum(begin_inventoty_amt+qm_amt)/2/sum(use_amt)*30 fact_values
   from csx_ads.csx_ads_wms_goods_turnover_df a
     left join (select  shop_code,performance_province_name,performance_city_name
				from csx_dim.csx_dim_shop 
				where sdt='20230930'
				)c
        on a.dc_code = c.shop_code

   where a.sdt in ('20230630','20230731','20230831')  
   and a.division_name='易耗品'
   and a.dc_code in 
   (
'W0T5',
'W080',
'W048',
'W053',
'WB03',
'W0T3',
'W0Q3',
'W0Q4',
'W0Q1',
'W0T6',
'W0Q8',
'W088',
'W0R8',
'W0R9',
'W0P6',
'W0AS',
'W0AR',
'W0A6',
'W079',
'W0M6',
'W0P3',
'WB01',
'W039',
'W0T7',
'W0AZ'
)
	    and a.goods_code not in 
		('150140','904544','904488','81676','1232005','614996','1234069','1263534','195740','437148','452453','407010','360855','436691','452462','360900',
		'912340','926110','930553','929543','902220','444698','408725','435984','902298','406937','902343','407070','845790','1234034','741211','1210073',
		'1295393','1284067','1253936','450944','902324','475492','841823','447477','447612','447667','361998','508379','1250473','902317','902296','1235578',
		'1235612','1235577','403141','1188543','409738','850572','706239','721250','704366','346672','1238532','362238','873996','443429','873997','1093155',
		'1186407','1198595','1056043','235783','1268580','1256215','902305','437149','437150','1260353','406950','407039','407026','407015','408646','1282161',
		'1256369','1370102','440414','1256368','1420060','1256367','1407884','417979','1256366','1256365','761397','457658','1235625','1235559','902225',
		'902330','406976','1237048','1185412','858860','678884','519143','901873','1465218','942974','360852','407025','440301','452457','406974','1428096',
		'1428163','717195','408667','447637','452352','910247','850641','437154','678885','362031','519142','407041','1068062','1274251','440413','528417',
		'1371169','1313392','1313397','153304','1273659','444244','452447','360867','766273','1067931','1250245','1221784','361547','1221383','361540','361538',
		'361530','361375','361417','361425','361529','361528','361463','361938','1227333','362001','362012','408711','196194','6624','904493','360838','444760',
		'407044','1212586','360869','360864','440280','850107','1422474','360884','453278','436642','436653','440284','440317','440342','1063715','440362',
		'556397','436667','360938','837212','1253564','364128','447636','406977','997594','872507','873985','873986','441960','441973','889814','858854',
		'872544','872510','872515','872516','438979','611437','611438','437812','433059','433048','433437','433484','433044','435962','433033','433026',
		'918996','603619','585579','585578','436575','872539','362009','441991','436955','448769','448768','441965','441966','585568','441972','441978',
		'443428','441983','441982','873998','873988','873982','873980','441964','441962','873975','873976','873978','1250887','1438064','1446866','1446289',
		'1446948','1382006','440343','1426746','1195031','1432691','1423800','453276','407059','1458170','1461000','1448939','436650','436657','360874',
		'1445428','1425675','1425676','1446966','437158','961617','1461773','1488877','902287','408684','360932','1438645','324426','1434686','362015',
		'850557','407020','1433180','1246864','1246865','651935','451845','1424270','1480737','324660','1448605','391329','1468869','763150','324388',
		'324395','437167','440339','1360294','408733','436718','1466565','440252','437143','1471858','1429375','1422815','1464495','1468916','1475837',
		'1455681','910270','1465582','910271','1420230','1448594','407065','1424818','360868','436670','437168','1428867','360851','437151','1371255',
		'360881','360890','440241','407061','407060','1460269','407057','1457822','910277','437161','993293','437155','1431848','440366','1381932','1482110',
		'941552','1454546','440312','1363380','324429','1437112','1437300','408691','906260','1403622','1460909','407027','406949','1415678','902282','1430491',
		'902284','436666','1368944','1470876','1441277','1424230','1467612','910267','1438006','360931','360934','360929','367509','1463445','778115','723248',
		'723252','1421597','766272','1455467','577405','1455551','407817','1015662','1211369','403231','434026','1416932','408640','872454','1424225','902224',
		'1472937','1472936','1449780','756017','1437640','1431976','1048752','1123881','1048761','1048753','1419502','1455437','1062628','1267348','904528',
		'994898','1054340','902329','360846','407028','440237','452454','736078','361277','1243380','1192102','1192103','1243381','413415','875743','822456',
		'276669','1468706','717933','436715','1479576','692922','1018135','556396','1500340','1378275','1501149','1505072','1501187','929760','452451',
		'1505521','1505458','1195038','1505503','436668','1511932','1508853','1498923','436627','408728','360936','1235956','243755')

 group by  months,
        c.performance_province_name,
		c.performance_city_name) b 
	  on a.performance_city_name=b.performance_city_name 
		and a.months=b.months
		) a 
    group by a.performance_province_name,
		a.performance_city_name;
---------------     mingxi
select
    performance_region_name
    ,performance_province_name
    ,dc_code
    ,dc_name
    ,goods_code
    ,goods_name
    ,brand_name
    ,division_code
    ,division_name
    ,category_large_code
    ,category_large_name
    ,category_middle_code
    ,category_middle_name
    ,category_small_code
    ,category_small_name
    ,purchase_group_code
    ,purchase_group_name
    ,qm_qty
    ,qm_amt
    ,requisition_amt
    ,month_turnover_days
    ,month_of_year
   from csx_ads.csx_ads_wms_goods_turnover_df a
where a.sdt in ('20230731','20230831','20230930')  
and a.division_name='易耗品'
and a.dc_code in 
   (
'W0T5',
'W080',
'W048',
'W053',
'WB03',
'W0T3',
'W0Q3',
'W0Q4',
'W0Q1',
'W0T6',
'W0Q8',
'W088',
'W0R8',
'W0R9',
'W0P6',
'W0AS',
'W0AR',
'W0A6',
'W079',
'W0M6',
'W0P3',
'WB01',
'W039',
'W0T7',
'W0AZ'
)
	    and a.goods_code not in 
		('150140','904544','904488','81676','1232005','614996','1234069','1263534','195740','437148','452453','407010','360855','436691','452462','360900',
		'912340','926110','930553','929543','902220','444698','408725','435984','902298','406937','902343','407070','845790','1234034','741211','1210073',
		'1295393','1284067','1253936','450944','902324','475492','841823','447477','447612','447667','361998','508379','1250473','902317','902296','1235578',
		'1235612','1235577','403141','1188543','409738','850572','706239','721250','704366','346672','1238532','362238','873996','443429','873997','1093155',
		'1186407','1198595','1056043','235783','1268580','1256215','902305','437149','437150','1260353','406950','407039','407026','407015','408646','1282161',
		'1256369','1370102','440414','1256368','1420060','1256367','1407884','417979','1256366','1256365','761397','457658','1235625','1235559','902225',
		'902330','406976','1237048','1185412','858860','678884','519143','901873','1465218','942974','360852','407025','440301','452457','406974','1428096',
		'1428163','717195','408667','447637','452352','910247','850641','437154','678885','362031','519142','407041','1068062','1274251','440413','528417',
		'1371169','1313392','1313397','153304','1273659','444244','452447','360867','766273','1067931','1250245','1221784','361547','1221383','361540','361538',
		'361530','361375','361417','361425','361529','361528','361463','361938','1227333','362001','362012','408711','196194','6624','904493','360838','444760',
		'407044','1212586','360869','360864','440280','850107','1422474','360884','453278','436642','436653','440284','440317','440342','1063715','440362',
		'556397','436667','360938','837212','1253564','364128','447636','406977','997594','872507','873985','873986','441960','441973','889814','858854',
		'872544','872510','872515','872516','438979','611437','611438','437812','433059','433048','433437','433484','433044','435962','433033','433026',
		'918996','603619','585579','585578','436575','872539','362009','441991','436955','448769','448768','441965','441966','585568','441972','441978',
		'443428','441983','441982','873998','873988','873982','873980','441964','441962','873975','873976','873978','1250887','1438064','1446866','1446289',
		'1446948','1382006','440343','1426746','1195031','1432691','1423800','453276','407059','1458170','1461000','1448939','436650','436657','360874',
		'1445428','1425675','1425676','1446966','437158','961617','1461773','1488877','902287','408684','360932','1438645','324426','1434686','362015',
		'850557','407020','1433180','1246864','1246865','651935','451845','1424270','1480737','324660','1448605','391329','1468869','763150','324388',
		'324395','437167','440339','1360294','408733','436718','1466565','440252','437143','1471858','1429375','1422815','1464495','1468916','1475837',
		'1455681','910270','1465582','910271','1420230','1448594','407065','1424818','360868','436670','437168','1428867','360851','437151','1371255',
		'360881','360890','440241','407061','407060','1460269','407057','1457822','910277','437161','993293','437155','1431848','440366','1381932','1482110',
		'941552','1454546','440312','1363380','324429','1437112','1437300','408691','906260','1403622','1460909','407027','406949','1415678','902282','1430491',
		'902284','436666','1368944','1470876','1441277','1424230','1467612','910267','1438006','360931','360934','360929','367509','1463445','778115','723248',
		'723252','1421597','766272','1455467','577405','1455551','407817','1015662','1211369','403231','434026','1416932','408640','872454','1424225','902224',
		'1472937','1472936','1449780','756017','1437640','1431976','1048752','1123881','1048761','1048753','1419502','1455437','1062628','1267348','904528',
		'994898','1054340','902329','360846','407028','440237','452454','736078','361277','1243380','1192102','1192103','1243381','413415','875743','822456',
		'276669','1468706','717933','436715','1479576','692922','1018135','556396','1500340','1378275','1501149','1505072','1501187','929760','452451',
		'1505521','1505458','1195038','1505503','436668','1511932','1508853','1498923','436627','408728','360936','1235956','243755');

		/*
		-------城市服务商成功个数
select 
					customer_code,
					sum(sale_amt) sales_value
				from 
					 csx_dws.csx_dws_sale_detail_di
				where 
					sdt between '20220101' and '20220331'
					and business_type_code=4
					and channel_code in('1','7','9')
					and customer_code in 
					(
)
group by customer_code;
---------------城市服务商2.0 1.0 销售额毛利率
SELECT
 province_name,
  if(d.shop_name like '%V2DC%','2.0','1.0') ff,
  sum(a.sales_value), --总销售额
sum(profit) profit
FROM csx_dw.dws_sale_r_d_detail a
left JOIN csx_dw.dws_basic_w_a_csx_shop_m d
  ON a.dc_code = d.shop_code
where 
	sdt between '20220701' and '20220930'
	and business_type_code in ('4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	and channel_code in('1','7','9')
group by  province_name,
  if(d.shop_name like '%V2DC%','2.0','1.0')
  
  
 --现在使用 结算中心数据计算 
---库存消耗品全量
select 
sum(fact_values)/3  fact_cnt
from (
   select months,sum(begin_inventoty_amt+end_inventoty_amt)/2/sum(use_amt)*30 fact_values
   from csx_tmp.ads_fr_r_m_consumables_turnover_report a   
   where  months in ('202201','202202','202203')  and dc_code in 
                     ('W0K3',
                      'W0R8',
                      'W039',
                      'W088',
                      'W053',
                      'W079',
                      'W080',
                      'W0T6',
                      'W0Q1',
                      'W0T7',
                      'W0P6',
                      'WB01',
                      'W0P3',
                      'W0T5',
                      'WB03',
                      'W0Q8',
                      'W0S9',
                      'W0Q4',
                      'W048',
                      'W0AZ',
                      'W0M6')
			and ((category_middle_name not in ('厨具及不锈钢','点续袋','豆浆包装品','小型电器','赠品') and province_name<>'重庆市') or 
		(category_middle_name not in ('厨具及不锈钢','点续袋','豆浆包装品','连卷袋','小型电器','赠品') and province_name='重庆市'))
    group by months)a

---库存消耗品全量
select 
province_name,
city_group_name,
sum(fact_values)/3  fact_cnt
from (
   select
        months,
        c.province_name,
		c.city_group_name,
		sum(begin_inventoty_amt+end_inventoty_amt)/2/sum(use_amt)*30 fact_values
   from csx_tmp.ads_fr_r_m_consumables_turnover_report a
   join 
         (select shop_id,province_name,city_group_name from csx_dw.dws_basic_w_a_csx_shop_m cs where sdt='20220630'and cs.purpose in ('01','02','03')--1仓库2工厂3	门店
         )c on a.dc_code =c.shop_id
   where months in ('202110','202111','202112')  and dc_code in ('W0K3',
                      'W0R8',
                      'W039',
                      'W088',
                      'W053',
                      'W079',
                      'W080',
                      'W0T6',
                      'W0Q1',
                      'W0T7',
                      'W0P6',
                      'WB01',
                      'W0P3',
                      'W0T5',
                      'WB03',
                      'W0Q8',
                      'W0S9',
                      'W0Q4',
                      'W048',
                      'W0AZ',
                      'W0M6')
	    and ((category_middle_name not in ('厨具及不锈钢','点续袋','豆浆包装品','小型电器','赠品') and province_name<>'重庆市') or 
		(category_middle_name not in ('厨具及不锈钢','点续袋','豆浆包装品','连卷袋','小型电器','赠品') and province_name='重庆市'))
    group by months,
        c.province_name,
		c.city_group_name)a
    group by province_name,
city_group_name;

---库存消耗品全量
select 
sum(fact_values)/3  fact_cnt
from 
(
select 
a.smonth,
sum(a.amt)/sum(t3.fact_values-t3.fact_values11)*30 fact_values
from (
      select 
       regexp_replace(substr(a.order_time,1,7),'-','') smonth,
	   location_code,   
	   sum(if(status=1,unit_price*qty,0)) fact_values11,
       sum(if(status=0,unit_price*qty,0)) fact_values
      from csx_ods.source_mms_r_a_factory_mr_receive_return  a
      where  sdt='20220510' 
	   and   order_time>='2021-10-01' and order_time<'2022-01-01'
	   and  location_code in ('W0K3','W0R8','W039','W088','W053','W079','W080','W0T6','W0Q1','W0T7','W0P6','WB01','W0P3','W0T5',
								'WB03','W0Q8','W0S9','W0Q4','WB04','W0AZ','W0M6')
	   and mrp_key in('3062','3063') --'物料MRP属性键,3010-生鲜，3061-加工厂-主原料，3062-加工厂-包装材料', 
      group by regexp_replace(substr(a.order_time,1,7),'-',''), location_code

    )t3

join (select substr(a.sdt,1,6) smonth,dc_code,

		avg(a.amt) amt
 from(select a.sdt,dc_code,
		sum(a.amt) amt
      from (select *
     from csx_dw.dws_wms_r_d_accounting_stock_m
     where sdt in ('20211001','20211031','20211101','20211130','20211201','20211231')
      and sys='new'
      and dc_code in ('W0K3',
                      'W0R8',
                      'W039',
                      'W088',
                      'W053',
                      'W079',
                      'W080',
                      'W0T6',
                      'W0Q1',
                      'W0T7',
                      'W0P6',
                      'WB01',
                      'W0P3',
                      'W0T5',
                      'WB03',
                      'W0Q8',
                      'W0S9',
                      'W0Q4',
                      'WB04',
                      'W0AZ',
                      'W0M6')
     AND substr(reservoir_area_code, 1, 2) <> 'PD'
     AND substr(reservoir_area_code, 1, 2) <> 'TS')a
     left join
              (
                select
                  goods_id,
                  division_code, division_name
                from csx_dw.dws_basic_w_a_csx_product_m
                where sdt = '20220630'
              ) b on b.goods_id = a.goods_code
     where b.division_name='易耗品' 
     group by a.sdt,dc_code
	)a
	 group by substr(a.sdt,1,6),dc_code
)a
     on a.dc_code = t3.location_code and a.smonth=t3.smonth
group by 
a.smonth)a
;

-----库存消耗品按工厂当月领用的小类全量计算
select 
sum(fact_values)/3  fact_cnt
from 
(
select 
a.smonth,
sum(a.amt)/sum(t3.fact_values-t3.fact_values11)*30 fact_values
from (
      select 
       regexp_replace(substr(a.order_time,1,7),'-','') smonth,
	   location_code,   
      b.category_small_code,--product_code,
	  sum(if(status=1,unit_price*qty,0)) fact_values11,
      sum(if(status=0,unit_price*qty,0)) fact_values
      from csx_ods.source_mms_r_a_factory_mr_receive_return  a
	  left join
         (
           select
             goods_id,
             category_small_code
           from csx_dw.dws_basic_w_a_csx_product_m
           where sdt = '20220630'
         ) b on b.goods_id = a.product_code
      where  sdt='20220331' 
	  and   order_time>='2022-01-01' and order_time<'2022-04-01'
	  and  location_code in ('W0K3','W0R8','W039','W088','W053','W079','W080','W0T6','W0Q1','W0T7','W0P6','WB01','W0P3','W0T5',
'WB03','W0Q8','W0S9','W0Q4','WB04','W0AZ','W0M6')
	  and mrp_key in('3062','3063') --'物料MRP属性键,3010-生鲜，3061-加工厂-主原料，3062-加工厂-包装材料', 
      group by substr(a.sdt,1,6), location_code,-- product_code,  
      	b.category_small_code
    )t3

join (select substr(a.sdt,1,6) smonth,dc_code,
		a.category_small_code,
		--a.goods_code,
		avg(a.amt) amt
 from(select a.sdt,dc_code,
		a.category_small_code,
		--a.goods_code,
		sum(a.amt) amt
 from (select *
from csx_dw.dws_wms_r_d_accounting_stock_m
where sdt in ('20220101','20220131','20220201','20220228','20220301','20220331')
 and sys='new'
 and dc_code in ('W0K3',
'W0R8',
'W039',
'W088',
'W053',
'W079',
'W080',
'W0T6',
'W0Q1',
'W0T7',
'W0P6',
'WB01',
'W0P3',
'W0T5',
'WB03',
'W0Q8',
'W0S9',
'W0Q4',
'WB04',
'W0AZ',
'W0M6')
AND substr(reservoir_area_code, 1, 2) <> 'PD'
AND substr(reservoir_area_code, 1, 2) <> 'TS')a
left join
         (
           select
             goods_id,
             division_code, division_name
           from csx_dw.dws_basic_w_a_csx_product_m
           where sdt = '20220630'
         ) b on b.goods_id = a.goods_code
 where b.division_name='易耗品' 
 group by a.sdt,dc_code,
		a.category_small_code
	--	a.goods_code
	)a
	 group by substr(a.sdt,1,6),dc_code,
		a.category_small_code
)a
     on a.dc_code = t3.location_code and  a.category_small_code = t3.category_small_code and a.smonth=t3.smonth

group by 
a.smonth)a
;

--工厂耗材 库存周转天数 去掉小类 工厂系统记录口径
select 
province_name,city_group_name,
sum(fact_values)/3  fact_cnt
from 
(
/*select 
province_name,city_group_name,
smonth,
sum(fact_values)/count(category_small_code) fact_cnt
from 
(
select 
c.province_name,
c.city_group_name,
a.smonth,
--a.category_small_code,
sum(a.amt)/sum(t3.fact_values-t3.fact_values11)*30 fact_values
from (
      select 
       regexp_replace(substr(a.order_time,1,7),'-','') smonth,
	   location_code,   
	  sum(if(status=1,unit_price*qty,0)) fact_values11,
      sum(if(status=0,unit_price*qty,0)) fact_values
      from csx_ods.source_mms_r_a_factory_mr_receive_return  a
      where  sdt='20220510' 
	  and   order_time>='2021-10-01' and order_time<'2022-01-01'
	  and  location_code in ('W0K3','W0R8','W039','W088','W053','W079','W080','W0T6','W0Q1','W0T7','W0P6','WB01','W0P3','W0T5',
'WB03','W0Q8','W0S9','W0Q4','WB04','W0AZ','W0M6')
	  and mrp_key in('3062','3063') --'物料MRP属性键,3010-生鲜，3061-加工厂-主原料，3062-加工厂-包装材料', 
      group by regexp_replace(substr(a.order_time,1,7),'-',''), location_code 
    )t3

join (select substr(a.sdt,1,6) smonth,dc_code,
		--a.category_small_code,
		--a.goods_code,
		avg(a.amt) amt
 from(select a.sdt,dc_code,
		--a.category_small_code,
		--a.goods_code,
		sum(a.amt) amt
 from (select *
from csx_dw.dws_wms_r_d_accounting_stock_m
where sdt in ('20211001','20211031','20211101','20211130','20211201','20211231')
 and sys='new'
 and dc_code in ('W0K3',
'W0R8',
'W039',
'W088',
'W053',
'W079',
'W080',
'W0T6',
'W0Q1',
'W0T7',
'W0P6',
'WB01',
'W0P3',
'W0T5',
'WB03',
'W0Q8',
'W0S9',
'W0Q4',
'WB04',
'W0AZ',
'W0M6')
AND substr(reservoir_area_code, 1, 2) <> 'PD'
AND substr(reservoir_area_code, 1, 2) <> 'TS')a
left join
         (
           select
             goods_id,
             division_code, division_name
           from csx_dw.dws_basic_w_a_csx_product_m
           where sdt = '20220630'
         ) b on b.goods_id = a.goods_code
 where b.division_name='易耗品' 
 group by a.sdt,dc_code
		--a.category_small_code
	--	a.goods_code
	)a
	 group by substr(a.sdt,1,6),dc_code
		--a.category_small_code
)a
     on a.dc_code = t3.location_code --and a.category_small_code = t3.category_small_code
	 and a.smonth=t3.smonth
left join 
  (select shop_id,province_name,city_group_name 
     from csx_dw.dws_basic_w_a_csx_shop_m cs 
   where sdt='20220630'--and cs.purpose in ('01','02','03')--1仓库2工厂3	门店
  )c on a.dc_code =c.shop_id
group by c.province_name,
c.city_group_name,
a.smonth
--a.category_small_code
)a
/*group by province_name,city_group_name,
smonth)a
group by province_name,city_group_name;

--工厂耗材 库存周转天数 有小类  工厂系统记录口径
select 
province_name,city_group_name,
sum(fact_values)/3  fact_cnt
from 
(
/*select 
province_name,city_group_name,
smonth,
sum(fact_values)/count(category_small_code) fact_cnt
from 
(
select 
c.province_name,
c.city_group_name,
a.smonth,
--a.category_small_code,
sum(a.amt)/sum(t3.fact_values-t3.fact_values11)*30 fact_values
from (
      select 
       regexp_replace(substr(a.order_time,1,7),'-','') smonth,
	   location_code,   
      b.category_small_code,--product_code,
	  sum(if(status=1,unit_price*qty,0)) fact_values11,
      sum(if(status=0,unit_price*qty,0)) fact_values
      from csx_ods.source_mms_r_a_factory_mr_receive_return  a
	  left join
         (
           select
             goods_id,
             category_small_code
           from csx_dw.dws_basic_w_a_csx_product_m
           where sdt = '20220630'
         ) b on b.goods_id = a.product_code
      where  sdt='2022510' 
	  and   order_time>='2022-01-01' and order_time<'2022-04-01'
	  and  location_code in ('W0K3','W0R8','W039','W088','W053','W079','W080','W0T6','W0Q1','W0T7','W0P6','WB01','W0P3','W0T5',
'WB03','W0Q8','W0S9','W0Q4','WB04','W0AZ','W0M6')
	  and mrp_key in('3062','3063') --'物料MRP属性键,3010-生鲜，3061-加工厂-主原料，3062-加工厂-包装材料', 
      group by regexp_replace(substr(a.order_time,1,7),'-',''), location_code,-- product_code,  
      	b.category_small_code
    )t3

join (select substr(a.sdt,1,6) smonth,dc_code,
		a.category_small_code,
		--a.goods_code,
		avg(a.amt) amt
 from(select a.sdt,dc_code,
		a.category_small_code,
		--a.goods_code,
		sum(a.amt) amt
 from (select *
from csx_dw.dws_wms_r_d_accounting_stock_m
where sdt in ('20220101','20220131','20220201','20220228','20220301','20220331')
 and sys='new'
 and dc_code in ('W0K3',
'W0R8',
'W039',
'W088',
'W053',
'W079',
'W080',
'W0T6',
'W0Q1',
'W0T7',
'W0P6',
'WB01',
'W0P3',
'W0T5',
'WB03',
'W0Q8',
'W0S9',
'W0Q4',
'WB04',
'W0AZ',
'W0M6')
AND substr(reservoir_area_code, 1, 2) <> 'PD'
AND substr(reservoir_area_code, 1, 2) <> 'TS')a
left join
         (
           select
             goods_id,
             division_code, division_name
           from csx_dw.dws_basic_w_a_csx_product_m
           where sdt = '20220630'
         ) b on b.goods_id = a.goods_code
 where b.division_name='易耗品' 
 group by a.sdt,dc_code,
		a.category_small_code
	--	a.goods_code
	)a
	 group by substr(a.sdt,1,6),dc_code,
		a.category_small_code
)a
     on a.dc_code = t3.location_code and a.category_small_code = t3.category_small_code
	 and a.smonth=t3.smonth
left join 
  (select shop_id,province_name,city_group_name 
     from csx_dw.dws_basic_w_a_csx_shop_m cs 
   where sdt='20220630'--and cs.purpose in ('01','02','03')--1仓库2工厂3	门店
  )c on a.dc_code =c.shop_id
group by c.province_name,
c.city_group_name,
a.smonth
--a.category_small_code
)a
/*group by province_name,city_group_name,
smonth)a
group by province_name,city_group_name;*/