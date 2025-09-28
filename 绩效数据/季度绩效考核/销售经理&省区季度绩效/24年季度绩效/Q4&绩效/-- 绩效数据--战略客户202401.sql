-- 绩效数据--战略客户
--  -战略客户new
--
SELECT 
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from csx_dws.csx_dws_sale_detail_di a
left join
--    客户信息
(
  select 
    customer_code,
	customer_name  
  from csx_dim.csx_dim_crm_customer_info 
  where sdt='20230930'
  and customer_code<>''
  and channel_code in('1','7','9')
)b on b.customer_code=a.customer_code
where  a.channel_code in ('1','7','9')--  and  a.business_type_name  in ('城市服务商','BBC','日配业务','福利业务')  
and a.customer_code in 
('120416'
,'123755'
,'125201'
,'123706'
,'124579'
,'117217'
,'130536'
,'130983'
,'164047'
,'228532'
,'130971'
,'114982'
,'235304'
,'235502'
,'235446'
,'234928'
,'195022'
,'234959'
,'235291'
,'235165'
,'235408'
,'235463'
,'235874'
,'122860'
,'234958'
,'127861'
,'124524'
,'130087'
,'130369'
,'130625'
,'131129'
,'131162'
,'131187'
,'131146'
,'131041'
,'130750'
,'130928'
,'128661'
,'130899'
,'224480'
,'225477'
,'129307'
,'201599'
,'224120'
,'155386'
,'130145'
,'232646'
,'225902'
,'239912'
,'239884'
,'223559'
,'224016'
,'232023'
,'230158'
,'226389'
,'225776'
,'230272'
,'233599'
,'115210'
,'224759'
,'230931'
,'229557'
,'228747'
,'235929'
,'232479'
,'236416'
,'237705'
,'125089'
,'125662'
,'123559'
,'231095'
,'228749'
,'228401'
,'231320'
,'234827'
,'123561'
,'130571'
,'113758'
,'113779'
,'113809'
,'113837'
,'113850'
,'113870'
,'113906'
,'114540'
,'114548'
,'114613'
,'114693'
,'114706'
,'115692'
,'115703'
,'117349'
,'117374'
,'117405'
,'117424'
,'117444'
,'117448'
,'117450'
,'118084'
,'118396'
,'118466'
,'119376'
,'119377'
,'119898'
,'120056'
,'120101'
,'120366'
,'120490'
,'120681'
,'120755'
,'120930'
,'122620'
,'122738'
,'122781'
,'124211'
,'124230'
,'124597'
,'125233'
,'126147'
,'130449'
,'130563'
,'130849'
,'130867'
,'227857'
,'227927'
,'117927'
,'226569'
,'127123'
,'130941'
,'228446'
,'230760'
,'114477'
,'237857'
,'227579'
,'228190'
,'228748'
,'229231'
,'228516'
,'158226'
,'159226'
,'228541'
,'212969'
,'232846'
,'232923'
,'128521'
,'128362'
,'128363'
,'128453'
,'128454'
,'128458'
,'128459'
,'128481'
,'128482'
,'128489'
,'128496'
,'128508'
,'128509'
,'128511'
,'128512'
,'128515'
,'128516'
,'128517'
,'128518'
,'128519'
,'128520'
,'128522'
,'128523'
,'128524'
,'128525'
,'128526'
,'128527'
,'128530'
,'128531'
,'128532'
,'128533'
,'128534'
,'128560'
,'128535'
,'128536'
,'128537'
,'128538'
,'128540'
,'128541'
,'128545'
,'128546'
,'128548'
,'128550'
,'128555'
,'128556'
,'128557'
,'128559'
,'128565'
,'128567'
,'128570'
,'128573'
,'128575'
,'128576'
,'234884'
,'236564'
,'237765'
,'237723'
,'236840'
)
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by a.customer_code,b.customer_name,a.business_type_name;


-- 线上报表--战略负责人
select * from
desc csx_dim.csx_dim_crm_customer_info where sdt='current'
;

-- 增加月份

SELECT 
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  strategy_user_name,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from csx_dws.csx_dws_sale_detail_di a
left  join
--    客户信息
(
  select 
    customer_code,
	customer_name ,
	strategy_status,
	strategy_user_name
  from csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  and customer_code<>''
  and channel_code in('1','7','9')
  
)b on b.customer_code=a.customer_code
where  a.channel_code in ('1','7','9')--  and  a.business_type_name  in ('城市服务商','BBC','日配业务','福利业务')  
and a.sdt<='20231231' 
and a.sdt>='20231001'
and b.strategy_status =1 
group by a.customer_code,b.customer_name,a.business_type_name,strategy_user_name
;

SELECT substr(sdt,1,6) mon,
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  strategy_user_name,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from csx_dws.csx_dws_sale_detail_di a
left  join
--    客户信息
(
  select 
  customer_code,
	customer_name ,
	strategy_status,
	strategy_user_name
  from csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  and customer_code<>''
  and channel_code in('1','7','9')
  
)b on b.customer_code=a.customer_code
where  a.channel_code in ('1','7','9')--  and  a.business_type_name  in ('城市服务商','BBC','日配业务','福利业务')  
and a.sdt>='20240101' 
and a.sdt<='20240531'
and b.strategy_status =1 
group by a.customer_code,b.customer_name,a.business_type_name,strategy_user_name,
substr(sdt,1,6)
;