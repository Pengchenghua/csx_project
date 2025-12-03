-- 绩效数据--战略客户部
-- 战略客户关联商机20250914
WITH tmp_sale_detail AS (
  SELECT 
    SUBSTR(sdt, 1, 4) AS years,
    SUBSTR(sdt, 1, 6) AS mon,
    a.performance_region_name,
    a.performance_province_name,
    a.customer_code,
    b.customer_name,
    a.sign_company_code,    -- 签约公司
    a.business_type_name,
    b.strategy_user_number,
    b.strategy_user_name,
    b.second_category_name,
    SUM(sale_amt) AS sale_amt,
    SUM(a.profit) AS profit,
    CASE 
      WHEN SUM(a.sale_amt) != 0 THEN SUM(a.profit) / ABS(SUM(a.sale_amt)) 
      ELSE 0 
    END AS profit_rate,
    SUM(CASE WHEN partner_type_code IN (1,3) THEN sale_amt END) AS partner_sale_amt,
    SUM(CASE WHEN partner_type_code IN (1,3) THEN profit END) AS partner_profit
  FROM   csx_dws.csx_dws_sale_detail_di a
  LEFT JOIN (
    -- 客户信息
    SELECT 
      customer_code,
      customer_name,
      strategy_status,
      strategy_user_number,
      strategy_user_name,
      first_category_name,
      second_category_name,
      third_category_name
    FROM     csx_dim.csx_dim_crm_customer_info 
    WHERE sdt = 'current'
      AND customer_code <> ''
      AND channel_code IN ('1','7','9')
  ) b ON b.customer_code = a.customer_code
  WHERE a.channel_code IN ('1','7','9')
    AND a.sdt >= '${sdate}' 
    AND a.sdt <= '${edate}'
    AND (b.strategy_status = 1 OR a.customer_code IN ('243884','232923'))
  GROUP BY 
    a.customer_code,
    b.customer_name,
    b.strategy_user_number,
    a.business_type_name,
    b.strategy_user_name,
    SUBSTR(sdt, 1, 6),
    SUBSTR(sdt, 1, 4),
    a.performance_province_name,
    a.performance_region_name,
    a.sign_company_code,
    b.second_category_name
),
business_info AS (
  SELECT * FROM (
    SELECT 
      business_number,
      customer_code,
      business_sign_time,
      CASE 
        WHEN business_attribute_name IN ('日配','福利') THEN CONCAT(business_attribute_name, '业务') 
        ELSE business_attribute_name 
      END AS business_attribute_name,
      SUBSTR(REPLACE(CAST(business_sign_time AS STRING), '-', ''), 1, 4) AS sye,
      owner_user_number,
      owner_user_name,
      estimate_contract_amount,
      ROW_NUMBER() OVER (
        PARTITION BY customer_code, business_attribute_name, SUBSTR(REPLACE(CAST(business_sign_time AS STRING), '-', ''), 1, 4) 
        ORDER BY business_sign_time DESC
      ) AS rn
    FROM csx_dim.csx_dim_crm_business_info 
    WHERE sdt = 'current' 
      AND business_stage = 5
  ) t WHERE rn = 1
)
SELECT 
  a.years,
  a.mon,
  a.performance_region_name,
  a.performance_province_name,
  a.customer_code,
  a.customer_name,
  a.sign_company_code,
  a.second_category_name,
  a.business_type_name,  
  a.strategy_user_number,
  a.strategy_user_name,
  a.sale_amt,
  a.profit,
  a.profit_rate,
  a.partner_sale_amt,
  a.partner_profit,
  CASE 
    WHEN a.partner_sale_amt != 0 THEN a.partner_profit / a.partner_sale_amt 
    ELSE 0 
    END AS partner_profit_rate,
  CASE 
    WHEN a.mon = d.first_business_sign_month THEN '是' 
    ELSE '否' 
  END AS is_new_flag,
  b.business_number,
  b.business_sign_time,
  b.estimate_contract_amount,
  b.owner_user_number,
  b.owner_user_name, 
  d.last_business_sale_date
FROM tmp_sale_detail a  
LEFT JOIN business_info b
  ON a.customer_code = b.customer_code 
  AND b.sye = a.years 
  AND a.business_type_name = b.business_attribute_name
LEFT JOIN (
  SELECT 
    customer_code,
    business_type_name,
    last_business_sale_date, 
    SUBSTR(first_business_sign_date, 1, 6) AS first_business_sign_month
  FROM csx_dws.csx_dws_crm_customer_business_active_di 
  WHERE sdt = 'current'
    AND last_business_sale_date <= '${edate}'
) d ON a.customer_code = d.customer_code 
  AND a.business_type_name = d.business_type_name
  ;
  

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
where  
a.channel_code <>2 --  and  a.business_type_name  in ('城市服务商','BBC','日配业务','福利业务')  
and a.sdt<'20250901' 
and a.sdt>='20250101'
and b.strategy_status =1 
group by a.customer_code,b.customer_name,a.business_type_name,strategy_user_name
;

SELECT substr(sdt,1,6) mon,
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  strategy_user_name,
  -- sign_company_code,
  agreement_company_code,
--   agreement_dc_code,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from   csx_dws.csx_dws_sale_detail_di a
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
  -- and channel_code in('1','7','9')
  
)b on b.customer_code=a.customer_code
left join 
(select shop_code,company_code as agreement_company_code from csx_dim.csx_dim_shop where sdt='current') c on a.agreement_dc_code=c.shop_code
left join 
(select customer_code,business_type_code,last_business_sale_date from csx_dim.csx_dws_crm_customer_business_active_di where sdt='current')
where  a.channel_code in ('1','7','9')--  and  a.business_type_name  in ('城市服务商','BBC','日配业务','福利业务')  
and a.sdt>='20250701' 
and a.sdt<='20250831'
    and (b.strategy_status =1 or a.customer_code in('243884','232923'))

group by a.customer_code,b.customer_name,a.business_type_name,strategy_user_name,
substr(sdt,1,6)
-- sign_company_code,
-- agreement_dc_code,
,agreement_company_code
;


-- 战略客户关联商机
with tmp_sale_detail as 
(
SELECT substr(sdt,1,4)yesrs,
 substr(sdt,1,6) mon,
  a.performance_region_name,
  a.performance_province_name,
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  strategy_user_name,
  sign_company_code,
  agreement_company_code,
--   agreement_dc_code,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from   csx_dws.csx_dws_sale_detail_di a
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
left join 
(select shop_code,company_code as agreement_company_code from csx_dim.csx_dim_shop where sdt='current') c on a.agreement_dc_code=c.shop_code
where  a.channel_code in ('1','7','9')--  and  a.business_type_name  in ('城市服务商','BBC','日配业务','福利业务')  
and a.sdt>='20250601' 
and a.sdt<='20250831'
    and (b.strategy_status =1 or a.customer_code in('243884','232923'))

group by a.customer_code,b.customer_name,a.business_type_name,strategy_user_name,
substr(sdt,1,6)
sign_company_code,
-- agreement_dc_code,
agreement_company_code,
substr(sdt,1,4),
performance_province_name,
performance_region_name
)
select a.*,b.business_sign_time,business_number, owner_user_number,
    owner_user_name, last_business_sale_date from tmp_sale_detail a  
left join 
(select  
    business_number,
    customer_code,
    business_sign_time,
    substr(regexp_replace(to_date(business_sign_time),'-',''),1,4) sye,
    owner_user_number,
    owner_user_name,
    row_rank() over(partition by customer_code,business_att order by business_sign_time desc) rn
  from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
        and business_stage=5
) b
on a.customer_code=b.customer_code and sye=a.yesrs
left join 
(select customer_code,business_type_name,last_business_sale_date 
from   csx_dws.csx_dws_crm_customer_business_active_di 
where sdt='current') d on a.customer_code=d.customer_code and a.business_type_name=d.business_type_name

;



-- 杨青青指定客户销售



SELECT substr(sdt,1,6) mon,
    performance_region_name,
    performance_province_name,
 case when a.customer_code in ('131146'
,'131129'
,'232646') then '131146'
when a.customer_code in ('250319','249548') then '250319'
 when a.customer_code in ('252119'
,'252121'
,'252122'
,'252123'
,'252124'
,'252125'
,'252160'
,'252170'
,'252323'
,'252117'
,'252118'
,'252120'
,'252171') then '252119' 
 when a.customer_code in ('123755',
'152748',
'206621',
'220820',
'124579',
'130536',
'205173',
'122860',
'123706',
'128743',
'130971',
'202792',
'220536',
'223302',
'237125',
'249342',
'255628',
'165280',
'175823',
'130536',
'162338',
'130983',
'205173') and business_type_code=6 then '123755'
when a.customer_code in ('205173','123755') and business_type_code in (2,10) then '205173'
 else a.customer_code end new_cust,
case 
when a.customer_code in ('131146'
,'131129'
,'232646') then '融通石家庄'
when a.customer_code in ('252119'
,'252121'
,'252122'
,'252123'
,'252124'
,'252125'
,'252160'
,'252170'
,'252323'
,'252117'
,'252118'
,'252120'
,'252171') then '杞县教育局' 
 when a.customer_code in ('123755',
'152748',
'206621',
'220820',
'124579',
'130536',
'205173',
'122860',
'123706',
'128743',
'130971',
'202792',
'220536',
'223302',
'237125',
'249342',
'255628',
'165280',
'175823',
'130536',
'162338',
'130983',
'205173') and business_type_code =6 then '中国建设银行股份有限公司'
when a.customer_code in ('205173','123755') and business_type_code in (2,10) then '中国建设银行股份有限公司'
when a.customer_code in ('250319','249548') then '中国人民解放军32144部队'
  else a.customer_name end new_cust_name,
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  strategy_user_name,
  sum(sale_amt)/10000 sale_amt,
  sum(a.profit)/10000 profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from   csx_dws.csx_dws_sale_detail_di a
left  join
--    客户信息
(
  select 
    customer_code,
	customer_name ,
	strategy_status,
	strategy_user_name,
	bloc_name
  from      csx_dim.csx_dim_crm_customer_info 
  where sdt='current'
  and customer_code<>''
--   and channel_code in('1','7','9')
  
)b on b.customer_code=a.customer_code
left join 
(select shop_code,company_code as agreement_company_code from csx_dim.csx_dim_shop where sdt='current') c on a.agreement_dc_code=c.shop_code
 
where   a.sdt>='20250801' 
and a.sdt<='20250827'
and a.customer_code in ('131146'
,'131129'
,'232646'
,'251677'
,'155386'
,'239912'
,'239884'
,'252119'
,'252121'
,'252122'
,'252123'
,'252124'
,'252125'
,'252160'
,'252170'
,'252323'
,'252117'
,'252118'
,'252120'
,'252171'
,'232923'
,'251868'
,'228748'
,'227579'
,'250219'
,'252157'
,'212969'
,'159226'
,'228541'
,'247865'
,'228190'
,'252155'
,'126157'
,'257822'
,'228011'
,'236564'
,'250319'
,'249548'
,'256068'
,'255475'
,'117217'
,'254392'
,'254161'
,'154565'
,'164047'
,'123755'
,'152748'
,'206621'
,'220820'
,'124579'
,'130536'
,'205173'
,'122860'
,'123706'
,'128743'
,'130971'
,'202792'
,'220536'
,'223302'
,'237125'
,'249342'
,'255628'
,'165280'
,'175823'
,'205173'
,'123755'
,'130536'
,'162338'
,'130983'
,'205173'
,'125306'
,'126387'
,'125301'
,'103997'
,'125394'
,'258261'
,'155386'
,'239912'
,'239884'

)  

group by  substr(sdt,1,6)  ,
    performance_region_name,
    performance_province_name,
    a.customer_code,
  b.customer_name,
  a.business_type_name,
  strategy_user_name,
  case when a.customer_code in ('131146'
,'131129'
,'232646') then '131146'
when a.customer_code in ('250319','249548') then '250319'
 when a.customer_code in ('252119'
,'252121'
,'252122'
,'252123'
,'252124'
,'252125'
,'252160'
,'252170'
,'252323'
,'252117'
,'252118'
,'252120'
,'252171') then '252119' 
 when a.customer_code in ('123755',
'152748',
'206621',
'220820',
'124579',
'130536',
'205173',
'122860',
'123706',
'128743',
'130971',
'202792',
'220536',
'223302',
'237125',
'249342',
'255628',
'165280',
'175823',
'130536',
'162338',
'130983',
'205173') and business_type_code=6 then '123755'
when a.customer_code in ('205173','123755') and business_type_code in (2,10) then '205173'
 else a.customer_code end ,
case when a.customer_code in ('131146','131129','232646') then '融通石家庄'
when a.customer_code in ('252119'
,'252121'
,'252122'
,'252123'
,'252124'
,'252125'
,'252160'
,'252170'
,'252323'
,'252117'
,'252118'
,'252120'
,'252171') then '杞县教育局' 
 when a.customer_code in ('123755',
'152748',
'206621',
'220820',
'124579',
'130536',
'205173',
'122860',
'123706',
'128743',
'130971',
'202792',
'220536',
'223302',
'237125',
'249342',
'255628',
'165280',
'175823',
'130536',
'162338',
'130983',
'205173') and business_type_code=6 then '中国建设银行股份有限公司'
when a.customer_code in ('205173','123755') and business_type_code in (2,10) then '中国建设银行股份有限公司'
when a.customer_code in ('250319','249548') then '中国人民解放军32144部队'
  else a.customer_name end 
;