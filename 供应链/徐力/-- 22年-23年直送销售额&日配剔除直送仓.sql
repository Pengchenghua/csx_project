-- 22年-23年直送销售额&日配剔除直送仓
    select 
    year,
    performance_province_name,
    classify_large_name_flag,
    classify_middle_name,
    sum(case when delivery_type_code='直送' then sale_amt end )zs_sale,
    sum(case when delivery_type_code='直送' then profit end )zs_proit,
    sum(sale_amt) sale_amt,
    sum(profit) profit
    from (select substr(sdt,1,6) as year,
      performance_province_name,
      if(
        a.classify_large_name in ('肉禽水产', '蔬菜水果', '干货加工'),
        a.classify_large_name,
        '食百'
      ) as classify_large_name_flag,
     -- classify_large_name,
    --  shop_low_profit_flag,
    classify_middle_name,
      case when (date_format(order_time,'HH')>='22' or date_format(order_time,'HH')<='02') and a.direct_delivery_type_name='普通' then '临时加单' 
            else direct_delivery_type_name end  direct_delivery_type_name,
    if(delivery_type_code=2,'直送','配送')   delivery_type_code,
    --   delivery_type_name,
      sum(sale_amt) sale_amt,
      sum(profit) profit
    from
      csx_analyse.csx_analyse_bi_sale_detail_di a
    where
      sdt >= '20220101'
      and sdt <=  '20231231'
      and channel_code in('1', '7', '9')
      and business_type_code in (1)
      and shop_low_profit_flag!=1
    group by
      if(
        a.classify_large_name in ('肉禽水产', '蔬菜水果', '干货加工'),
        a.classify_large_name,
        '食百'
      ),
      --classify_large_name,
    --  shop_low_profit_flag,
      case when (date_format(order_time,'HH')>='22' or date_format(order_time,'HH')<='02') and a.direct_delivery_type_name='普通' then '临时加单' 
            else direct_delivery_type_name end ,
      if(delivery_type_code=2,'直送','配送') ,
    --   delivery_type_name,
      performance_province_name
      ,substr(sdt,1,6),classify_middle_name
      ) a 
      group by  year,
    performance_province_name,
    classify_large_name_flag,
    classify_middle_name