select
  month_of_year,
  basic_performance_region_name,
  province_name,
  city_name,
  classify_large_name,
  classify_middle_name,
	-- 月至今
--   COUNT(is_qty_satisfy ) AS totalOrderCount,  -- 总数
  COUNT(CASE WHEN ( is_punctuality_status = 0 OR is_punctuality_status = 1 ) THEN is_punctuality_status ELSE NULL END ) AS deliveryTotalOrderZSCount,  -- 配送准时base
  COUNT(CASE WHEN is_punctuality_status = 0 THEN is_punctuality_status ELSE NULL END ) AS deliveryPunctualityOrderCount,  -- 配送准时
  COUNT(CASE WHEN is_qty_satisfy = 1 THEN 0 ELSE NULL END) AS satisfyOrderCount,  -- 到货准确满足
  SUM(CASE WHEN (is_punctuality_status = 0 AND is_qty_satisfy = 1 ) THEN 1 ELSE 0 END) AS deliveryReliableOrderCount,  -- 配送准时&到货准确满足
  COUNT(CASE WHEN is_qty_satisfy = 1 THEN 0 ELSE NULL END)/COUNT(is_qty_satisfy ) AS satisfyOrderCount_rate ,-- 到货准确满足率
  COUNT(CASE WHEN is_punctuality_status = 0 THEN is_punctuality_status ELSE NULL END )/COUNT(is_qty_satisfy ) AS deliveryPunctualityOrderCount_rate, -- 配送准时率
  SUM(CASE WHEN (is_punctuality_status = 0 AND is_qty_satisfy = 1 ) THEN 1 ELSE 0 END) /COUNT(is_qty_satisfy ) AS deliveryReliableOrderCount_rate -- 到货可靠率
from
  csx_analyse_tmp.csx_ads_scm_supplier_evaluation_detail_1d03 
where
  sdt >= '20240101'
  and sdt <= '20251231' --   and province_name='安徽'
  and source_type in (1, 10, 19, 23)
  and location_code not in ('W0BD','W0T0','WC51')
group by
   month_of_year,
  basic_performance_region_name,
  province_name,
  city_name,
  classify_large_name,
  classify_middle_name