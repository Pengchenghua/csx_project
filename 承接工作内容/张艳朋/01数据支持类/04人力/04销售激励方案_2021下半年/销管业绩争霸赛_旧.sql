-- 月初取上个月，例2020-09-01
set i_sdate = '${START_DATE}';

-- 销售员 破冰王 新开客户
SELECT
  concat_ws('-', from_unixtime(unix_timestamp(trunc(date_sub(${hiveconf:i_sdate},1),'MM'), 'yyyy-MM-dd'),'MMdd'),
    from_unixtime(unix_timestamp(date_sub(${hiveconf:i_sdate},1), 'yyyy-MM-dd'),'MMdd')) AS statistic_month,
  province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name,
  sales_value, front_profit, front_profit_rate, cust_count, avg_sales_value
FROM
(
  SELECT
    province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name,
    sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
    sum(front_profit) / sum(sales_value) AS front_profit_rate, count(DISTINCT customer_no) AS cust_count,
    sum(avg_sales_value) as avg_sales_value
  FROM
  (
    SELECT
      a.first_date, b.customer_no, b.province_name, b.city_real,
	  b.supervisor_work_no, b.supervisor_name, b.work_no, b.sales_name,
      sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
      sum(sales_value) / (datediff(date_sub(${hiveconf:i_sdate},1), a.first_date)+1) AS avg_sales_value
    FROM
    (
      SELECT customer_no, from_unixtime(unix_timestamp(first_order_date,'yyyyMMdd'),'yyyy-MM-dd') AS first_date
      FROM csx_dw.dws_crm_r_a_customer_active_info
      WHERE sdt = regexp_replace(date_sub(${hiveconf:i_sdate}, 1), '-', '')
        AND first_order_date >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND first_order_date < regexp_replace(${hiveconf:i_sdate}, '-', '')
    ) a JOIN
    (
      SELECT customer_no, province_name, city_real, supervisor_work_no, supervisor_name,
        work_no, sales_name, sales_value, front_profit
      FROM csx_dw.dws_sale_r_d_customer_sale
      WHERE sdt >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND sdt < regexp_replace(${hiveconf:i_sdate}, '-', '')
        AND sales_type IN ('qyg', 'bbc') AND channel IN ('1', '7')
        AND attribute_code IN (1, 2) AND first_category_code IN ('21', '23') 
    ) b ON a.customer_no = b.customer_no
    JOIN
	(
	  SELECT customer_no FROM csx_dw.dws_crm_w_a_customer_m_v1
      WHERE sdt = regexp_replace(date_sub(${hiveconf:i_sdate}, 1), '-', '') AND source = 'crm' AND position = 'SALES'
	) c ON b.customer_no = c.customer_no
    GROUP BY a.first_date, b.customer_no, b.province_name, b.city_real,
      b.supervisor_work_no, b.supervisor_name, b.work_no, b.sales_name
  ) tmp
  GROUP BY province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name
) tmp2
WHERE front_profit_rate > 0.03 AND avg_sales_value >= 1000
ORDER BY cust_count DESC, sales_value DESC;


-- 销售员 增长王 业绩增长
SELECT
  concat_ws('-', from_unixtime(unix_timestamp(trunc(date_sub(${hiveconf:i_sdate},1),'MM'), 'yyyy-MM-dd'),'MMdd'),
    from_unixtime(unix_timestamp(date_sub(${hiveconf:i_sdate},1), 'yyyy-MM-dd'),'MMdd')) AS statistic_month,
  province_name,
  city_real,
  supervisor_work_no,
  supervisor_name,
  work_no,
  sales_name,
  sales_value,
  front_profit,
  front_profit_rate,
  value2,
  profit2 / value2 as profit_rate2,
  month_growth
FROM
(
  SELECT
    a.province_name, a.city_real, a.supervisor_work_no, a.supervisor_name, a.work_no, a.sales_name,
    sum(a.sales_value) AS sales_value, sum(a.front_profit) AS front_profit,
    sum(a.front_profit) / sum(a.sales_value) AS front_profit_rate,
    coalesce(sum(b.sales_value), 0) AS value2, coalesce(sum(b.front_profit), 0) AS profit2,
    sum(a.sales_value) - coalesce(sum(b.sales_value), 0) AS month_growth
  FROM
  (
    SELECT customer_no, province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name,
      sum(sales_value) AS sales_value, sum(front_profit) AS front_profit
    FROM csx_dw.dws_sale_r_d_customer_sale
    WHERE sdt >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
      AND sdt < regexp_replace(${hiveconf:i_sdate}, '-', '')
      AND sales_type IN ('qyg', 'bbc') AND channel IN ('1', '7')
      AND attribute_code IN (1, 2) AND first_category_code IN ('21', '23')
    GROUP BY customer_no, province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name
  ) a LEFT JOIN
  (
    SELECT customer_no, sum(sales_value) AS sales_value, sum(front_profit) AS front_profit
    FROM csx_dw.dws_sale_r_d_customer_sale
    WHERE sdt >= regexp_replace(add_months(trunc(date_sub(${hiveconf:i_sdate},1),'MM'),-1),'-','')
      AND sdt < regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
      AND sales_type IN ('qyg', 'bbc') AND channel IN ('1', '7')
    GROUP BY customer_no
  ) b ON a.customer_no = b.customer_no
  JOIN
  (
    SELECT customer_no FROM csx_dw.dws_crm_w_a_customer_m_v1
    WHERE sdt = regexp_replace(date_sub(${hiveconf:i_sdate}, 1), '-', '') AND source = 'crm' AND position = 'SALES'
  ) c ON a.customer_no = c.customer_no
  GROUP BY a.province_name, a.city_real, a.supervisor_work_no, a.supervisor_name, a.work_no, a.sales_name
) tmp
WHERE month_growth >= 50000 AND front_profit_rate > 0.05
ORDER BY month_growth DESC;


-- 销售员 BBC突破 BBC业绩突破
SELECT
  concat_ws('-', from_unixtime(unix_timestamp(trunc(date_sub(${hiveconf:i_sdate},1),'MM'), 'yyyy-MM-dd'),'MMdd'),
    from_unixtime(unix_timestamp(date_sub(${hiveconf:i_sdate},1), 'yyyy-MM-dd'),'MMdd')) AS statistic_month,
  province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name,
  sales_value, front_profit, front_profit_rate, cust_count, avg_sales_value
FROM
(
  SELECT
    province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name,
    sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
    sum(front_profit) / sum(sales_value) AS front_profit_rate, count(DISTINCT customer_no) AS cust_count,
    sum(avg_sales_value) as avg_sales_value
  FROM
  (
    SELECT
      a.first_date, b.customer_no, b.province_name, b.city_real, b.supervisor_work_no,
      b.supervisor_name, b.work_no, b.sales_name,
      sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
      sum(sales_value) / (datediff(date_sub(${hiveconf:i_sdate},1), a.first_date)+1) AS avg_sales_value
    FROM
    (
      SELECT customer_no, from_unixtime(unix_timestamp(first_order_date,'yyyyMMdd'),'yyyy-MM-dd') AS first_date
      FROM csx_dw.dws_crm_r_a_customer_active_info
      WHERE sdt = regexp_replace(date_sub(${hiveconf:i_sdate}, 1), '-', '')
        AND first_order_date >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND first_order_date < regexp_replace(${hiveconf:i_sdate}, '-', '')
    ) a JOIN
    (
      SELECT customer_no, province_name, city_real, supervisor_work_no, supervisor_name,
        work_no, sales_name, sales_value, front_profit
      FROM csx_dw.dws_sale_r_d_customer_sale
      WHERE sdt >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND sdt < regexp_replace(${hiveconf:i_sdate}, '-', '')
        AND sales_type = 'bbc' AND channel = '7'
        AND attribute_code IN (1, 2) AND first_category_code IN ('21', '23')
    ) b ON a.customer_no = b.customer_no
	JOIN
	(
	  SELECT customer_no FROM csx_dw.dws_crm_w_a_customer_m_v1
      WHERE sdt = regexp_replace(date_sub(${hiveconf:i_sdate}, 1), '-', '') AND source = 'crm' AND position = 'SALES'
	) c ON b.customer_no = c.customer_no
    GROUP BY a.first_date, b.customer_no, b.province_name, b.city_real,
      b.supervisor_work_no, b.supervisor_name, b.work_no, b.sales_name
  ) tmp
  GROUP BY province_name, city_real, supervisor_work_no, supervisor_name, work_no, sales_name
) tmp2
WHERE front_profit_rate > 0.05 AND avg_sales_value >= 700
ORDER BY cust_count DESC, sales_value DESC;



-- 销售主管 破冰王 新开客户
SELECT
  concat_ws('-', from_unixtime(unix_timestamp(trunc(date_sub(${hiveconf:i_sdate},1),'MM'), 'yyyy-MM-dd'),'MMdd'),
    from_unixtime(unix_timestamp(date_sub(${hiveconf:i_sdate},1), 'yyyy-MM-dd'),'MMdd')) AS statistic_month,
  province_name, city_real, supervisor_work_no, supervisor_name,
  sales_value, front_profit, front_profit_rate, cust_count, avg_sales_value
FROM
(
  SELECT
    province_name, city_real, supervisor_work_no, supervisor_name,
    sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
    sum(front_profit) / sum(sales_value) AS front_profit_rate, count(DISTINCT customer_no) AS cust_count,
    sum(avg_sales_value) as avg_sales_value
  FROM
  (
    SELECT
      a.first_date, b.customer_no, b.province_name, b.city_real, b.supervisor_work_no, b.supervisor_name,
      sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
      sum(sales_value) / (datediff(date_sub(${hiveconf:i_sdate},1), a.first_date)+1) AS avg_sales_value
    FROM
    (
      SELECT customer_no, from_unixtime(unix_timestamp(first_order_date,'yyyyMMdd'),'yyyy-MM-dd') AS first_date
      FROM csx_dw.dws_crm_r_a_customer_active_info
      WHERE sdt = regexp_replace(date_sub(${hiveconf:i_sdate}, 1), '-', '')
        AND first_order_date >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND first_order_date < regexp_replace(${hiveconf:i_sdate}, '-', '')
    ) a JOIN
    (
      SELECT customer_no, province_name, city_real, supervisor_work_no, supervisor_name, sales_value, front_profit
      FROM csx_dw.dws_sale_r_d_customer_sale
      WHERE sdt >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND sdt < regexp_replace(${hiveconf:i_sdate}, '-', '')
        AND sales_type IN ('qyg', 'bbc') AND channel IN ('1', '7')
        AND attribute_code IN (1, 2) AND first_category_code IN ('21', '23')
    ) b ON a.customer_no = b.customer_no
    GROUP BY a.first_date, b.customer_no, b.province_name, b.city_real, b.supervisor_work_no, b.supervisor_name
  ) tmp
  GROUP BY province_name, city_real, supervisor_work_no, supervisor_name
) tmp2
WHERE front_profit_rate > 0.03 AND avg_sales_value >= 1000
ORDER BY cust_count DESC, sales_value DESC;


-- 销售主管 BBC突破 BBC业绩突破
SELECT
  concat_ws('-', from_unixtime(unix_timestamp(trunc(date_sub(${hiveconf:i_sdate},1),'MM'), 'yyyy-MM-dd'),'MMdd'),
    from_unixtime(unix_timestamp(date_sub(${hiveconf:i_sdate},1), 'yyyy-MM-dd'),'MMdd')) AS statistic_month,
  province_name, city_real, supervisor_work_no, supervisor_name,
  sales_value, front_profit, front_profit_rate, cust_count, avg_sales_value
FROM
(
  SELECT
    province_name, city_real, supervisor_work_no, supervisor_name,
    sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
    sum(front_profit) / sum(sales_value) AS front_profit_rate, count(DISTINCT customer_no) AS cust_count,
    sum(avg_sales_value) as avg_sales_value
  FROM
  (
    SELECT
      a.first_date, b.customer_no, b.province_name, b.city_real, b.supervisor_work_no, b.supervisor_name,
      sum(sales_value) AS sales_value, sum(front_profit) AS front_profit,
      sum(sales_value) / (datediff(date_sub(${hiveconf:i_sdate},1), a.first_date)+1) AS avg_sales_value
    FROM
    (
      SELECT customer_no, from_unixtime(unix_timestamp(first_order_date,'yyyyMMdd'),'yyyy-MM-dd') AS first_date
      FROM csx_dw.dws_crm_r_a_customer_active_info
      WHERE sdt = regexp_replace(date_sub(${hiveconf:i_sdate}, 1), '-', '')
        AND first_order_date >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND first_order_date < regexp_replace(${hiveconf:i_sdate}, '-', '')
    ) a JOIN
    (
      SELECT customer_no, province_name, city_real, supervisor_work_no, supervisor_name, sales_value, front_profit
      FROM csx_dw.dws_sale_r_d_customer_sale
      WHERE sdt >= regexp_replace(trunc(date_sub(${hiveconf:i_sdate}, 1), 'MM'), '-', '')
        AND sdt < regexp_replace(${hiveconf:i_sdate}, '-', '')
        AND sales_type = 'bbc' AND channel = '7'
        AND attribute_code IN (1, 2) AND first_category_code IN ('21', '23')
    ) b ON a.customer_no = b.customer_no
    GROUP BY a.first_date, b.customer_no, b.province_name, b.city_real, b.supervisor_work_no, b.supervisor_name
  ) tmp
  GROUP BY province_name, city_real, supervisor_work_no, supervisor_name
) tmp2
WHERE front_profit_rate > 0.05 AND avg_sales_value >= 700
ORDER BY cust_count DESC, sales_value DESC;
