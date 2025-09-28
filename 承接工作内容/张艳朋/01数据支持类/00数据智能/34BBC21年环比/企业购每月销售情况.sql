
SELECT
  a.*, (a.sale_amt-b.sale_amt2)/b.sale_amt2 as a, (a.cnt-b.cnt2)/b.cnt2 as b
from
	(
    SELECT 
		substr(sdt, 1, 6) smt,
		sum(sale_amt) sale_amt, count(distinct original_order_code) as cnt
    from 
		csx_dws.csx_dws_bbc_sale_detail_di
    where 
		sdt >= '20210101' and sdt < '20220101'
    group by 
		substr(sdt, 1, 6)
	) a 
	left join
		(
		SELECT 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string) mom,
			sum(sale_amt) sale_amt2, count(distinct original_order_code) as cnt2
		from 
			csx_dws.csx_dws_bbc_sale_detail_di
		where 
			sdt >= '20201201' and sdt < '20211201'
		group by 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string)
		) b on a.smt = b.mom
order by a.smt;


-- 20221205 增加自营联营的标识
SELECT
  a.*, (a.sale_amt-b.sale_amt2)/b.sale_amt2 as a, (a.cnt-b.cnt2)/b.cnt2 as b
from
	(
    SELECT 
		substr(sdt, 1, 6) smt,
		operation_mode_name,
		sum(sale_amt) sale_amt, 
		count(distinct original_order_code) as cnt
    from 
		csx_dws.csx_dws_bbc_sale_detail_di
    where 
		sdt >= '20200101' and sdt < '20210101'
    group by 
		substr(sdt, 1, 6), operation_mode_name
	) a 
	left join
		(
		SELECT 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string) mom,
			operation_mode_name,
			sum(sale_amt) sale_amt2, count(distinct original_order_code) as cnt2
		from 
			csx_dws.csx_dws_bbc_sale_detail_di
		where 
			sdt >= '20191201' and sdt < '20201201'
		group by 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string), operation_mode_name
		) b on a.smt = b.mom and a.operation_mode_name = b.operation_mode_name
order by a.smt, a.operation_mode_name;



-- 2021年
SELECT
  a.*, (a.sale_amt-b.sale_amt2)/b.sale_amt2 as a, (a.cnt-b.cnt2)/b.cnt2 as b
from
	(
    SELECT 
		substr(sdt, 1, 6) smt,
		operation_mode_name,
		sum(sale_amt) sale_amt, 
		count(distinct original_order_code) as cnt
    from 
		csx_dws.csx_dws_bbc_sale_detail_di
    where 
		sdt >= '20210101' and sdt < '20220101'
    group by 
		substr(sdt, 1, 6), operation_mode_name
	) a 
	left join
		(
		SELECT 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string) mom,
			operation_mode_name,
			sum(sale_amt) sale_amt2, count(distinct original_order_code) as cnt2
		from 
			csx_dws.csx_dws_bbc_sale_detail_di
		where 
			sdt >= '20201201' and sdt < '20211201'
		group by 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string), operation_mode_name
		) b on a.smt = b.mom and a.operation_mode_name = b.operation_mode_name
order by a.smt, a.operation_mode_name;


-- 2022年
SELECT
  a.*, (a.sale_amt-b.sale_amt2)/b.sale_amt2 as a, (a.cnt-b.cnt2)/b.cnt2 as b
from
	(
    SELECT 
		substr(sdt, 1, 6) smt,
		operation_mode_name,
		sum(sale_amt) sale_amt, 
		count(distinct original_order_code) as cnt
    from 
		csx_dws.csx_dws_bbc_sale_detail_di
    where 
		sdt >= '20220101' and sdt < '20221201'
    group by 
		substr(sdt, 1, 6), operation_mode_name
	) a 
	left join
		(
		SELECT 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string) mom,
			operation_mode_name,
			sum(sale_amt) sale_amt2, count(distinct original_order_code) as cnt2
		from 
			csx_dws.csx_dws_bbc_sale_detail_di
		where 
			sdt >= '20211201' and sdt < '20221101'
		group by 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string), operation_mode_name
		) b on a.smt = b.mom and a.operation_mode_name = b.operation_mode_name
order by a.smt, a.operation_mode_name;



SELECT
  a.*, (a.sale_amt-b.sale_amt2)/b.sale_amt2 as a, (a.cnt-b.cnt2)/b.cnt2 as b
from
	(
    SELECT 
		substr(sdt, 1, 6) smt,
		sum(sale_amt) sale_amt, count(distinct original_order_code) as cnt
    from 
		csx_dws.csx_dws_bbc_sale_detail_di
    where 
		sdt >= '20220101' and sdt < '20230101'
    group by 
		substr(sdt, 1, 6)
	) a 
	left join
		(
		SELECT 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string) mom,
			sum(sale_amt) sale_amt2, count(distinct original_order_code) as cnt2
		from 
			csx_dws.csx_dws_bbc_sale_detail_di
		where 
			sdt >= '20211201' and sdt < '20221201'
		group by 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string)
		) b on a.smt = b.mom
order by a.smt;


SELECT
  a.*, (a.sale_amt-b.sale_amt2)/b.sale_amt2 as a, (a.cnt-b.cnt2)/b.cnt2 as b
from
	(
    SELECT 
		substr(sdt, 1, 6) smt,
		sum(sale_amt) sale_amt, count(distinct original_order_code) as cnt
    from 
		csx_dws.csx_dws_bbc_sale_detail_di
    where 
		sdt >= '20230101' and sdt < '20230901'
    group by 
		substr(sdt, 1, 6)
	) a 
	left join
		(
		SELECT 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string) mom,
			sum(sale_amt) sale_amt2, count(distinct original_order_code) as cnt2
		from 
			csx_dws.csx_dws_bbc_sale_detail_di
		where 
			sdt >= '20221201' and sdt < '20230801'
		group by 
			cast(from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd'), 1)), 'yyyyMM') as string)
		) b on a.smt = b.mom
order by a.smt;