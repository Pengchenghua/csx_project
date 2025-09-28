--1~6调整销售
-- 7、价量差工厂未使用的商品
-- 8、工厂分摊后成本小于0，未分摊金额
-- 9、报损
--10~11、盘点


drop table csx_tmp.cbgb_tz_m_cbgb_all;
create table csx_tmp.cbgb_tz_m_cbgb_all
as 
--1~6调整销售
select province_name,city_name,division_name,is_daijiagong,
	coalesce(channel_name,'BBC') channel_name,
	sum(case when channel_name='大客户' then adj_gc_xs_no end)/10000 B_adj_gc_xs_no,
	sum(case when channel_name='商超' then adj_gc_xs_no end)/10000 M_adj_gc_xs_no,
	sum(case when channel_name not in('大客户','商超') or channel_name is null then adj_gc_xs_no end)/10000 BBC_adj_gc_xs_no,
	sum(case when channel_name='大客户' then adj_gc_xs end)/10000 B_adj_gc_xs,
	sum(case when channel_name='商超' then adj_gc_xs end)/10000 M_adj_gc_xs,
	sum(case when channel_name not in('大客户','商超') or channel_name is null then adj_gc_xs end)/10000 BBC_adj_gc_xs,
	sum(adj_ddfkc_no)/10000 adj_ddfkc_no,sum(adj_ddfkc)/10000 adj_ddfkc,
	sum(adj_cgth_no)/10000 adj_cgth_no,sum(adj_cgth)/10000 adj_cgth,
	sum(adj_gc_xs_no)/10000 adj_gc_xs_no,sum(adj_gc_xs)/10000 adj_gc_xs,
	sum(adj_gc_db_no)/10000 adj_gc_db_no,sum(adj_gc_db)/10000 adj_gc_db,
	sum(adj_gc_qt_no)/10000 adj_gc_qt_no,sum(adj_gc_qt)/10000 adj_gc_qt,
	sum(adj_sg_no)/10000 adj_sg_no,sum(adj_sg)/10000 adj_sg,
	sum(adj_bj_xs_no)/10000 adj_bj_xs_no,sum(adj_bj_xs)/10000 adj_bj_xs,
	sum(adj_bj_db_no)/10000 adj_bj_db_no,sum(adj_bj_db)/10000 adj_bj_db,
	sum(adj_bj_qt_no)/10000 adj_bj_qt_no,sum(adj_bj_qt)/10000 adj_bj_qt,
	'' bs_amt_no_tax,'' bs_amt,'' gcfth_amount,'' jlc_amount,
	'' inventory_p_no,'' inventory_l_no,'' inventory_p,'' inventory_l,
	substr(${hiveconf:i_sdate_22},1,6) as sdt	
from csx_tmp.cbgb_tz_m_cbgb_xstz_djg
--where sdt='${EDATE}'
group by province_name,city_name,channel_name,division_name,is_daijiagong

union all
-- 9、报损
select province_name,city_name,division_name,is_daijiagong,'' channel_name,	
	'' B_adj_gc_xs_no,'' M_adj_gc_xs_no,'' BBC_adj_gc_xs_no,'' B_adj_gc_xs,'' M_adj_gc_xs,'' BBC_adj_gc_xs,
	'' adj_ddfkc_no,'' adj_ddfkc,'' adj_cgth_no,'' adj_cgth,
	'' adj_gc_xs_no,'' adj_gc_xs,'' adj_gc_db_no,'' adj_gc_db,
	'' adj_gc_qt_no,'' adj_gc_qt,'' adj_sg_no,'' adj_sg,
	'' adj_bj_xs_no,'' adj_bj_xs,'' adj_bj_db_no,'' adj_bj_db,
	'' adj_bj_qt_no,'' adj_bj_qt,
	sum(amt_no_tax)/10000 bs_amt_no_tax,
	sum(amt)/10000 bs_amt,
	'' gcfth_amount,'' jlc_amount,
	'' inventory_p_no,'' inventory_l_no,'' inventory_p,'' inventory_l,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.cbgb_tz_m_cbgb_bs_djg
--where sdt='${EDATE}'
group by province_name,city_name,division_name,is_daijiagong

union all
-- 8、工厂分摊后成本小于0，未分摊金额
select province_name,city_name,division_name,is_daijiagong,'' channel_name,
	'' B_adj_gc_xs_no,'' M_adj_gc_xs_no,'' BBC_adj_gc_xs_no,'' B_adj_gc_xs,'' M_adj_gc_xs,'' BBC_adj_gc_xs,
	'' adj_ddfkc_no,'' adj_ddfkc,'' adj_cgth_no,'' adj_cgth,
	'' adj_gc_xs_no,'' adj_gc_xs,'' adj_gc_db_no,'' adj_gc_db,
	'' adj_gc_qt_no,'' adj_gc_qt,'' adj_sg_no,'' adj_sg,
	'' adj_bj_xs_no,'' adj_bj_xs,'' adj_bj_db_no,'' adj_bj_db,
	'' adj_bj_qt_no,'' adj_bj_qt,
	'' bs_amt_no_tax,'' bs_amt,
	sum(amount)/10000 gcfth_amount,
	'' jlc_amount,
	'' inventory_p_no,'' inventory_l_no,'' inventory_p,'' inventory_l,
    substr(${hiveconf:i_sdate_22},1,6) as sdt	
from csx_tmp.cbgb_tz_m_cbgb_gcfth_djg
--where sdt='${EDATE}'
group by province_name,city_name,division_name,is_daijiagong

union all
-- 7、价量差工厂未使用的商品
select province_name,city_name,division_name,is_daijiagong,'' channel_name,
	'' B_adj_gc_xs_no,'' M_adj_gc_xs_no,'' BBC_adj_gc_xs_no,'' B_adj_gc_xs,'' M_adj_gc_xs,'' BBC_adj_gc_xs,
	'' adj_ddfkc_no,'' adj_ddfkc,'' adj_cgth_no,'' adj_cgth,
	'' adj_gc_xs_no,'' adj_gc_xs,'' adj_gc_db_no,'' adj_gc_db,
	'' adj_gc_qt_no,'' adj_gc_qt,'' adj_sg_no,'' adj_sg,
	'' adj_bj_xs_no,'' adj_bj_xs,'' adj_bj_db_no,'' adj_bj_db,
	'' adj_bj_qt_no,'' adj_bj_qt,
	'' bs_amt_no_tax,'' bs_amt,	
	'' gcfth_amount,
	sum(amount)/10000 jlc_amount,
	'' inventory_p_no,'' inventory_l_no,'' inventory_p,'' inventory_l,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.cbgb_tz_m_cbgb_jlc_djg
--where sdt='${EDATE}'
group by province_name,city_name,division_name,is_daijiagong

union all
--10~11、盘点
select province_name,city_name,division_name,is_daijiagong,'' channel_name,
	'' B_adj_gc_xs_no,'' M_adj_gc_xs_no,'' BBC_adj_gc_xs_no,'' B_adj_gc_xs,'' M_adj_gc_xs,'' BBC_adj_gc_xs,
	'' adj_ddfkc_no,'' adj_ddfkc,'' adj_cgth_no,'' adj_cgth,
	'' adj_gc_xs_no,'' adj_gc_xs,'' adj_gc_db_no,'' adj_gc_db,
	'' adj_gc_qt_no,'' adj_gc_qt,'' adj_sg_no,'' adj_sg,
	'' adj_bj_xs_no,'' adj_bj_xs,'' adj_bj_db_no,'' adj_bj_db,
	'' adj_bj_qt_no,'' adj_bj_qt,
	'' bs_amt_no_tax,'' bs_amt,	
	'' gcfth_amount,'' jlc_amount,	
	sum(inventory_p_no)/10000 inventory_p_no,
	sum(inventory_l_no)/10000 inventory_l_no,
	sum(inventory_p)/10000 inventory_p,
	sum(inventory_l)/10000 inventory_l,
    substr(${hiveconf:i_sdate_22},1,6) as sdt	
from csx_tmp.cbgb_tz_m_cbgb_pd_djg
--where sdt='${EDATE}'
group by province_name,city_name,division_name,is_daijiagong; 


--INVALIDATE METADATA csx_tmp.cbgb_tz_m_cbgb_all;





