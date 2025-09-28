create table csx_analyse.csx_analyse_fr_crm_customer_dc_rule_mf(
`dc_code`                            string              COMMENT    'DC编码',
`customer_code`                      string              COMMENT    '客户编码',
`customer_flag`                      string              COMMENT    '落地类型'
) COMMENT '商品池指标-客户对应dc规则表'
STORED AS TEXTFILE;



CREATE TABLE `crm_customer_dc_rule` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`dc_code`                        varchar(32)         DEFAULT NULL  COMMENT    'DC编码',
`customer_code`                  varchar(32)         DEFAULT NULL  COMMENT    '客户编码',
`customer_flag`                  varchar(32)         DEFAULT NULL  COMMENT    '落地类型',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品池指标-客户对应dc规则表';



select * from crm_customer_dc_rule limit 100;

select count(*) from crm_customer_dc_rule ;

select count(*) from csx_analyse.csx_analyse_fr_crm_customer_dc_rule_mf ;




create table csx_analyse.csx_analyse_fr_category_rule_config_mf(
`big_category_code`                  string              COMMENT    '大类编码',
`big_category_name`                  string              COMMENT    '大类名称',
`second_classify_name`               string              COMMENT    '二级分类名称'
) COMMENT '商品池指标-品类规则配置表'
STORED AS TEXTFILE;



CREATE TABLE `category_rule_config` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`big_category_code`              varchar(32)         DEFAULT NULL  COMMENT    '大类编码',
`big_category_name`              varchar(32)         DEFAULT NULL  COMMENT    '大类名称',
`second_classify_name`           varchar(32)         DEFAULT NULL  COMMENT    '二级分类名称',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品池指标-品类规则配置表';


