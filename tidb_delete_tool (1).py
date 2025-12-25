import logging
import sys
import time
from enum import Enum

import pymysql

# 配置日志记录
logging.basicConfig(
    format="%(asctime)s - %(message)s",
    level=logging.INFO,  # 可根据需要调整为 DEBUG、WARNING、ERROR 等
    handlers=[
        logging.StreamHandler(),  # 将日志输出到控制台
        # logging.FileHandler("spark_etKtXb.log")  # 将日志输出到本地文件
    ]
)


class DBConfig(Enum):
    PRD_DATA_MARKET_DB = {
        "host": "prd.datalake-tidb.b2bcsx.com",
        "port": 4000,
        "user": "csx_data_market_user",
        "password": "2*gJhLHt^*q0wNkW",
        "db": "csx_data_market",
        "charset": "utf8mb4"
    }

    PRD_CRM_REPORT_DB = {
        "host": "prd.datalake-tidb.b2bcsx.com",
        "port": 4000,
        "user": "csx_data_market_user",
        "password": "2*gJhLHt^*q0wNkW",
        "db": "csx_crm_report",
        "charset": "utf8mb4"
    }

    PRD_ANALYSIS_PRD_DB = {
        "host": "prd.mysql-master-datagroup.b2bcsx.com",
        "port": 7477,
        "user": "datagroup_app",
        "password": "Hoaerwsadr",
        "db": "data_analysis_prd",
        "charset": "utf8mb4"
    }

    PRD_TIDB_DATA_MARKET_DB = {
        "host": "prd.datalake-tidb.b2bcsx.com",
        "port": 4000,
        "user": "csx_data_market_user",
        "password": "2*gJhLHt^*q0wNkW",
        "db": "csx_data_market",
        "charset": "utf8mb4"
    }

    PRD_TIDB_DATA_REAL_DB = {
        "host": "prd.datalake-tidb.b2bcsx.com",
        "port": 4000,
        "user": "csx_data_market_user",
        "password": "2*gJhLHt^*q0wNkW",
        "db": "csx_data_real",
        "charset": "utf8mb4"
    }

    DEV_DATA_MARKET_DB = {
        "host": "10.192.1.55",
        "port": 3306,
        "user": "datagrouptest",
        "password": "OUl2u82p83$#2",
        "db": "test_csx_data_market",
        "charset": "utf8mb4"
    }

    DEV_CRM_REPORT_DB = {
        "host": "10.252.193.44",
        "port": 3306,
        "user": "csxcrm_report",
        "password": "Ur&yowI80m&16",
        "db": "csx_crm_report",
        "charset": "utf8mb4"
    }

    DEV_ANALYSIS_PRD_DB = {
        "host": "10.192.1.55",
        "port": 3306,
        "user": "datagrouptest",
        "password": "OUl2u82p83$#2",
        "db": "data_analysis_prd",
        "charset": "utf8mb4"
    }

    DEV_TIDB_DATA_MARKET_DB = {
        "host": "dev.tidb-all39.b2bcsx.com",
        "port": 4000,
        "user": "test",
        "password": "123456asd",
        "db": "csx_data_market",
        "charset": "utf8mb4"
    }

    DEV_TIDB_DATA_REAL_DB = {
        "host": "dev.tidb-all39.b2bcsx.com",
        "port": 4000,
        "user": "test",
        "password": "123456asd",
        "db": "csx_data_real",
        "charset": "utf8mb4"
    }

    @classmethod
    def get_db_config(cls, db_name: str):
        try:
            return cls[db_name].value
        except KeyError:
            raise ValueError(f"没有找到名为{db_name}的数据库配置")


class MySQLDeleteTool:
    def __init__(self, db_config_dict: dict):
        # 校验数据库枚举值是否有效
        db_enum = db_config_dict.get("db_enum")
        try:
            self.db_config = DBConfig.get_db_config(db_enum)  # 校验 db_enum 是否有效
        except ValueError as e:
            logging.error(f"{str(e)}")  # 用 logging 记录错误信息
            sys.exit(1)  # 如果 db_enum 无效，则退出程序

        # 获取 table、partition 和 named_date
        self.table = db_config_dict.get("table")
        self.partition = db_config_dict.get("partition")
        self.named_date = db_config_dict.get("named_date")

        # 校验 table、partition 和 named_date 是否为空
        if not self.table:
            logging.error("表名 (table) 不能为空。")
            sys.exit(1)
        if not self.partition:
            logging.error("分区字段 (partition) 不能为空。")
            sys.exit(1)
        if not self.named_date:
            logging.error("日期条件 (named_date) 不能为空。")
            sys.exit(1)

        # 获取 limit 值，如果没有指定，则使用默认值 1000
        self.limit = db_config_dict.get("limit", 1000)

        # 检查 limit 是否在合法范围内
        if not (500 <= self.limit <= 20000):
            logging.error(f"无效的limit值: {self.limit}. 该值必须在500到20000之间。")
            sys.exit(1)  # 如果 limit 超出范围，退出程序

        # 在这里打印 MySQL 连接的 URL
        self.print_mysql_url()

        self.database = self.db_config["db"]

        self.comparison_operator = db_config_dict.get("comparison_operator", 1)

        # 校验 comparison_operator 是否有效
        valid_operators = {0, 1, 2, 3, 4}
        if self.comparison_operator not in valid_operators:
            logging.error(f"无效的 comparison_operator 值: {self.comparison_operator}. 该值必须是 0, 1, 2, 3 或 4。")
            sys.exit(1)  # 如果 comparison_operator 无效，则退出程序

        self.extension_condition = db_config_dict.get("extension_condition", "1 = 1")

        self.connection = None
        self.cursor = None

        # 在这里构造 DELETE 语句
        self.construct_delete_statement()

    def print_mysql_url(self):
        """打印 MySQL 连接 URL"""
        mysql_url = f"jdbc:mysql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['db']}"
        logging.info(f"数据源地址: {mysql_url}")

    def construct_delete_statement(self):
        """构造并返回DELETE语句"""
        # 判断 comparison_operator 的值并选择相应的操作符
        operator_map = {
            0: '>',
            1: '>=',
            2: '=',
            3: '<=',
            4: '<'
        }

        operator = operator_map.get(self.comparison_operator)

        return f"DELETE FROM {self.database}.{self.table} WHERE {self.partition} {operator} '{self.named_date}' AND {self.extension_condition} LIMIT {self.limit};"

    def print_delete_statement(self):
        """打印DELETE语句"""
        delete_statement = self.construct_delete_statement()
        logging.info(f"即将循环执行SQL: {delete_statement}")

    def connect_db(self):
        """创建数据库连接"""
        self.connection = pymysql.connect(
            host=self.db_config["host"],
            port=self.db_config["port"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            database=self.db_config["db"],
            charset=self.db_config["charset"]
        )
        self.cursor = self.connection.cursor()

    def close_db(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def delete_data(self):
        """执行数据删除操作"""
        try:
            # 连接数据库
            self.connect_db()

            # 构造DELETE语句
            delete_statement = self.construct_delete_statement()

            # 初始化统计信息
            total_rows_deleted = 0
            total_execution_time = 0
            delete_count = 0

            # 循环执行删除语句
            while True:
                try:
                    # 记录开始时间
                    start_time = time.time()

                    # 执行删除语句
                    affected_rows = self.cursor.execute(delete_statement)

                    # 提交事务
                    self.connection.commit()

                    # 记录结束时间
                    end_time = time.time()

                    # 计算本次执行时间
                    execution_time = end_time - start_time

                    # 更新统计信息
                    total_rows_deleted += affected_rows
                    total_execution_time += execution_time
                    delete_count += 1

                    # 如果受影响行数为0，则表示数据已经全部删除完毕，循环停止
                    if affected_rows == 0:
                        break

                except Exception as e:
                    # 发生异常时输出错误信息并抛出异常
                    logging.error(f"执行删除时发生错误: {str(e)}")
                    # 只抛异常，作业不会出错。因此需要直接退出，让平台认定作业失败
                    sys.exit(1)  # 终止执行

            # 计算平均执行时间
            average_execution_time = total_execution_time / delete_count

            # 打印删除执行次数、总删除数据条数、总执行时间和每次执行时间平均值
            logging.info(f"删除执行次数: {delete_count}")
            logging.info(f"总删除数据条数: {total_rows_deleted}")
            logging.info(f"总执行时间: {total_execution_time:.3f}s")
            logging.info(f"每次执行时间平均值: {average_execution_time:.3f}s")
            logging.info(f"作业执行成功!")

        finally:
            self.close_db()


# 调用示例
if __name__ == "__main__":
    db_config_dict = {
        "db_enum": "DEV_DATA_MARKET_DB",  # 数据库配置枚举值，必填
        "table": "ads_sale_r_m_sales_customer_goods",  # 表名，必填
        "partition": "month",  # 分区字段，必填
        "named_date": "202112",  # 指定日期，必填
        # "limit": 1000,  # 每次删除的数量，该值必须在500到20000之间。非必填，默认为1000
        # "comparison_operator": 1,  # 指定 WHERE 条件中的判断符 (0:'>',1:'>=',2:'=',3:'<=',4:'<') 。非必填，默认为 1:'>='
        # "extension_condition": "customer_no = '107958'",  # 扩展条件，日期条件后加入的其他过滤条件。非必填，默认为'1 = 1'
    }

    delete_tool = MySQLDeleteTool(db_config_dict)
    delete_tool.print_delete_statement()  # 打印DELETE语句
    delete_tool.delete_data()  # 执行删除操作
