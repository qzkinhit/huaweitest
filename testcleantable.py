# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import logging

def main():
    # 配置日志级别
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # 创建SparkSession并配置特定参数
    spark = SparkSession.builder \
        .appName("DataCleaning") \
        .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.UQueryHiveACLSessionStateBuilder") \
        .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.UQueryHiveACLExternalCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.DliSparkExtension") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # 读取数据湖中的表格信息
        query = "SELECT * FROM tid_sdi_ai4data.ai4data_enterprise_bak LIMIT 100"  # 仅读取前100行进行示例
        df = spark.sql(query)
        
        # 显示原始数据
        logger.info("Original Data:")
        df.show()
    except Exception as e:
        logger.error("Error occurred while executing the query or displaying the data.", exc_info=True)
    finally:
        # 停止SparkSession
        spark.stop()

if __name__ == "__main__":
    main()
