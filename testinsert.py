# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession


def main():
    # 创建SparkSession并配置以访问Spark元数据
    spark = SparkSession.builder \
        .appName("DataCleaning") \
        .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.DliLakeHouseBuilder") \
        .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.DliLakeHouseCatalog") \
        .enableHiveSupport() \
        .getOrCreate()

    # 读取数据湖中的表格信息
    query = "SELECT * FROM tid_sdi_ai4data.ai4data_enterprise_bak LIMIT 100"  # 仅读取前100行进行示例
    df = spark.sql(query)
    print(df.count())

    # 显示原始数据
    print("Original Data:")
    df.show()

    # 将查询结果保存到临时视图
    df.createOrReplaceTempView("temp_view")

    # 使用Hive SQL创建表并插入数据
    spark.sql("CREATE TABLE IF NOT EXISTS tid_sdi_ai4data.find100 AS SELECT * FROM temp_view")

    # 停止SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
