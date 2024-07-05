# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

def main():
    # 创建SparkSession并配置以访问Spark元数据
    spark = SparkSession.builder \
        .appName("DataCleaning") \
        .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.UQueryHiveACLSessionStateBuilder")\
        .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.UQueryHiveACLExternalCatalog")\
        .config("spark.sql.extensions", "org.apache.spark.sql.DliSparkExtension")\
        .getOrCreate()



    # 读取数据湖中的表格信息
    query = "SELECT * FROM tid_sdi_ai4data.ai4data_enterprise_bak LIMIT 100"  # 仅读取前100行进行示例
    df = spark.sql(query)
    print(df.count())
    # 显示原始数据
    print("Original Data:")
    df.show()

    # 将查询结果写入一个新表 find100
    df.write.mode("overwrite").saveAsTable("tid_sdi_ai4data.find100")

    # 停止SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
