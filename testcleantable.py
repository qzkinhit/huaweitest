# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession


def main(output_table):
    # 创建SparkSession
    spark = SparkSession.builder.appName("DataCleaning").enableHiveSupport().getOrCreate()

    # 读取数据湖中的表格信息
    query = "SELECT * FROM tid_sdi_ai4data.ai4data_enterprise_bak LIMIT 100"  # 仅读取前100行进行示例
    df = spark.sql(query)

    # 显示原始数据
    print("Original Data:")
    df.show()

    # 数据清洗操作示例：删除包含缺失值的行
    cleaned_df = df.na.drop()

    # 显示清洗后的数据
    print("Cleaned Data:")
    cleaned_df.show()

    # 将清洗后的数据写入另一个表
    cleaned_df.write.mode("overwrite").saveAsTable(output_table)

    # 停止SparkSession
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: dliquery <outputTable>")
        exit(-1)

    output_table = sys.argv[1]
    main(output_table)
