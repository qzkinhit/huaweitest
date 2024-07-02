# encoding: utf-8
""" @ author: g00564447 葛昱辰
    @ file_name: dli_test.py
    @ create_time:2021/11/15 17:39
    @ description:
"""
from pyspark.sql import SparkSession


class MLSReadDLITable:
    def __init__(self,
                 DLI_database,
                 DLI_table,
                 DLI_save_path
                 ):
        """
        read dataset from DLI
        :param DLI_database:
        :param DLI_table:
        """

        self.DLI_database = DLI_database
        self.DLI_table = DLI_table
        self.DLI_save_path = DLI_save_path
        self._outputs = {}


def run(self):
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.UQueryHiveACLSessionStateBuilder") \
        .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.UQueryHiveACLExternalCatalog") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.dli.metaAccess.enable", "true") \
        .appName("pySpark DLI sql test") \
        .getOrCreate()
    input_table = self.DLI_database + "." + self.DLI_table
    input_df = spark.sql(f"SELECT * FROM {input_table} limit 1000")
    column_names = input_df.columns
    for column in column_names:
        input_df = input_df.withColumnRenamed(column, column.strip())
    self._outputs = {
        "output_port_1": input_df
    }
    output_table = self.DLI_save_path
    input_df.write.format("csv").save(output_table)

if __name__ == "__main__":
    read_DLI_table_ = MLSReadDLITable('cdm-resource-test', 'mysql2obs-2020-10-16_17.15.48.386.csv',
                                      'obs://cdm-resource-test/dli_test_gyc/test')
    read_DLI_table_.run()
# @output {"label":"dataframe","name":"read_DLI_table_#id#.get_outputs()['output_port_1']","type":"DataFrame"}
"""obs://cdm-resource-test/dli_test_gyc
obs://cdm-resource-test/mysql2obs-2020-10-16_17.15.48.386.csv"""
