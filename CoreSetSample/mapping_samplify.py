import os
import shutil
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from CoreSetSample.get_patition_block import find_blocks_spark


def Generate_Sample(TotalData, sset,tset,models=None, save_path=None):
    """
    从 Spark 数据集生成样本并转存为 Pandas DataFrame。

    参数:
    - file_load: 字符串，CSV 文件的路径。
    - sset: 源属性集列表，例如 ['zip'],
    - tset: 目标属性集列表，例如 ['city']
    - p: 浮点数，核心集占总数据集比例。
    - save_path: 字符串，保存结果的路径，如果为 None，则不保存。
    """

    # 进行抽样
    start_time = time.time()
    # 读取数据，仅包含必要的列
    required_columns = list(set(sset + tset + ['index']))  # 移除重复项
    data = TotalData.select(required_columns)
    if models is not None:
        sampled_data = block_sample_cycle(data,models)
    else:
        sampled_data = block_sample(data, sset,tset)
    # 保存数据
    if save_path:
        block_path = os.path.join(save_path, "sample_data.csv")
        sampled_data.write.csv(block_path, header=True, mode='overwrite')
        print(f"Sample data saved to: {block_path}")

    # 计时并打印抽样所需时间
    print(f"核心样本抽取时间（秒）: {time.time() - start_time}")

    return [sampled_data]  # 为了输出格式和Generate_BlockSample统一，方便后续对采样数据的处理


def Generate_BlockSample(TotalData, sset, tset,models=None):
    """
    从 Spark 数据集生成样本并转存为 Pandas DataFrame。

    参数:
    - file_load: 字符串，CSV 文件的路径。
    - sset: 源属性集列表，例如 ['zip'],
    - tset: 目标属性集列表，例如 ['city']
    - p: 浮点数，核心集占总数据集比例。
    - save_path: 字符串，保存结果的路径，如果为 None，则不保存。
    """
    # 读取数据，仅包含必要的列
    required_columns = list(set(sset + tset + ['index']))   # 移除重复项
    data = TotalData.select(required_columns)

    # 进行抽样
    start_time = time.time()

    if models is not None:
        sampled_data = block_sample_cycle(data, models)
    else:
        sampled_data = block_sample(data, sset, tset)
    block_sample_data = find_blocks_spark(sampled_data, sset)

    # 计时并打印抽样所需时间
    print(f"核心样本抽取时间（秒）: {time.time() - start_time}")

    return block_sample_data
def block_sample(df, sourceSet, targetSet):
    # 对 sourceSet 和 targetSet 进行分组
    group_columns = sourceSet + targetSet

    # 聚合并计算每个 source 组合中的 target 分布情况
    grouped = df.groupBy(*group_columns).count()

    # 对每个 source 组合，计算不同 target 种类的数量
    window_spec_partition = Window.partitionBy(*sourceSet)
    grouped = grouped.withColumn("target_count", F.count(targetSet[0]).over(window_spec_partition))

    # 如果 target_count > 1，则排序并设置抽样数；否则 sample_count 设置为 0
    window_spec_rank = Window.partitionBy(*sourceSet).orderBy("count")
    grouped = grouped.withColumn("rank", F.when(F.col("target_count") > 1, F.dense_rank().over(window_spec_rank)).otherwise(0)) \
        .withColumn("sample_count", F.when(F.col("rank") > 0, F.col("rank")).otherwise(0))

    # 生成实际的采样数据
    # 根据上面计算的 sample_count 进行采样
    window_spec_target = Window.partitionBy(*group_columns).orderBy(F.rand())
    sampled_data = df.join(grouped, group_columns) \
        .withColumn("row_num", F.row_number().over(window_spec_target)) \
        .filter(F.col("row_num") <= F.col("sample_count")) \
        .drop("count", "target_count", "rank", "sample_count", "row_num")

    # 输出被采样的数据
    return sampled_data







def block_sample_cycle(df, attrSet):
    # 对 attrSet 进行分组
    group_columns = attrSet

    # 聚合并计算每个组合中的分布情况
    grouped = df.groupBy(*group_columns).count()

    # 按照聚合块的大小从小到大排序，并使用 dense_rank 确保相等 count 得到相同 rank
    window_spec_partition = Window.orderBy(F.asc("count"))
    grouped = grouped.withColumn("rank", F.dense_rank().over(window_spec_partition))

    # 计算采样数，按照 rank 进行采样，rank 最小的采样一个，以此类推
    grouped = grouped.withColumn("sample_count", F.col("rank"))

    # 生成实际的采样数据
    window_spec_target = Window.partitionBy(*group_columns).orderBy(F.rand())
    sampled_data = df.join(grouped, group_columns) \
        .withColumn("row_num", F.row_number().over(window_spec_target)) \
        .filter(F.col("row_num") <= F.col("sample_count")) \
        .drop("count", "rank", "sample_count", "row_num")

    # 输出被采样的数据
    return sampled_data

def save_sample(df: DataFrame, output_path: str, csv_filename: str, temp_folder: str):
    """
    将给定的 DataFrame 保存为 CSV ，复制到目标文件夹。

    :param df: 要保存的 DataFrame
    :param output_path: 保存的目标文件夹
    :param csv_filename: CSV 文件的自定义文件名
    :param temp_folder: 用于临时保存的文件夹路径
    """
    # 保存 CSV 文件，repartition 为 1 以获得单一输出文件
    df.repartition(1).write.mode('overwrite').option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .option("header", "true").csv(path=os.path.join(temp_folder, "sampleddata"), encoding="utf-8")

    # 找到并复制生成的 CSV 文件到目标文件夹，并重命名
    for filename in os.listdir(os.path.join(temp_folder, "sampleddata")):
        if filename.startswith("part-") and filename.endswith(".csv"):
            full_file_name = os.path.join(temp_folder, "sampleddata", filename)
            if os.path.isfile(full_file_name):
                shutil.copy(full_file_name, os.path.join(output_path, csv_filename))
                break
