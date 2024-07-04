
from pyspark.sql.functions import col, collect_list
from pyspark.sql.window import Window
def find_blocks(df, partition):
    # Step 1: 初步分块，基于partition[0]的值分组，并建立每个组的索引映射
    initial_blocks = df.groupby(partition[0]).groups
    block_mapping = {}
    for key, indexes in initial_blocks.items():
        for index in indexes:
            block_mapping[index] = indexes

    # Step 2: 对每个后续属性进行迭代，根据这些属性的值合并块
    for attr in partition[1:]:
        # 对当前属性进行分组，获取值到索引的映射
        value_to_indexes = df.groupby(attr).groups

        # 遍历每个值对应的索引集合
        for indexes in value_to_indexes.values():
            if len(indexes) > 1:
                # 找到所有通过当前属性值连接的行索引集合
                connected_indexes = set()
                for index in indexes:
                    connected_indexes.update(block_mapping[index])

                # 更新块映射，将所有连接的行索引合并为一个块
                for index in connected_indexes:
                    block_mapping[index] = connected_indexes

    # 构建最终的块列表
    final_blocks = set()
    for indexes in block_mapping.values():
        # 使用frozenset是因为它是hashable的，可以作为集合的元素
        final_blocks.add(frozenset(indexes))

    # 返回块的索引列表，每个块是索引的集合
    return [list(block) for block in final_blocks]


def find_blocks_spark(df, partition):
    preattr = partition[0]
    # 初始分块，基于第一个分区属性
    df = df.withColumn("block_id", col(preattr))

    # 迭代合并块
    for attr in partition[1:]:
        # 为当前属性构建窗口，收集所有相同值的 block_id
        w = Window.partitionBy(attr)
        df = df.withColumn("connected_blocks", collect_list("block_id").over(w))

        # 将 block_id 更新为 connected_blocks 中的最小值
        df = df.withColumn("block_id", col("connected_blocks")[0])
        #重新聚类一下上一个属性，目标是把整体加入到闭包中
        w = Window.partitionBy(preattr)
        df = df.withColumn("connected_blocks", collect_list("block_id").over(w))
        # 将 block_id 更新为 connected_blocks 中的最小值
        df = df.withColumn("block_id", col("connected_blocks")[0])
        preattr = attr
    # # 生成最终的块
    # blocks = df.select("index", "block_id").distinct().groupBy("block_id").agg(
    #     collect_list("index").alias("block_rows"))
    #
    # # 转换每个块为单独的 DataFrame
    #
    # block_dfs = [df.filter(col("index").isin(block["block_rows"])) for
    #              block in blocks.collect()]

    # 获取所有类别

    categories = df.select("block_id").distinct().rdd.flatMap(lambda x: x).collect()

    splits = [df.filter(df.block_id == category) for category in categories]
    return splits