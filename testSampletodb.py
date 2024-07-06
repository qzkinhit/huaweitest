from pyspark.sql.functions import monotonically_increasing_id
from AnalyticsCache.cleaner_associations_cycle import PreParamClassier, discover_cleaner_associations, \
    associations_classier
# from AnalyticsCache.cleaner_associations_cycle import PreParamClassier, discover_cleaner_associations, \
#     associations_classier
from CoreSetSample.mapping_samplify import Generate_Sample
# from SampleScrubber.cleaner.multiple import AttrRelation
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import SparkSession

class AttrRelation():
    """AttrRelation 是两组属性之间的依赖关系，使用最小修复对这个关系进行修复。功能依赖关系 (A -> B) 意味着对于每个 B 属性，可以通过 A 属性进行修复
    """

    def __init__(self, source, target, name,condition_func=None,ExtDict=None,edit_rule=None):
        """FunctionalDependency 构造函数

        source -- 一个属性名称列表
        target -- 一个属性名称列表
        """
        self.source = set(source)
        self.target = set(target)
        self.domain = set(source + target)
        self.quality_history = {}  # 改为字典，用来记录最新的运算情况
        self.cleanerList = []
        self.name = name
        self.fixValueRules = {}
        self.msg = '[FunctionalDependency:(s: %s, t: %s)]' % (self.source, self.target)#默认就是基于FD的最小修复
        if condition_func!=None:
            #非简单字典类的过滤规则，condition_func是一个函数，condition_func（source=[x,y,z...]可以返回 true 或 false）
            self.fixValueRules['condition_func'] = condition_func
        if ExtDict!=None:
            self.fixValueRules['ExtDict'] = ExtDict
        if edit_rule!=None:
            self.fixValueRules['edit_rule'] = edit_rule
        self.cleanerList=[self]
cleaners = [
    AttrRelation(['establishment_date'], ['establishment_time'], '1'),
    AttrRelation(['registered_capital'], ['registered_capital_scale'], '2'),
    AttrRelation(['enterprise_name'], ['industry_third'], '3'),
    AttrRelation(['enterprise_name'], ['industry_second'], '4'),
    AttrRelation(['enterprise_name'], ['industry_first'], '5'),
    AttrRelation(['industry_first'], ['industry_second'], '6'),
    AttrRelation(['industry_second'], ['industry_third'], '7'),
    AttrRelation(['annual_turnover'], ['annual_turnover_interval'], '8'),
    AttrRelation(['latitude', 'longitude'], ['province'], '9'),
    AttrRelation(['latitude', 'longitude'], ['city'], '10'),
    AttrRelation(['latitude', 'longitude'], ['district'], '11'),
    AttrRelation(['enterprise_address'], ['province'], '12'),
    AttrRelation(['enterprise_address'], ['city'], '13'),
    AttrRelation(['enterprise_address'], ['district'], '14'),
    AttrRelation(['enterprise_address'], ['latitude'], '15'),
    AttrRelation(['enterprise_address'], ['longitude'], '16'),
    AttrRelation(['province'], ['city'], '17'),
    AttrRelation(['city'], ['district'], '18'),
    AttrRelation(['enterprise_name'], ['enterprise_type'], '19'),
    AttrRelation(['enterprise_id'], ['enterprise_name'], '20'),
    AttrRelation(['social_credit_code'], ['enterprise_name'], '21')
]




# 创建SparkSession并配置以访问Spark元数据
spark = SparkSession.builder \
    .appName("DataCleaning") \
    .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.DliLakeHouseBuilder") \
    .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.DliLakeHouseCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取数据湖中的表格信息
query = "SELECT * FROM tid_sdi_ai4data.ai4data_enterprise_bak"  # 读取整个表
data = spark.sql(query)

# 添加数据行的索引
data = data.withColumn("index", monotonically_increasing_id())
data.persist(StorageLevel.MEMORY_AND_DISK)

# 初始化和分析清洗器
print("初始化清洗器和分析依赖关系...")
Edges, singles, multis = PreParamClassier(cleaners)
source_sets, target_sets, explain, processing_order = discover_cleaner_associations(Edges)

print("执行层级和目标模型分类...")
levels, models, nodes = associations_classier(multis, source_sets, target_sets)
print("执行层级 (并行组):", levels)
print("目标模型分类:", models)
for level_index, level in enumerate(nodes):
    print(f"\n处理第 {level_index + 1} 层级, 包含节点: {level}")
    sample_id = 0
    for node in level:
        if node in models and models[node]:
            sset = set()
            tset = set()
            for m in models[node]:
                sset = sset.union(m.source)
                tset = tset.union(m.target)
            sset = list(sset)
            tset = list(tset)
            print(f"  抽样处理：源属性 {sset} -> 目标属性 {node}")
            if len(tset) == 1:  # 无环
                sample_Block_df = Generate_Sample(data, sset, tset)
            else:  # 有环
                sample_Block_df = Generate_Sample(data, sset, tset, models=models[node])
            sample_id += 1
            print(f"  在 spark 的分块数: {len(sample_Block_df)}")
            for block_index, blockData in enumerate(sample_Block_df):
                print(f"  当前块内的样本大小: {blockData.count()}")
                if blockData.count() > 1000000:
                    # 创建临时视图
                    blockData.createOrReplaceTempView("temp_view")

                    # 使用Hive SQL创建表并插入数据
                    table_name = f"sample1_{level_index}_{sample_id}_{block_index}"
                    spark.sql(f"CREATE TABLE IF NOT EXISTS tid_sdi_ai4data.{table_name} AS SELECT * FROM temp_view")
                    print(f"  块数据已写入表: {table_name}")

# 停止SparkSession
spark.stop()
