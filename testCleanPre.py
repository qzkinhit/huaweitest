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
    .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.UQueryHiveACLSessionStateBuilder") \
    .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.UQueryHiveACLExternalCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.DliSparkExtension") \
    .config("spark.sql.hive.implementation", "org.apache.spark.sql.hive.client.DliHiveClientImpl") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取数据湖中的表格信息
query = "SELECT * FROM tid_sdi_ai4data.ai4data_enterprise_bak LIMIT 1000"  # 仅读取前1000行进行示例
data = spark.sql(query)


# 添加数据行的索引
data = data.withColumn("index", monotonically_increasing_id())
data.persist(StorageLevel.MEMORY_AND_DISK)
# 显示前几行数据
# data.show()

# # 打印Schema
# data.printSchema()

# 初始化和分析清洗器
print("初始化清洗器和分析依赖关系...")
Edges, singles, multis = PreParamClassier(cleaners)
source_sets, target_sets, explain, processing_order = discover_cleaner_associations(Edges)

print("执行层级和目标模型分类...")
levels, models, nodes = associations_classier(multis, source_sets, target_sets)
print("执行层级 (并行组):", levels)
print("目标模型分类:", models)
# editRuleDict = {}
# editRuleLists = []
# editRules = NOOP()  # 初始化无操作规则
for level_index, level in enumerate(nodes):
    print(f"\n处理第 {level_index + 1} 层级, 包含节点: {level}")

    for node in level:
        if node in models and models[node]:
            # 定义一个空集合
            sset = set()
            tset = set()
            for m in models[node]:
                sset = sset.union(m.source)
                tset = tset.union(m.target)
            sset = list(sset)  # 变成列表
            tset = list(tset)
            print(f"  抽样处理：源属性 {sset} -> 目标属性 {node}")
            if len(tset) == 1:#无环
                sample_Block_df = Generate_Sample(data, sset, tset)
            else:#有环
                sample_Block_df = Generate_Sample(data, sset, tset, models=models[node])
            # editRule = NOOP()
            # editRuleList = []
            print(f"  在 spark 的分块数: {len(sample_Block_df)}")
            for blockData in sample_Block_df:
                blockData.show()
                # sample_df = blockData.toPandas()
                # print(f"数据块大小: {sample_df.shape[0]}")

            #     preCleaners = [single for single in singles if
            #                    any(attr_set in sset + [node] for attr_set in single.domain)]
            #     _, output, _, _ = customclean(sample_df, precleaners=preCleaners)
            #     blockmodels = None
            #     weights = [1] * len(models[node])
            #     for model, weight in zip(models[node], weights):
            #         if blockmodels is None:
            #             blockmodels = model * weight
            #         else:
            #             blockmodels += model * weight
            #     editRule1, output, editRuleList1, _ = customclean(output, cleaners=[blockmodels], partition=sset)
            #     editRule *= editRule1
            #     editRuleList.extend(editRuleList1)
            # print(f"  当前流程挖掘的清洗规则: {editRule}")

            # editRules *= editRule  # 累积应用的清洗规则
            # editRuleLists.extend(editRuleList)
            # editRuleDict[str(node)] = editRuleList

# print("\n累积的编辑规则:", str(len(editRuleDict)))
#
# print("\n清洗规则溯源分析:")
# grouped_opinfo = cleaner_grouping(nodes, models)
# single_opinfo = getSingle_opinfo(singles)
# group_weights = getOpWeights(editRuleDict, grouped_opinfo)
# print(group_weights)
# print("\n清洗流程展示:")
# sorted_dict = sort_opinfo_by_weights(grouped_opinfo, group_weights)
#
# # 生成调整后的 PlantUML 文本,并且绘制 plantuml
# OpPlantuml = generate_plantuml_corrected(single_opinfo, grouped_opinfo, sorted_dict)
# # print(plantuml_text)
# PlantUML().process_str(OpPlantuml)
# print("\n绘制清洗流程图:", "Plantuml.svg")
#
# print("\n应用挖掘到的编辑规则和清洗流程:")
# sparkEditRules = transformRules(editRuleLists)
# return sparkEditRules
