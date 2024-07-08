初始化清洗器和分析依赖关系...
执行层级和目标模型分类...
执行层级 (并行组): [[{'registered_capital'}, {'annual_turnover'}, {'enterprise_id'}, {'enterprise_address'}, {'social_credit_code'}, {'establishment_date'}], [{'registered_capital_scale'}, {'annual_turnover_interval'}, {'latitude'}, {'longitude'}, {'enterprise_name'}, {'establishment_time'}], [{'province'}, {'industry_first'}, {'enterprise_type'}], [{'city'}, {'industry_second'}], [{'district'}, {'industry_third'}]]
目标模型分类: {'establishment_time': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f21b3195bd0>], 'registered_capital_scale': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f21b326c9d0>], 'industry_third': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a892410>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a550>], 'industry_second': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a490>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a510>], 'industry_first': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a4d0>], 'annual_turnover_interval': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a590>], 'province': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a5d0>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a6d0>], 'city': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a650>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a610>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a7d0>], 'district': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a690>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a710>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a810>], 'latitude': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a750>], 'longitude': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a790>], 'enterprise_type': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a850>], 'enterprise_name': [<SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a890>, <SampleScrubber.cleaner.multiple.AttrRelation object at 0x7f211a83a910>]}

处理第 1 层级, 包含节点: ['registered_capital', 'annual_turnover', 'enterprise_id', 'enterprise_address', 'social_credit_code', 'establishment_date']

处理第 2 层级, 包含节点: ['registered_capital_scale', 'annual_turnover_interval', 'latitude', 'longitude', 'enterprise_name', 'establishment_time']
  抽样处理：源属性 ['registered_capital'] -> 目标属性 registered_capital_scale
核心样本抽取时间（秒）: 0.8250632286071777
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['annual_turnover'] -> 目标属性 annual_turnover_interval
核心样本抽取时间（秒）: 0.17204046249389648
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['enterprise_address'] -> 目标属性 latitude
核心样本抽取时间（秒）: 0.15479469299316406
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['enterprise_address'] -> 目标属性 longitude
核心样本抽取时间（秒）: 0.23204827308654785
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['social_credit_code', 'enterprise_id'] -> 目标属性 enterprise_name
核心样本抽取时间（秒）: 0.1882171630859375
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['establishment_date'] -> 目标属性 establishment_time
核心样本抽取时间（秒）: 0.2269914150238037
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP

处理第 3 层级, 包含节点: ['province', 'industry_first', 'enterprise_type']
  抽样处理：源属性 ['latitude', 'longitude', 'enterprise_address'] -> 目标属性 province
核心样本抽取时间（秒）: 0.19437289237976074
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['enterprise_name'] -> 目标属性 industry_first
核心样本抽取时间（秒）: 0.18288183212280273
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['enterprise_name'] -> 目标属性 enterprise_type
核心样本抽取时间（秒）: 0.16875958442687988
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP

处理第 4 层级, 包含节点: ['city', 'industry_second']
  抽样处理：源属性 ['latitude', 'province', 'longitude', 'enterprise_address'] -> 目标属性 city
核心样本抽取时间（秒）: 0.23282194137573242
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['enterprise_name', 'industry_first'] -> 目标属性 industry_second
核心样本抽取时间（秒）: 0.16170716285705566
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP

处理第 5 层级, 包含节点: ['district', 'industry_third']
  抽样处理：源属性 ['latitude', 'longitude', 'enterprise_address', 'city'] -> 目标属性 district
核心样本抽取时间（秒）: 0.16419625282287598
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP
  抽样处理：源属性 ['enterprise_name', 'industry_second'] -> 目标属性 industry_third
核心样本抽取时间（秒）: 0.14940261840820312
  在 spark 的分块数: 1
数据块大小: 0
WARNING:root:No partitioning rule specified, processing might be slow
  当前流程挖掘的清洗规则: NOOP
NOOP

累积的编辑规则: 13

清洗规则溯源分析:

处理第 1 层级, 包含节点: ['registered_capital', 'annual_turnover', 'enterprise_id', 'enterprise_address', 'social_credit_code', 'establishment_date']

处理第 2 层级, 包含节点: ['registered_capital_scale', 'annual_turnover_interval', 'latitude', 'longitude', 'enterprise_name', 'establishment_time']

处理第 3 层级, 包含节点: ['province', 'industry_first', 'enterprise_type']

处理第 4 层级, 包含节点: ['city', 'industry_second']

处理第 5 层级, 包含节点: ['district', 'industry_third']
{'registered_capital_scale': [0], 'annual_turnover_interval': [0], 'latitude': [0], 'longitude': [0], 'enterprise_name': [0, 0], 'establishment_time': [0], 'province': [0, 0], 'industry_first': [0], 'enterprise_type': [0], 'city': [0, 0, 0], 'industry_second': [0, 0], 'district': [0, 0, 0], 'industry_third': [0, 0]}

应用挖掘到的编辑规则和清洗流程:
[]
执行时间: 30.6323 秒
验证是否成功修复
Traceback (most recent call last):
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 63, in deco
  File "/opt/spark/python/lib/py4j-0.10.7-src.zip/py4j/protocol.py", line 328, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o1332.selectExpr.
: org.apache.spark.sql.catalyst.parser.ParseException: 
mismatched input ',' expecting <EOF>(line 1, pos 48)

== SQL ==
original.enterprise_id as original_enterprise_id, cleaned.enterprise_id as cleaned_enterprise_id
------------------------------------------------^^^

	at org.apache.spark.sql.catalyst.parser.ParseException.withCommand(ParseDriver.scala:251)
	at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parse(ParseDriver.scala:127)
	at org.apache.spark.sql.execution.SparkSqlParser.parse(SparkSqlParser.scala:52)
	at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parseExpression(ParseDriver.scala:46)
	at org.apache.spark.sql.parser.DliSqlParser.parseExpression(DliSqlParser.scala:88)
	at org.apache.spark.sql.Dataset$$anonfun$selectExpr$1.apply(Dataset.scala:1396)
	at org.apache.spark.sql.Dataset$$anonfun$selectExpr$1.apply(Dataset.scala:1395)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
	at scala.collection.Iterator$class.foreach(Iterator.scala:891)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
	at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
	at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
	at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
	at scala.collection.AbstractTraversable.map(Traversable.scala:104)
	at org.apache.spark.sql.Dataset.selectExpr(Dataset.scala:1395)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:238)
	at java.lang.Thread.run(Thread.java:750)


During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/tmp/spark-2991dcd1-15b8-4667-b808-a3fa0f52c3ca/main2.py", line 86, in <module>
    diffData = joinedData.selectExpr(*select_expr)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 1340, in selectExpr
  File "/opt/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py", line 1257, in __call__
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 73, in deco
pyspark.sql.utils.ParseException: "\nmismatched input ',' expecting <EOF>(line 1, pos 48)\n\n== SQL ==\noriginal.enterprise_id as original_enterprise_id, cleaned.enterprise_id as cleaned_enterprise_id\n------------------------------------------------^^^\n"
