Traceback (most recent call last):
  File "/tmp/spark-da70ebfc-a137-4281-a705-50687e1cb3a5/main2.py", line 62, in <module>
    rules = CleanBlockonSingle(cleaners, data)
  File "/tmp/spark-da70ebfc-a137-4281-a705-50687e1cb3a5/Clean.py", line 88, in CleanBlockonSingle
    sparkEditRules = transformRules(editRuleLists)
  File "/tmp/spark-da70ebfc-a137-4281-a705-50687e1cb3a5/mypack_env.zip/AnalyticsCache/handle_rules.py", line 77, in transformRules
  File "/tmp/spark-da70ebfc-a137-4281-a705-50687e1cb3a5/mypack_env.zip/AnalyticsCache/spark_rule_model.py", line 135, in __init__
  File "/tmp/spark-da70ebfc-a137-4281-a705-50687e1cb3a5/mypack_env.zip/AnalyticsCache/spark_rule_model.py", line 8, in formatString
TypeError: can only concatenate str (not "numpy.float64") to str
