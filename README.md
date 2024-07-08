批次 1/11 已写入表: cleanedData100w
Traceback (most recent call last):
  File "/tmp/spark-34dd6e31-6678-4dfe-8ec5-98f97047416d/main2_100w.py", line 83, in <module>
    batch_data.write.mode("append").saveAsTable("tid_sdi_ai4data.cleanedData100w")
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 778, in saveAsTable
  File "/opt/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py", line 1257, in __call__
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 63, in deco
  File "/opt/spark/python/lib/py4j-0.10.7-src.zip/py4j/protocol.py", line 328, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o1331.saveAsTable.
: java.lang.AssertionError: assertion failed: schema should be empty when ctas.
	at scala.Predef$.assert(Predef.scala:170)
