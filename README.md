Traceback (most recent call last):
  File "/tmp/spark-6a499f31-1a85-436e-818f-a83d8a7aadec/test.py", line 27, in <module>
    main()
  File "/tmp/spark-6a499f31-1a85-436e-818f-a83d8a7aadec/test.py", line 17, in main
    df = spark.sql(query)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/session.py", line 723, in sql
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1305, in __call__
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 117, in deco
pyspark.sql.utils.AnalysisException: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: DLCatalog session metastore client class is not been config, hive-ext.dlcatalog.metastore.session.client.class
