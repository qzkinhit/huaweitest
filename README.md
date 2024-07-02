Traceback (most recent call last):
  File "/tmp/spark-fe972b4c-640f-49a9-a1fd-a9d643665737/test.py", line 26, in <module>
    main()
  File "/tmp/spark-fe972b4c-640f-49a9-a1fd-a9d643665737/test.py", line 16, in main
    df = spark.sql(query)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/session.py", line 723, in sql
  File "/opt/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1305, in __call__
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 117, in deco
pyspark.sql.utils.AnalysisException: java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: DLCatalog session metastore client class is not been config, hive-ext.dlcatalog.metastore.session.client.class
