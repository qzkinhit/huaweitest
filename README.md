2024-07-02 13:17:59,133 | INFO  | [main] | INFO: Watching file:/opt/hadoop/conf/log4j-executor.properties for changes with interval : 60000 | com.huawei.hadoop.dynalogger.DynaLog4jWatcher.watchLog4jConfiguration(DynaLog4jWatcher.java:79)
2024-07-02 13:17:59,851 | WARN  | [main] | The configuration key 'spark.maxRemoteBlockSizeFetchToMem' has been deprecated as of Spark 3.0 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 13:17:59,857 | WARN  | [main] | The configuration key 'spark.reducer.maxReqSizeShuffleToMem' has been deprecated as of Spark 2.3 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 13:17:59,857 | WARN  | [main] | The configuration key 'spark.blacklist.enabled' has been deprecated as of Spark 3.1.0 and may be removed in the future. Please use spark.excludeOnFailure.enabled | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 13:18:00,011 | WARN  | [main] | The configuration key 'spark.maxRemoteBlockSizeFetchToMem' has been deprecated as of Spark 3.0 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 13:18:00,012 | WARN  | [main] | The configuration key 'spark.reducer.maxReqSizeShuffleToMem' has been deprecated as of Spark 2.3 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 13:18:00,012 | WARN  | [main] | The configuration key 'spark.blacklist.enabled' has been deprecated as of Spark 3.1.0 and may be removed in the future. Please use spark.excludeOnFailure.enabled | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 13:18:00,809 | INFO  | [main] | Cache missed the key(obs\dli-resource-080da2722d0010d31f0dc0119f88acaa\YGNDXT6ZAXK7NWETCYKP\\c.h.l.f.l.BasicOBSCredentialProvider\true\/OBSFileSystem-Trash/\8M). | org.apache.hadoop.fs.LuxorOBSFileSystem.getFileSystem(LuxorOBSFileSystem.java:113)
2024-07-02 13:18:00,838 | INFO  | [main] | create ObsClient using credentialsProvider: com.huawei.luxor.fs.luxorfs.BasicOBSCredentialProvider | org.apache.hadoop.fs.obs.OBSSecurityProviderUtil.createObsSecurityProvider(OBSSecurityProviderUtil.java:47)
2024-07-02 13:18:01,428 | INFO  | [main] | Changing view acls to: omm | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 13:18:01,429 | INFO  | [main] | Changing modify acls to: omm | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 13:18:01,429 | INFO  | [main] | Changing view acls groups to:  | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 13:18:01,430 | INFO  | [main] | Changing modify acls groups to:  | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 13:18:01,430 | INFO  | [main] | SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(omm); groups with view permissions: Set(); users  with modify permissions: Set(omm); groups with modify permissions: Set() | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 13:18:01,493 | INFO  | [main] | Cache missed the key(obs\d-tid-cnnorth9\HL0RIYKO9708F1F4TZT6\\c.h.l.f.l.LuxorRpcOBSCredentialProvider\false\\8M). | org.apache.hadoop.fs.LuxorOBSFileSystem.getFileSystem(LuxorOBSFileSystem.java:113)
2024-07-02 13:18:01,503 | INFO  | [main] | create ObsClient using credentialsProvider: com.huawei.luxor.fs.luxorfs.LuxorRpcOBSCredentialProvider | org.apache.hadoop.fs.obs.OBSSecurityProviderUtil.createObsSecurityProvider(OBSSecurityProviderUtil.java:47)
2024-07-02 13:18:01,735 | INFO  | [shutdown-hook-0] | Shutdown hook called | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 13:18:01,736 | INFO  | [shutdown-hook-0] | Deleting directory /tmp/spark-eaa1fd26-76db-48b3-b854-150009c03fa2 | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
