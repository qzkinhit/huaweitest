2024-07-02 14:27:48,801 | INFO  | [main] | INFO: Watching file:/opt/hadoop/conf/log4j-executor.properties for changes with interval : 60000 | com.huawei.hadoop.dynalogger.DynaLog4jWatcher.watchLog4jConfiguration(DynaLog4jWatcher.java:79)
2024-07-02 14:27:49,548 | WARN  | [main] | The configuration key 'spark.maxRemoteBlockSizeFetchToMem' has been deprecated as of Spark 3.0 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 14:27:49,555 | WARN  | [main] | The configuration key 'spark.reducer.maxReqSizeShuffleToMem' has been deprecated as of Spark 2.3 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 14:27:49,556 | WARN  | [main] | The configuration key 'spark.blacklist.enabled' has been deprecated as of Spark 3.1.0 and may be removed in the future. Please use spark.excludeOnFailure.enabled | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 14:27:49,695 | WARN  | [main] | The configuration key 'spark.maxRemoteBlockSizeFetchToMem' has been deprecated as of Spark 3.0 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 14:27:49,696 | WARN  | [main] | The configuration key 'spark.reducer.maxReqSizeShuffleToMem' has been deprecated as of Spark 2.3 and may be removed in the future. Please use the new key 'spark.network.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 14:27:49,696 | WARN  | [main] | The configuration key 'spark.blacklist.enabled' has been deprecated as of Spark 3.1.0 and may be removed in the future. Please use spark.excludeOnFailure.enabled | org.apache.spark.internal.Logging.logWarning(Logging.scala:69)
2024-07-02 14:27:50,505 | INFO  | [main] | Cache missed the key(obs\dli-resource-080da2722d0010d31f0dc0119f88acaa\YGNDXT6ZAXK7NWETCYKP\\c.h.l.f.l.BasicOBSCredentialProvider\true\/OBSFileSystem-Trash/\8M). | org.apache.hadoop.fs.LuxorOBSFileSystem.getFileSystem(LuxorOBSFileSystem.java:113)
2024-07-02 14:27:50,546 | INFO  | [main] | create ObsClient using credentialsProvider: com.huawei.luxor.fs.luxorfs.BasicOBSCredentialProvider | org.apache.hadoop.fs.obs.OBSSecurityProviderUtil.createObsSecurityProvider(OBSSecurityProviderUtil.java:47)
2024-07-02 14:27:51,147 | INFO  | [main] | Changing view acls to: omm | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 14:27:51,148 | INFO  | [main] | Changing modify acls to: omm | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 14:27:51,148 | INFO  | [main] | Changing view acls groups to:  | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 14:27:51,149 | INFO  | [main] | Changing modify acls groups to:  | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 14:27:51,149 | INFO  | [main] | SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(omm); groups with view permissions: Set(); users  with modify permissions: Set(omm); groups with modify permissions: Set() | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 14:27:51,212 | INFO  | [main] | Cache missed the key(obs\d-tid-cnnorth9\OPXXCL72RVOUEZDQX0IV\\c.h.l.f.l.LuxorRpcOBSCredentialProvider\false\\8M). | org.apache.hadoop.fs.LuxorOBSFileSystem.getFileSystem(LuxorOBSFileSystem.java:113)
2024-07-02 14:27:51,213 | INFO  | [main] | create ObsClient using credentialsProvider: com.huawei.luxor.fs.luxorfs.LuxorRpcOBSCredentialProvider | org.apache.hadoop.fs.obs.OBSSecurityProviderUtil.createObsSecurityProvider(OBSSecurityProviderUtil.java:47)
2024-07-02 14:27:51,510 | INFO  | [shutdown-hook-0] | Shutdown hook called | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
2024-07-02 14:27:51,511 | INFO  | [shutdown-hook-0] | Deleting directory /tmp/spark-223c914e-9fcc-448b-a11c-d99daf425351 | org.apache.spark.internal.Logging.logInfo(Logging.scala:57)
