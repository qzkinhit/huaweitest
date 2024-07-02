Spark Command: /usr/local/huaweijre-8/bin/java -cp /opt/spark/conf/:/opt/spark/jars/*:/opt/hadoop/conf/ -Xmx15G -XX:CICompilerCount=2 -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:+UseGCOverheadLimit -XX:+ExitOnOutOfMemoryError -XX:-OmitStackTraceInFastThrow -Xloggc:/var/log/Bigdata/spark/gc.log -XX:+PrintGCDetails -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=20 -XX:GCLogFileSize=10M -XX:+PrintGCDateStamps -Dlog4j.configuration=file:/opt/hadoop/conf/log4j-executor.properties -Dlog4j.configuration.watch=true -Djetty.version=x.y.z -Dcarbon.properties.filepath=/opt/hadoop/conf/carbon.properties -Dluxor.rpc.netty.dispatcher.numThreads=128 -Dscc.configuration.path=/opt/Bigdata/components/current/conf/scc/scc.conf -Dscc.configuration.domain=6 -Dscc.nativeLibrary.enabled=true -Dalluxio.master.hostname=alluxio-master-0.ns-18588.svc.cluster.local -Dalluxio.master.rpc.addresses=alluxio-master-0.ns-18588.svc.cluster.local:19998 -Dalluxio.job.master.rpc.addresses=alluxio-master-0.ns-18588.svc.cluster.local:20001 -Dlog4j.configuratorClass=org.apache.hadoop.fs.log4j.DliPropertyConfigurator -Dlog4j.logger.turnOffLogClass=log4j.logger.com.obs.services.AbstractClient org.apache.spark.deploy.SparkSubmit --deploy-mode client --conf spark.driver.bindAddress=172.16.128.117 --properties-file /opt/spark/conf/spark.properties --class org.apache.spark.deploy.PythonRunner obs://d-tid-cnnorth9/ai4data/hitzekai/test.py
========================================
log4j:ERROR turnOffLogClassName = log4j.logger.com.obs.services.AbstractClient
log4j:WARN No such property [maxBackupIndex] in uk.org.simonsite.log4j.appender.TimeAndSizeRollingAppender.
log4j:ERROR Could not find value for key log4j.appender.stdout
log4j:ERROR Could not instantiate appender named "stdout".
log4j:ERROR Could not find value for key log4j.appender.stdout
log4j:ERROR Could not instantiate appender named "stdout".
log4j:WARN No such property [maxBackupIndex] in uk.org.simonsite.log4j.appender.TimeAndSizeRollingAppender.
log4j:ERROR Could not find value for key log4j.appender.stdout
log4j:ERROR Could not instantiate appender named "stdout".
log4j:ERROR Could not find value for key log4j.appender.stdout
log4j:ERROR Could not instantiate appender named "stdout".
ERROR StatusLogger Log4j2 could not find a logging implementation. Please add log4j-core to the classpath. Using SimpleLogger to log to the console...
Exception in thread "main" org.apache.hadoop.security.AccessControlException: getFileStatus on obs://d-tid-cnnorth9/ai4data/hitzekai/test.py: ResponseCode[403],ErrorCode[AccessDenied],ErrorMessage[Request Error.],RequestId[000001907220D045934CC16D705C67D9]
	at org.apache.hadoop.fs.obs.OBSCommonUtils.translateException(OBSCommonUtils.java:418)
	at org.apache.hadoop.fs.obs.OBSCommonUtils.translateException(OBSCommonUtils.java:676)
	at org.apache.hadoop.fs.obs.OBSObjectBucketUtils.innerGetObjectStatus(OBSObjectBucketUtils.java:600)
	at org.apache.hadoop.fs.obs.OBSFileSystem.innerGetFileStatus(OBSFileSystem.java:1724)
	at org.apache.hadoop.fs.obs.OBSCommonUtils.lambdagetFileStatusWithRetrygetFileStatusWithRetry8(OBSCommonUtils.java:1462)
	at org.apache.hadoop.fs.obs.OBSInvoker.retryByMaxTime(OBSInvoker.java:68)
	at org.apache.hadoop.fs.obs.OBSInvoker.retryByMaxTime(OBSInvoker.java:55)
	at org.apache.hadoop.fs.obs.OBSCommonUtils.getFileStatusWithRetry(OBSCommonUtils.java:1461)
	at org.apache.hadoop.fs.obs.OBSFileSystem.getFileStatus(OBSFileSystem.java:1700)
	at org.apache.hadoop.fs.FilterFileSystem.getFileStatus(FilterFileSystem.java:473)
	at org.apache.hadoop.fs.FileSystem.isFile(FileSystem.java:1845)
	at org.apache.spark.util.Utils$.fetchHcfsFile(Utils.scala:922)
	at org.apache.spark.util.Utils$.doFetchFile(Utils.scala:899)
	at org.apache.spark.deploy.DependencyUtils$.downloadFile(DependencyUtils.scala:139)
	at org.apache.spark.deploy.SparkSubmit.anonfunanonfunprepareSubmitEnvironment$9(SparkSubmit.scala:396)
	at scala.Option.map(Option.scala:230)
	at org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment(SparkSubmit.scala:396)
	at org.apache.spark.deploy.SparkSubmit.orgapacheapachesparkdeploydeploySparkSubmit$$runMain(SparkSubmit.scala:938)
	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:183)
	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:206)
	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:93)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1083)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1092)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Caused by: com.obs.services.exception.ObsException: Error message:Request Error.OBS servcie Error Message. -- ResponseCode: 403, ResponseStatus: Forbidden, RequestId: 000001907220D045934CC16D705C67D9, HostId: 32AAAQAAEAABAAAQAAEAABAAAQAAEAABCS/MFwso7EBpExvGPR0YGPMMAh1jIdXF
	at com.obs.services.internal.utils.ServiceUtils.changeFromServiceException(ServiceUtils.java:533)
	at com.obs.services.AbstractClient.doActionWithResult(AbstractClient.java:402)
	at com.obs.services.AbstractObjectClient.getObjectMetadata(AbstractObjectClient.java:606)
	at org.apache.hadoop.fs.obs.OBSObjectBucketUtils.getObjectMetadata(OBSObjectBucketUtils.java:334)
	at org.apache.hadoop.fs.obs.OBSObjectBucketUtils.innerGetObjectStatus(OBSObjectBucketUtils.java:588)
	... 21 more
