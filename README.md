input:{"algorithm_type":"DIRTY_DATA_DISCOVERY","batch_job_engine_type":"DLI","obs_path":"obs://d-tid-cnnorth9/dli-trans-dii/ai4data/algorithms/ai4data_test_results/ds_test_enterprise_business_result_12","is_csv_output":false,"is_json_output":false,"is_column_dem":false,"is_db_output":true,"db_name":"tid_sdi_ai4data","table_name":"ai4data_enterprise_business_dirty_data_results","table_sources":[{"db_name":"tid_sdi_ai4data","table_name":"ai4data_enterprise_business_info_update","id_column_name":"enterprise_id","column_names":["create_time","enterprise_name","social_credit_code","establishment_date","legal_entity"]}]}
[2024/07/02 13:17:48 GMT+0800] [INFO] ===============================================
[2024/07/02 13:17:48 GMT+0800] [INFO] ===============================================
[2024/07/02 13:17:48 GMT+0800] [INFO] ===============================================
[2024/07/02 13:17:48 GMT+0800] [INFO] Execute user name is d_cn9_qianzekai_qWX1313729, user id is cfa427d070554e3ab6e8022e3315aa25, job id is 4EEC9FEC54664CA7B9C53E1DAF6A617ELFEfw2CN
[2024/07/02 13:17:48 GMT+0800] [INFO] Spark feature is basic
[2024/07/02 13:17:48 GMT+0800] [INFO] Spark image is null
[2024/07/02 13:17:48 GMT+0800] [INFO] prepare to submit dli spark job.
[2024/07/02 13:17:48 GMT+0800] [INFO] submit dli spark job with Service-Transaction-Id: 06a328ee-1831-4c44-877c-5ed4840d26e6
[2024/07/02 13:17:48 GMT+0800] [DEBUG] 
[2024/07/02 13:17:48 GMT+0800] [INFO] try to submit dli spark job, url is https://dli.cn-north-9.myhuaweicloud.com/v2.0/06bc26236400105b2f3fc002a3460114/batches, request body is 
[2024/07/02 13:17:48 GMT+0800] [INFO] 
{
    "file":"obs://d-tid-cnnorth9/ai4data/hitzekai/test.py",
    "conf":{
        "spark.memory.offHeap.enabled":"true",
        "spark.rpc.message.maxSize":"1024",
        "spark.driver.maxResultSize":"200G",
        "spark.memory.offHeap.size":"200G",
        "spark.dli.metaAccess.enable":"true"
    },
    "name":"ds_test",
    "modules":[
        "sys.datasource.dli-inner-table"
    ],
    "driverMemory":"15G",
    "driverCores":4,
    "executorMemory":"8G",
    "executorCores":2,
    "numExecutors":14,
    "feature":"basic",
    "cluster_name":"tid_dii_flink_queue01",
    "sc_type":"C",
    "catalog_name":"dli",
    "spark_version":"3.1.1"
}
[2024/07/02 13:17:48 GMT+0800] [DEBUG] 
[2024/07/02 13:17:51 GMT+0800] [DEBUG] 
[2024/07/02 13:17:51 GMT+0800] [INFO] DLI Spark submit Succeed. Response is 
[2024/07/02 13:17:51 GMT+0800] [INFO] 
{
    "statusCode":0,
    "message":null,
    "errorInfo":null,
    "success":false,
    "is_success":false,
    "id":"1e016f5a-54e9-496e-9767-41af8cb2ebbd",
    "state":"starting",
    "log":[
        
    ],
    "error_code":null,
    "error_msg":null
}
[2024/07/02 13:17:51 GMT+0800] [DEBUG] 
[2024/07/02 13:17:51 GMT+0800] [INFO] Spark UI path of DLI Spark JOB [ds_test] is [https://console.huaweicloud.com/dli/web/06bc26236400105b2f3fc002a3460114/batch/1e016f5a-54e9-496e-9767-41af8cb2ebbd/sparkui].
[2024/07/02 13:17:51 GMT+0800] [INFO] Spark Driver path of DLI Spark JOB [ds_test] is [https://console.huaweicloud.com/dli/#/main/jobs/spark/submitLog/1e016f5a-54e9-496e-9767-41af8cb2ebbd?type=RUN].
[2024/07/02 13:18:11 GMT+0800] [DEBUG] DLI Spark is running...
[2024/07/02 13:18:11 GMT+0800] [INFO] ATTENTION: IF NO SUBMIT LOG, MAY BE SUBMIT TIMEOUT AND WE KILL THE JOB ENFORCE.

2024-07-02 13:17:52,861 | WARN  | main | The configuration key 'spark.reducer.maxReqSizeShuffleToMem' has been deprecated as of Spark 2.3 and may be removed in the future. Please use the new key 'spark.maxRemoteBlockSizeFetchToMem' instead. | org.apache.spark.internal.Logging class.logWarning(Logging.scala:66)2024−07−0213:17:53,359|WARN|main|Unabletoloadnative−hadooplibraryforyourplatform...usingbuiltin−javaclasseswhereapplicable|org.apache.hadoop.util.NativeCodeLoader.<clinit>(NativeCodeLoader.java:60)2024−07−0213:17:54,078|INFO|main|Auto−configuringK8SclientusingcurrentcontextfromusersK8Sconfigfile|org.apache.spark.internal.Logging
 class.logInfo(Logging.scala:54)
2024-07-02 13:17:54,695 | WARN  | main | Driver's hostname would preferably be batch-session-1e016f5a-54e9-496e-9767-41af8cb2ebbd-e5a22f9071e0c40a-driver-svc, but this is too long (must be <= 63 characters). Falling back to use spark-7b050d9071e0c687-driver-svc as the driver service's name. | org.apache.spark.internal.Logging class.logWarning(Logging.scala:66)2024−07−0213:17:55,147|INFO|OkHttphttps://kubernetes.default.svc/...|Statechanged,newstate:podname:batch−session−1e016f5a−54e9−496e−9767−41af8cb2ebbdnamespace:ns−18589labels:app−>spark,dli−spark−app−id−>batch−session−1e016f5a−54e9−496e−9767−41af8cb2ebbd,organization−>DLI,spark−app−selector−>spark−3ebfc9d17fde4422bca0bcd89bcee319,spark−role−>driverpoduid:268d017f−17ca−4018−8fcd−135a66cd95f2creationtime:2024−07−02T05:17:55Zserviceaccountname:defaultvolumes:spark−logs,cluster−dir−base,spark−data−1,spark−localtime,scc−secret,hadoop−conf−volume,pod−template−volume,spark−local−dir−1,spark−conf−volume,default−token−rdhzgnodename:N/Astarttime:N/Aphase:Pendingcontainerstatus:N/A|org.apache.spark.internal.Logging
 class.logInfo(Logging.scala:54)
2024-07-02 13:17:55,153 | INFO  | OkHttp https://kubernetes.default.svc/... | State changed, new state: 
     pod name: batch-session-1e016f5a-54e9-496e-9767-41af8cb2ebbd
     namespace: ns-18589
     labels: app -> spark, dli-spark-app-id -> batch-session-1e016f5a-54e9-496e-9767-41af8cb2ebbd, organization -> DLI, spark-app-selector -> spark-3ebfc9d17fde4422bca0bcd89bcee319, spark-role -> driver
     pod uid: 268d017f-17ca-4018-8fcd-135a66cd95f2
     creation time: 2024-07-02T05:17:55Z
     service account name: default
     volumes: spark-logs, cluster-dir-base, spark-data-1, spark-localtime, scc-secret, hadoop-conf-volume, pod-template-volume, spark-local-dir-1, spark-conf-volume, default-token-rdhzg
     node name: 172.16.99.136
     start time: N/A
     phase: Pending
     container status: N/A | org.apache.spark.internal.Logging class.logInfo(Logging.scala:54)2024−07−0213:17:55,158|INFO|OkHttphttps://kubernetes.default.svc/...|Statechanged,newstate:podname:batch−session−1e016f5a−54e9−496e−9767−41af8cb2ebbdnamespace:ns−18589labels:app−>spark,dli−spark−app−id−>batch−session−1e016f5a−54e9−496e−9767−41af8cb2ebbd,organization−>DLI,spark−app−selector−>spark−3ebfc9d17fde4422bca0bcd89bcee319,spark−role−>driverpoduid:268d017f−17ca−4018−8fcd−135a66cd95f2creationtime:2024−07−02T05:17:55Zserviceaccountname:defaultvolumes:spark−logs,cluster−dir−base,spark−data−1,spark−localtime,scc−secret,hadoop−conf−volume,pod−template−volume,spark−local−dir−1,spark−conf−volume,default−token−rdhzgnodename:172.16.99.136starttime:2024−07−02T05:17:55Zphase:Pendingcontainerstatus:containername:spark−kubernetes−drivercontainerimage:swr.cn−north−9.myhuaweicloud.com/dli−image−x8664/spark:3.1.1−2.3.7.1720240614855611868450240.202406171112containerstate:waitingpendingreason:PodInitializing|org.apache.spark.internal.Logging
 class.logInfo(Logging.scala:54)
2024-07-02 13:17:55,312 | INFO  | main | Deployed Spark application batch-session-1e016f5a-54e9-496e-9767-41af8cb2ebbd with submission ID ns-18589:batch-session-1e016f5a-54e9-496e-9767-41af8cb2ebbd into Kubernetes | org.apache.spark.internal.Logging class.logInfo(Logging.scala:54)2024−07−0213:17:55,318|INFO|shutdown−hook−0|Shutdownhookcalled|org.apache.spark.internal.Logging
 class.logInfo(Logging.scala:54)
2024-07-02 13:17:55,318 | INFO  | shutdown-hook-0 | Deleting directory /tmp/spark-7855073d-587d-4b47-a8da-32976ca81243 | org.apache.spark.internal.Logging$class.logInfo(Logging.scala:54)
