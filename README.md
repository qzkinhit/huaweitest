[2024/07/05 10:50:16 GMT+0800] [INFO] ===============================================
[2024/07/05 10:50:16 GMT+0800] [INFO] ===============================================
[2024/07/05 10:50:16 GMT+0800] [INFO] ===============================================
[2024/07/05 10:50:16 GMT+0800] [INFO] Execute user name is d_cn9_qianzekai_qWX1313729, user id is cfa427d070554e3ab6e8022e3315aa25, job id is AC098C66466E4E4DB97F4FB74EF59F48hzj9rW9U
[2024/07/05 10:50:16 GMT+0800] [INFO] Spark feature is basic
[2024/07/05 10:50:16 GMT+0800] [INFO] Spark image is null
[2024/07/05 10:50:16 GMT+0800] [INFO] prepare to submit dli spark job.
[2024/07/05 10:50:16 GMT+0800] [INFO] submit dli spark job with Service-Transaction-Id: fda0ba08-fd88-4524-9b53-305b8329ea30
[2024/07/05 10:50:16 GMT+0800] [DEBUG] 
[2024/07/05 10:50:16 GMT+0800] [INFO] try to submit dli spark job, url is https://dli.cn-north-9.myhuaweicloud.com/v2.0/06bc26236400105b2f3fc002a3460114/batches, request body is 
[2024/07/05 10:50:16 GMT+0800] [INFO] 
{
    "file":"obs://d-tid-cnnorth9/zekaitest/testCleanPre.py",
    "pyFiles":[
        "obs://d-tid-cnnorth9/zekaitest/sample.zip"
    ],
    "conf":{
        "spark.dli.metaAccess.enable":"true"
    },
    "name":"zekaitestspark3",
    "driverMemory":"15G",
    "driverCores":4,
    "executorMemory":"8G",
    "executorCores":2,
    "numExecutors":14,
    "feature":"basic",
    "cluster_name":"dii_demo_ai4data_flink_queue01",
    "sc_type":"C",
    "catalog_name":"dli",
    "spark_version":"3.3.1"
}
[2024/07/05 10:50:16 GMT+0800] [DEBUG] 
[2024/07/05 10:50:16 GMT+0800] [DEBUG] 
[2024/07/05 10:50:16 GMT+0800] [INFO] DLI Spark submit failed. Response is 
[2024/07/05 10:50:16 GMT+0800] [INFO] 
{
    "error_code":"DLI.0001",
    "error_msg":"Cannot download obs object in d-tid-cnnorth9 bucket, please set the agency parameter in the conf. The key is \"spark.dli.job.agency.name\", and the value is the name of your agency."
}
[2024/07/05 10:50:16 GMT+0800] [DEBUG] 
[2024/07/05 10:50:16 GMT+0800] [DEBUG] 
[2024/07/05 10:50:16 GMT+0800] [ERROR] Failed to submit dli spark job, status code is 400 BAD_REQUEST ,error msg is {"error_code":"DLI.0001","error_msg":"Cannot download obs object in d-tid-cnnorth9 bucket, please set the agency parameter in the conf. The key is \"spark.dli.job.agency.name\", and the value is the name of your agency."} 
[2024/07/05 10:50:16 GMT+0800] [ERROR] Exception message: DLFException: Failed to submit dli spark job, status code is 400 BAD_REQUEST ,error msg is {"error_code":"DLI.0001","error_msg":"Cannot download obs object in d-tid-cnnorth9 bucket, please set the agency parameter in the conf. The key is \"spark.dli.job.agency.name\", and the value is the name of your agency."} 
[2024/07/05 10:50:16 GMT+0800] [ERROR] Root Cause message:DLFException: Failed to submit dli spark job, status code is 400 BAD_REQUEST ,error msg is {"error_code":"DLI.0001","error_msg":"Cannot download obs object in d-tid-cnnorth9 bucket, please set the agency parameter in the conf. The key is \"spark.dli.job.agency.name\", and the value is the name of your agency."} 
