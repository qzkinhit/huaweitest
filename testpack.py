import platform
import sys
import pyspark
from pyspark.sql import SparkSession

# 获取操作系统名称和版本
os_name = platform.system()
os_version = platform.version()
os_release = platform.release()

# 获取 CPU 架构
cpu_arch = platform.machine()

# 打印系统信息
print(f"Operating System: {os_name}")
print(f"OS Version: {os_version}")
print(f"OS Release: {os_release}")
print(f"CPU Architecture: {cpu_arch}")

# 获取Python版本
print("Python version")
print(sys.version)
print("Version info.")
print(sys.version_info)

# 获取 PySpark 版本
pyspark_version = pyspark.__version__

# 获取 Spark 版本
spark = SparkSession.builder.appName("VersionCheck").getOrCreate()
spark_version = spark.version

# 打印 PySpark 和 Spark 版本信息
print(f"PySpark Version: {pyspark_version}")
print(f"Spark Version: {spark_version}")

# 停止SparkSession
spark.stop()
