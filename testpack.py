import platform

# 获取操作系统名称和版本
os_name = platform.system()
os_version = platform.version()
os_release = platform.release()

# 获取 CPU 架构
cpu_arch = platform.machine()

# 打印信息
print(f"Operating System: {os_name}")
print(f"OS Version: {os_version}")
print(f"OS Release: {os_release}")
print(f"CPU Architecture: {cpu_arch}")

# 获取Python版本
import sys
print("Python version")
print(sys.version)
print("Version info.")
print(sys.version_info)
