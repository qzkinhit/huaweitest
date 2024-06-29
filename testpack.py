# -*- coding: utf-8 -*-
import sys
from pyspark import SparkConf, SparkContext
import importlib
import pkg_resources

def check_package_version(package_name, expected_version):
    try:
        package = importlib.import_module(package_name)
        installed_version = pkg_resources.get_distribution(package_name).version
        if installed_version == expected_version:
            print(f"{package_name} version {installed_version} is correctly installed.")
        else:
            print(f"{package_name} version {installed_version} is installed, but {expected_version} is expected.")
    except ImportError:
        print(f"{package_name} is not installed.")
    except pkg_resources.DistributionNotFound:
        print(f"{package_name} is not installed.")

if __name__ == "__main__":
    # 创建SparkConf
    conf = SparkConf().setAppName("PackageVersionCheck")
    # 创建SparkContext
    sc = SparkContext(conf=conf)

    packages = {
        "matplotlib": "3.7.2",
        "numpy": "1.24.3",
        "pandas": "1.5.3",
        "pyspark": "3.1.1",
        "gensim": "4.3.0",
        "Distance": "0.1.3",
        "scikit-learn": "1.3.0",
        "dateparser": "1.1.8",
        "streamlit": "1.33.0",
        "Pillow": "10.0.1",
        "joblib": "1.2.0",
        "networkx": "3.1",
        "plotly": "5.9.0",
        "httplib2": "0.22.0",
        "pydeck": "0.8.1b0",
        "altair": "5.2.0"
    }

    # 在 Spark 环境中并行检查包的版本
    rdd = sc.parallelize(packages.items())
    rdd.foreach(lambda package: check_package_version(package[0], package[1]))

    # 停止SparkContext
    sc.stop()
