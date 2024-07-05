[[[Operating System: Linux
OS Version: #1 SMP Wed Nov 25 18:33:06 UTC 2020
OS Release: 3.10.0-862.14.1.5.h520.eulerosv2r7.x86_64
CPU Architecture: x86_64
Python version
3.7.9 (default, Jun  3 2023, 09:00:54) 
[GCC 7.3.0]
Version info.
sys.version_info(major=3, minor=7, micro=9, releaselevel='final', serial=0)



conda create -n pyenv --copy -y python=3.7 numpy pandas xgboost  # 创建虚拟环境pyenv,并安装需要的第三方依赖包，以numpy pandas xgboost为例（当前python版本为3.7）
conda install -c johnsnowlabs spark-nlp==4.2.8  # 安装spark-nlp依赖包
cd /home/gecongcong/anaconda3/envs/  # 进入该虚拟环境所在目录
chmod -R 777 pyenv  # 增加权限，避免打包上传MRS后，服务器端解压失败（重要！）
zip -r pyenv_x86.zip pyenv/  # 打包该虚拟环境pyenv
explorer.exe .  # 若通过windows内置wsl的linux系统操作上述步骤，则打包后的文件存储位置可通过该命令访问
](https://cmc-szv.clouddragon.huawei.com/cmcversion/index/search)](https://cmc-szv.clouddragon.huawei.com/cmcversion/index/search)](https://cmc-szv.clouddragon.huawei.com/cmcversion/index/search)
