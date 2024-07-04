conda create -n pyenv --copy -y python=3.7 numpy pandas xgboost  # 创建虚拟环境pyenv,并安装需要的第三方依赖包，以numpy pandas xgboost为例（当前python版本为3.7）
conda install -c johnsnowlabs spark-nlp==4.2.8  # 安装spark-nlp依赖包
cd /home/gecongcong/anaconda3/envs/  # 进入该虚拟环境所在目录
chmod -R 777 pyenv  # 增加权限，避免打包上传MRS后，服务器端解压失败（重要！）
zip -r pyenv_x86.zip pyenv/  # 打包该虚拟环境pyenv
explorer.exe .  # 若通过windows内置wsl的linux系统操作上述步骤，则打包后的文件存储位置可通过该命令访问
