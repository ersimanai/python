# 依赖

## 安装 pip

  wget https://bootstrap.pypa.io/get-pip.py

  python get-pip.py

## 如有依赖包报错

  sudo pip install <pkg> 

## 安装打包程序

  sudo pip install pyinstaller

# 打包

  pyinstaller -F pfmon.py

  会在当前目录下生成可执行文件 dist/pfmon

  将 dist/pfmon 手动copy到<branch根目录>/bin下 

  执行 pack-xcloud.sh 或 make-pack.sh

# 运行 

  pfmon 要根据相对路径搜索ds.xml及行云pid

  必须将其放在 xpkg/bin下面

  正常执行 start-xcloud.sh 时 如果检测到pfmon在bin下面, 将自动启动pfmon

  手动执行需要cd到bin下面 执行 

  nohup ./pfmon > ../log/pfmon.log 2>&1 &

# 检测状态

  可以通过 ps -eu 检查有没有 pfmon

# 停止

  pfmon会随着行云的stop脚本自动停止

  如要手动停止, 可以根据xpkg/bin下面的pid_mon
  
  kill `cat bin/pid_mon`
