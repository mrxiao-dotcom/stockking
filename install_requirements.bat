@echo off
chcp 65001

echo Installing dependencies...

:: 卸载现有的 mysql-connector-python
pip uninstall -y mysql-connector-python

:: 安装特定版本的 mysql-connector-python（包含C扩展）
pip install mysql-connector-python==8.0.26 --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple

:: 安装 PyMySQL 作为备选
pip install pymysql -i https://pypi.tuna.tsinghua.edu.cn/simple

:: 安装其他依赖
pip install pandas>=1.3.0 -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install numpy>=1.21.0 -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install tushare>=1.2.89 -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install pyinstaller>=5.6.2 -i https://pypi.tuna.tsinghua.edu.cn/simple

echo Dependencies installation completed!
pause 