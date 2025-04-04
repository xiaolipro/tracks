@echo off
echo 正在安装Python包...
set PYTHON_PATH=C:\Users\BSI\AppData\Local\Programs\Python\Python312\python.exe

echo 正在安装selenium...
"%PYTHON_PATH%" -m pip install selenium
if errorlevel 1 (
    echo selenium安装失败
    pause
    exit /b 1
)

echo 正在安装pandas...
"%PYTHON_PATH%" -m pip install pandas
if errorlevel 1 (
    echo pandas安装失败
    pause
    exit /b 1
)

echo 正在安装openpyxl...
"%PYTHON_PATH%" -m pip install openpyxl
if errorlevel 1 (
    echo openpyxl安装失败
    pause
    exit /b 1
)

echo 正在安装webdriver-manager...
"%PYTHON_PATH%" -m pip install webdriver-manager
if errorlevel 1 (
    echo webdriver-manager安装失败
    pause
    exit /b 1
)

echo 所有包安装完成！
echo 正在验证安装...
"%PYTHON_PATH%" -c "import selenium; import pandas; import openpyxl; from webdriver_manager.chrome import ChromeDriverManager; print('所有包导入成功！')"
if errorlevel 1 (
    echo 包验证失败
    pause
    exit /b 1
)

echo 安装和验证都成功完成！
pause 