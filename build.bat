@echo off
echo ========================================
echo Сборка discount_checker.exe
echo ========================================
echo.

echo Установка зависимостей...
pip install -r requirements.txt

echo.
echo Сборка EXE файла...
pyinstaller --onefile --noconsole --name discount_checker discount_checker.py

echo.
echo ========================================
echo Сборка завершена!
echo EXE файл находится в папке dist\
echo ========================================
pause