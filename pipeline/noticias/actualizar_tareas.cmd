@echo off
rem Tareas programadas del archivo de prensa (noticias v3, fase D). Registrar una vez:
rem     pipeline\noticias\actualizar_tareas.cmd registrar
rem Las tareas llaman a este mismo archivo con «semanal» o «diaria».
setlocal
set PYTHONIOENCODING=utf-8
set NEAHUB=%~dp0..\..
set LOGS=C:\Users\ant\hexoma-fuentes\noticias
set PY=C:\Python314\python.exe
cd /d "%NEAHUB%"

if /i "%1"=="semanal" (
  echo ===== %date% %time% >> "%LOGS%\actualizar.log"
  "%PY%" pipeline\noticias\actualizar.py >> "%LOGS%\actualizar.log" 2>&1
  goto :eof
)
if /i "%1"=="diaria" (
  "%PY%" pipeline\noticias\vialidad_nacional.py >> "%LOGS%\vialidad_nacional.log" 2>&1
  goto :eof
)
if /i "%1"=="registrar" (
  schtasks /create /f /tn "Hexoma\Noticias semanal" /sc weekly /d SUN /st 04:00 /tr "\"%~f0\" semanal"
  schtasks /create /f /tn "Hexoma\Vialidad Nacional diaria" /sc daily /st 07:00 /tr "\"%~f0\" diaria"
  goto :eof
)
echo uso: actualizar_tareas.cmd semanal ^| diaria ^| registrar
