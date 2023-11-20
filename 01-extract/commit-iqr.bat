@echo off
cd /d "F:\Repositories\ETL-Pipeline\01-extract\csv\raw-iqair"

:: Extract the date and time and remove any trailing spaces
for /f "tokens=*" %%a in ('echo %DATE%') do set mydate=%%a
for /f "tokens=*" %%a in ('echo %TIME%') do set mytime=%%a

:: Format the date and time
:: Assuming the format of %mydate% is DD/MM/YYYY and %mytime% is HH:MM:SS.CC
for /f "tokens=1-3 delims=/" %%a in ("%mydate%") do (
    set year=%%c
    set month=%%b
    set day=%%a
)

:: Add leading zero to month and day if necessary
if 1%month% LSS 110 set month=0%month:~-1%
if 1%day% LSS 110 set day=0%day:~-1%

:: Get hour part from time and ensure it is two digits
for /f "tokens=1 delims=:" %%a in ("%mytime%") do set hour=%%a
if "%hour:~0,1%"==" " set hour=0%hour:~1%
if 1%hour% LSS 110 set hour=0%hour%

:: Combine to form YYYY-MM-DD-HH
set formattedDateTime=%year%-%month%-%day%-%hour%

:: Construct the commit message
set commit_message=feat: IQAir Data %formattedDateTime%
echo %commit_message%

git add *
git commit -m "%commit_message%"
git push origin main