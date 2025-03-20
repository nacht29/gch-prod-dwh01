@echo off
setlocal enabledelayedexpansion

:CHECK_INTERNET
REM Use curl to check if a website is reachable
curl -I https://www.google.com > nul 2>&1
if %errorlevel% equ 0 (
    echo Internet is UP. Sending email...
    powershell -Command "$credential = New-Object System.Management.Automation.PSCredential ('yanzhechan@gmail.com', (ConvertTo-SecureString 'vfts ltjp lssr trpz' -AsPlainText -Force)); Send-MailMessage -From 'yanzhechan@gmail.com' -To 'yanzhechan@gmail.com' -Subject 'Internet Restored' -Body 'Upload failed earlier. Internet connection is back.' -SmtpServer 'smtp.gmail.com' -Port 587 -UseSsl -Credential $credential"
    exit /b 0
) else (
    echo Internet is DOWN. Retrying in 60 seconds...
    timeout /t 60 > nul
    goto CHECK_INTERNET
)
