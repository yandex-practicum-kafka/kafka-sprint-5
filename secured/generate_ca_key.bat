@echo off

REM Установите путь к OpenSSL
SET OPENSSL_PATH=%OPENSSL_HOME%\bin\openssl.exe

REM  Выполняем команду OpenSSL для генерации самоподписанного сертификата (CA - Certificate Authority).
%OPENSSL_PATH% req -x509 -new -nodes -keyout ca.key -sha256 -days 3650 -out ca.crt -config ca.cnf

REM  Объединяем сертификат (ca.crt) и личный ключ (ca.key) в один файл (ca.pem).
copy /b ca.crt + ca.key ca.pem