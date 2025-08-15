@echo off
REM Установите путь к OpenSSL и keytool (при необходимости)
SET OPENSSL_PATH=%OPENSSL_HOME%\bin\openssl.exe
SET KEYTOOL_PATH=%JAVA_HOME%\bin\keytool.exe

REM Укажите расположение ca.key и ca.crt
SET CA_KEY=ca.key
SET CA_CRT=ca.crt

REM Настройки пользователя
SET USER_NAME=kafka_user

REM 1. Генерация CSR и ключа для пользователя
%OPENSSL_PATH% req -new -newkey rsa:2048 -keyout clients-creds/%USER_NAME%.key -out clients-creds/%USER_NAME%.csr -config clients-creds/%USER_NAME%.cnf -nodes

REM 2. Подпись сертификата пользователя с использованием корневого сертификата CA
%OPENSSL_PATH% x509 -req -days 3650 -in clients-creds/%USER_NAME%.csr -CA %CA_CRT% -CAkey %CA_KEY% -CAcreateserial -out clients-creds/%USER_NAME%.crt -extfile clients-creds/%USER_NAME%.cnf -extensions v3_req

REM 3. Создание PKCS#12 хранилища для пользователя
%OPENSSL_PATH% pkcs12 -export -inkey clients-creds/%USER_NAME%.key -in clients-creds/%USER_NAME%.crt -out clients-creds/%USER_NAME%.p12 -name %USER_NAME% -passout pass:changeit

REM 4. Импорт PKCS#12 хранилища в JKS keystore
%KEYTOOL_PATH% -importkeystore -srckeystore clients-creds/%USER_NAME%.p12 -srcstoretype PKCS12 -srcstorepass changeit -destkeystore clients-creds/%USER_NAME%.keystore.jks -deststoretype JKS -deststorepass changeit -noprompt -alias %USER_NAME%

REM 5. Создание PEM файла для пользователя
%OPENSSL_PATH% pkcs12 -in clients-creds/%USER_NAME%.p12 -out clients-creds/%USER_NAME%.pem -nodes -passin pass:changeit

REM 6. Добавление CA в truststore пользователя
%KEYTOOL_PATH% -import -trustcacerts -file %CA_CRT% -alias ca -keystore clients-creds/%USER_NAME%.truststore.jks -storepass changeit -noprompt

@echo on
echo Keys and certificate generated successfully for user %USER_NAME%!
