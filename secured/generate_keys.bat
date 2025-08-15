@echo off
REM Установите путь к OpenSSL и keytool
SET OPENSSL_PATH=%OPENSSL_HOME%\bin\openssl.exe
SET KEYTOOL_PATH=%JAVA_HOME%\bin\keytool.exe

REM Укажите расположение ca.key и ca.crt
SET CA_KEY=ca.key
SET CA_CRT=ca.crt

REM 4-6. Генерация CSR и ключей для брокеров
%OPENSSL_PATH% req -new -newkey rsa:2048 -keyout kafka-0-creds/kafka-0.key -out kafka-0-creds/kafka-0.csr -config kafka-0-creds/kafka-0.cnf -nodes
%OPENSSL_PATH% req -new -newkey rsa:2048 -keyout kafka-1-creds/kafka-1.key -out kafka-1-creds/kafka-1.csr -config kafka-1-creds/kafka-1.cnf -nodes
%OPENSSL_PATH% req -new -newkey rsa:2048 -keyout kafka-2-creds/kafka-2.key -out kafka-2-creds/kafka-2.csr -config kafka-2-creds/kafka-2.cnf -nodes

REM 7-9. Подпись сертификатов для брокеров с использованием корневого сертификата CA
%OPENSSL_PATH% x509 -req -days 3650 -in kafka-0-creds/kafka-0.csr -CA %CA_CRT% -CAkey %CA_KEY% -CAcreateserial -out kafka-0-creds/kafka-0.crt -extfile kafka-0-creds/kafka-0.cnf -extensions v3_req
%OPENSSL_PATH% x509 -req -days 3650 -in kafka-1-creds/kafka-1.csr -CA %CA_CRT% -CAkey %CA_KEY% -CAcreateserial -out kafka-1-creds/kafka-1.crt -extfile kafka-1-creds/kafka-1.cnf -extensions v3_req
%OPENSSL_PATH% x509 -req -days 3650 -in kafka-2-creds/kafka-2.csr -CA %CA_CRT% -CAkey %CA_KEY% -CAcreateserial -out kafka-2-creds/kafka-2.crt -extfile kafka-2-creds/kafka-2.cnf -extensions v3_req

REM 10-12. Создание PKCS#12 хранилища для каждого брокера
%OPENSSL_PATH% pkcs12 -export -inkey kafka-0-creds/kafka-0.key -in kafka-0-creds/kafka-0.crt -out kafka-0-creds/kafka-0.p12 -name kafka-0 -passout pass:changeit
%OPENSSL_PATH% pkcs12 -export -inkey kafka-1-creds/kafka-1.key -in kafka-1-creds/kafka-1.crt -out kafka-1-creds/kafka-1.p12 -name kafka-1 -passout pass:changeit
%OPENSSL_PATH% pkcs12 -export -inkey kafka-2-creds/kafka-2.key -in kafka-2-creds/kafka-2.crt -out kafka-2-creds/kafka-2.p12 -name kafka-2 -passout pass:changeit

REM 13-15. Создание JKS keystore для каждого брокера
%KEYTOOL_PATH% -importkeystore -srckeystore kafka-0-creds/kafka-0.p12 -srcstoretype PKCS12 -srcstorepass changeit -destkeystore kafka-0-creds/kafka.keystore.jks -deststoretype JKS -deststorepass changeit -noprompt -alias kafka-0
%KEYTOOL_PATH% -importkeystore -srckeystore kafka-1-creds/kafka-1.p12 -srcstoretype PKCS12 -srcstorepass changeit -destkeystore kafka-1-creds/kafka.keystore.jks -deststoretype JKS -deststorepass changeit -noprompt -alias kafka-1
%KEYTOOL_PATH% -importkeystore -srckeystore kafka-2-creds/kafka-2.p12 -srcstoretype PKCS12 -srcstorepass changeit -destkeystore kafka-2-creds/kafka.keystore.jks -deststoretype JKS -deststorepass changeit -noprompt -alias kafka-2

REM 16-18. Создание PEM файлов для каждого брокера
%OPENSSL_PATH% pkcs12 -in kafka-0-creds/kafka-0.p12 -out kafka-0-creds/kafka-0.pem -nodes -passin pass:changeit
%OPENSSL_PATH% pkcs12 -in kafka-1-creds/kafka-1.p12 -out kafka-1-creds/kafka-1.pem -nodes -passin pass:changeit
%OPENSSL_PATH% pkcs12 -in kafka-2-creds/kafka-2.p12 -out kafka-2-creds/kafka-2.pem -nodes -passin pass:changeit

REM Создание truststore для каждого брокера
%KEYTOOL_PATH% -import -trustcacerts -file %CA_CRT% -alias ca -keystore kafka-0-creds/kafka.truststore.jks -storepass changeit -noprompt
%KEYTOOL_PATH% -import -trustcacerts -file %CA_CRT% -alias ca -keystore kafka-1-creds/kafka.truststore.jks -storepass changeit -noprompt
%KEYTOOL_PATH% -import -trustcacerts -file %CA_CRT% -alias ca -keystore kafka-2-creds/kafka.truststore.jks -storepass changeit -noprompt

@echo on
echo All keys and certificates generated successfully!
