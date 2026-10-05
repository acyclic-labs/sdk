@echo off
setlocal
set "PLUGIN_JAR=%USERPROFILE%\.m2\repository\io\grpc\protoc-gen-grpc-kotlin\1.4.3\protoc-gen-grpc-kotlin-1.4.3-jdk8.jar"
if not exist "%PLUGIN_JAR%" (
  echo grpc-kotlin compiler %PLUGIN_JAR% is missing 1>&2
  exit /b 2
)
java -jar "%PLUGIN_JAR%" %*
