打包指定spark版本
```sh
mvn clean package -Pspark35 -DskipTests
```
打包测试包指定spark版本
```sh
mvn clean package -Pspark35,local-test -DskipTests
```
打包测试包指定spark4 uc版本
```sh
  mvn clean package -Pspark4-uc,local-test -DskipTests   
```