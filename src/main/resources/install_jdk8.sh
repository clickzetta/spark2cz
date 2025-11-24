# 1. 创建安装目录
mkdir -p /usr/local/java

# 2. 下载JDK 8
cd /usr/local/java
wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
"https://download.oracle.com/otn-pub/java/jdk/8u341-b10/424b9da4b48848379167015dcc250d8d/jdk-8u341-linux-x64.tar.gz"

# 如果上面的链接失效，可以使用OpenJDK：
# yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel

# 3. 解压JDK
tar -xzvf jdk-8u341-linux-x64.tar.gz

# 4. 设置环境变量
cat >> /etc/profile << 'EOF'
# Java Environment
export JAVA_HOME=/usr/local/java/jdk1.8.0_341
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH
EOF

# 5. 使环境变量生效
source /etc/profile

# 6. 验证安装
java -version

# 7. 配置默认Java版本（如果系统中存在多个Java版本）
alternatives --install /usr/bin/java java ${JAVA_HOME}/bin/java 1
alternatives --install /usr/bin/javac javac ${JAVA_HOME}/bin/javac 1
alternatives --install /usr/bin/jar jar ${JAVA_HOME}/bin/jar 1

# 8. 验证环境变量
echo $JAVA_HOME
echo $PATH