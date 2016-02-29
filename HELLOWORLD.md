Hello World
===========

## 修改conf/yarn-site.xml
将yarn.resourcemanager.hostname为ResourceManager的主机名或IP地址
```xml
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>172.16.8.106</value>
  </property>
```

## 修改samza任务配置文件
src/main/config/topic-replicator.properties
###  kafka broker列表
```java
systems.kafka.producer.bootstrap.servers=m106:9092
```

### 修改zookeeper地址

```java
systems.kafka.consumer.zookeeper.connect=m106:2181/kafka
```

### yarn程序包路径
之后我们编译的tar.gz包会放在这个位置
```java
yarn.package.path=hdfs://m107:8020/tmp/samza/${project.artifactId}-${pom.version}-dist.tar.gz
// m107为hdfs NameNode的主机名
```

## 编译程序包
```bash
cd samza-hello-samza
mvn clean package -DskipTests
```

## 将本地编译好的程序包上传到可以访问yarn的节点（如m106），并执行程序
### 在yarn节点上创建目录
登陆到m106
```bash
ssh root@m106
```
创建目录
```bash
mkdir /tmp/samza
```

### 上传程序包到yarn节点
```bash
scp target/hello-samza-0.10.0-dist.tar.gz root@m106:/tmp/samza/
```

### 上传程序包到hdfs(yarn.package.path中的路径)
```bash
cd /tmp/samza/
tar -xzf hello-samza-0.10.0-dist.tar.gz
hdfs dfs -put -f hello-samza-0.10.0-dist.tar.gz /tmp/samza/
```
### 运行samza任务
bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=config/word-feed.properties

## 向topic:word-raw队列添加测试数据
kafka-console-producer --broker-list m106:9092 --topic word-raw
[输入]hello
[输入]world

查看测试数据是否成功插入
kafka-console-consumer --zookeeper m106:2181/kafka --from-beginning --topic word-raw
正常应该看到如下输出
hello
world

## 监控目标topic
```bash
kafka-console-consumer --zookeeper m106:2181/kafka --from-beginning --topic word-raw-output
```
如果成功，则会看到如下的输出
hello
world

## 查看ApplicationMaster WEB界面
http://m106:8088/proxy/application_1456712041160_0040/

# Reference
## [编译](http://samza.apache.org/startup/hello-samza/0.9/)
