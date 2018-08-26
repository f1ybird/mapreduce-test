[debug_conf]: https://my.oschina.net/feinik/blog/1634690 "debug参考"







### hadoop2.6.5分布式环境

[通过IDEA远程调试服务器代码][debug_conf]

##### 1.shell增加临时设置
> export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000"

##### 2.服务器端执行MapReduce任务，如
hadoop jar mapreduce-task.jar com.os.china.mapreduce.weather.JobRun /file/weather.txt /out

##### 3.在IDEA中新建一个远程Debug调试配置

#### mapreduce demo
##### 1.wordCount
##### 2.sortMapper
##### 3.sortStep