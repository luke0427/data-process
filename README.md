# data-process

## 任务配置：
* 上传任务配置文件
* 上传同名任务将会覆盖(后续可以增加一个backup功能)
* 任务文件存储路径 uploads/{任务名}/任务文件

**注意事项:**
* 文件名要求：
    * 任务配置：{任务名}.xlsx
    * 全局参数配置：{任务名}_param.xlsx
    * 用例参数配置：{任务名}_data.properties,其中参数名命名格式为{任务名}.xxx.xxx
    * 数据源配置：{任务名}_ds.properties

## 任务列表：
* 查看任务列表
* 执行任务
* 下载任务报告：{任务名}_report.xlsx
* 删除任务


### 集群模式说明
* 集群模式打开方式：
    * zoo.cfg
        * server.1=127.0.0.1:2888:3888        IP为服务器IP
    * application.yml 
        * themis.servers.mode.cluster: true   值为true即打开集群模式
        * themis.servers: 127.0.0.1:2181      IP需要配置成ThemisServer的IP（zoo.cfg中的server.1中配置的IP一致，如果有多个服务器用逗号分隔）
    * 按顺序启动LaunchThemisServer、ThemisBootstrap
* 可以获取所有集群内的任务并执行
* 当某台服务器下线的时候会下线相关任务，当服务器上线的时候会上线相关任务
