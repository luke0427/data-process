# data-process

## 功能概述
数据处理框架旨在将所有数据处理过程进行统一配置管理。通过配置的方式实现复杂的数据处理逻辑任务。

## 主要模块说明
handler：V1任务转换组件
inout：V1任务输入输出组件
job：V1任务实现
web：V1触发式任务入口及任务管理
v2：V2任务实现

## V1任务说明
V1任务主要实现一个输入源一个输出源的数据处理过程。
* 输入
    * JDBCProcInput：关系库数据抽取
    * ESProcInput：ES数据抽取

* 输出
    * ESProcOutput：ES数据输出
    * RedisProcOutput：Redis输出

* 转换
    * AddColumnProcHandler：增加字段组件
    * ColumnMappingProcHandler：字段映射组件
    * DateFormatProcHandler：日期格式化组件
    * LonLatProcHandler：经纬度格式化组件
    * NotNullProcHandler：非空验证组件
    * JDBCCacheProcHandler：本地缓存组件
    * PersonIdProcHandler：人员ID生成组件
    * PKMD5ProcHandler：主键生成组件
    * RedisProcHandler：Redis缓存获取组件
    * RemoveBlankProcHandler：移除空字段组件
    * RemoveColumnProcHandler：移除字段组件
    * SfzhValidProcHandler：身份证验证组件
    * TrimProcHandler：去头尾空格组件
    * ValueFilterProcHandler：值过滤组件

## V2任务说明
V2任务主要实现多输入多输出源的数据处理过程。

* 输入
    * JDBCInput：关系库数据抽取
    * RandomIntInput：随机整数输入

* 输出
    * ESOutput：ES数据输出
    * JDBCOutput：关系库数据输出
    * RedisOutput：Redis输出
    * RkImageUpload：图片上传

* 转换
    * ColumnMappingTrans：字段映射组件
    * CopyColumnTrans：复制字段组件
    * DateFormatTrans：日期转换组件
    * FaceAnalyzerTrans：人脸检测组件
    * FileMD5Trans：文件MD5计算组件
    * LonLatTrans：经纬度格式化组件
    * NotNullValidTrans：非空验证组件
    * PersonExistValidTrans：人员是否存在验证组件
    * PersonIdTrans：人员ID组件
    * PhotoUrlColumnTrans：照片地址字段组件
    * PKMD5Trans：主键生成组件
    * RemoveBlankColumnTrans：移除空白字段组件
    * RemoveColumnTrans：移除字段组件
    * SfzhValidTrans：身份证验证组件
    * SwitchTrans：数据分发组件
