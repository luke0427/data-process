package com.ropeok.dataprocess.utils;

public class Constants {

    public static final String TABLE_JOB_INFO = "JOB_INFO_V2";

    public static final String TABLE_STEP_INFO = "STEP_INFO_V2";

    public static final String PROC_LAST_DATE = "${LAST_DATE}";

    public static final String PROC_PARAM_DATE = "${PARAM_DATE}";

    public static final String PROC_LAST_TIME = "${LAST_TIME}";

    public static final String PROC_PARAM_TIME = "${PARAM_TIME}";

    public static final String PROC_CURRENT_DATE = "${CURRENT_DATE}";

    public static final String PROC_CURRENT_TIME = "${CURRENT_TIME}";

    public static final String START_TIME = "START_TIME";

    public static final String END_TIME = "END_TIME";

    /**
     * 步骤/组件属性：线程数
     */
    public static final String THREAD_NUMS = "threads";

    /**
     *  步骤体：SQL语句
     */
    public static final String STEP_BODY = "step_body";

    /**
     * 步骤/组件属性：数据库访问地址
     */
    public static final String URL = "url";

    /**
     * 步骤/组件属性：用户名
     */
    public static final String USER_NAME = "username";

    /**
     * 步骤/组件属性：密码
     */
    public static final String PASSWORD = "password";

    /**
     * 步骤/组件属性：抓取数
     */
    public static final String FETCH_SIZE = "fetch_size";

    /**
     * 步骤/组件属性：批量数
     */
    public static final String BATCH_SIZE = "batch_size";

    /**
     * 步骤/组件属性：是否增量
     */
    public static final String INCRE = "incre";

    /**
     * 步骤/组件属性：增量字段
     */
    public static final String INCRE_COLUMN = "incre_column";

    /**
     * 步骤/组件属性：增量值
     */
    public static final String INCRE_START_TIME = "incre_start_time";

    /**
     * 步骤/组件属性：丢弃
     */
    public static final String THROW = "throw";

    /**
     * 步骤/组件属性：模式[insert, upsert, update]
     */
    public static final String MODE = "mode";
    public static final String MODE_INSERT = "insert";
    public static final String MODE_UPSERT = "upsert";
    public static final String MODE_UPDATE = "update";

    /**
     * 步骤/组件属性：字段
     */
    public static final String COLUMN = "column";

    /**
     * 步骤/组件属性：字段列表
     */
    public static final String COLUMNS = "columns";

    /**
     * 步骤/组件属性：插入字段列表
     */
    public static final String IN_COLUMNS = "in_columns";
    /**
     * 步骤/组件属性：更新字段列表
     */
    public static final String UP_COLUMNS = "up_columns";

    /**
     * 步骤/组件属性：唯一性字段列表
     */
    public static final String UNIQUE_COLUMNS = "unique_columns";

    /**
     * 步骤/组件属性：表
     */
    public static final String TABLE = "table";

    /**
     * 步骤/组件属性：日期格式
     */
    public static final String DATE_FORMAT = "date_format";

    /**
     * 步骤/组件属性：类型
     */
    public static final String TYPES = "types";

    /**
     * 步骤/组件属性：服务器IP，机器名
     */
    public static final String HOST = "host";

    /**
     * 步骤/组件属性：服务器IP,机器名列表
     */
    public static final String HOSTS = "hosts";

    /**
     * 步骤/组件属性：端口
     */
    public static final String PORT = "port";

    /**
     * 步骤/组件属性：端口列表
     */
    public static final String PORTS = "ports";

    /**
     * 步骤/组件属性：集群名称
     */
    public static final String CLUSTER_NAME = "cluster_name";

    /**
     * 步骤/组件属性：索引名称
     */
    public static final String INDEX = "index";

    /**
     * 步骤/组件属性：索引类型
     */
    public static final String IDX_TYPE = "idx_type";

    /**
     * 步骤/组件属性：主键字段名
     */
    public static final String ID_COLUMN = "id_column";

    /**
     * 步骤/组件属性：路由字段名
     */
    public static final String ROUTING_COLUMN = "routing_column";

    /**
     * 步骤/组件属性：关联字段名称
     */
    public static final String PARENT_COLUMN = "parent_column";

    /**
     * 步骤/组件属性：条件分支
     */
    public static final String SWITCH_CASE = "switch_case";

    /**
     * 步骤/组件属性：上传路径
     */
    public static final String UPLOAD_PATH = "upload_path";

    /**
     * 步骤/组件属性：上传目录
     */
    public static final String UPLOAD_DIR = "upload_dir";

    /**
     * 步骤/组件属性：上传图片的文件名字段
     */
    public static final String NAME_COLUMN = "name_column";

    /**
     * 步骤/组件属性：赋值字段名
     */
    public static final String TO_COLUMN = "to_column";

    /**
     * 步骤/组件属性：目的字段列表
     */
    public static final String TO_COLUMNS = "to_columns";

    /**
     * 步骤/组件属性：前缀
     */
    public static final String PREFIX = "prefix";

    /**
     * 步骤/组件属性：参数字段
     */
    public static final String PARAM_COLUMN = "param_column";

    /**
     * 步骤/组件属性：ES: 判断删除的字段名
     */
    public static final String DEL_COLUMN = "del_column";

    /**
     * 步骤/组件属性：缓存字段
     */
    public static final String CACHE_COLUMN = "cache_column";

    /**
     * 步骤/组件属性：缓存字段列表
     */
    public static final String CACHE_COLUMNS = "cache_columns";

    /**
     * 步骤/组件属性：缓存：KEY字段名
     */
    public static final String KEY_COLUMN = "key_column";

    /**
     * 步骤/组件属性：类型字段名
     */
    public static final String TYPE_COLUMN = "type_column";

    /**
     * 步骤/组件属性：出生日期字段名
     */
    public static final String BIRTHDAY_COLUMN = "birthday_column";

    /**
     * 步骤/组件属性：性别字段名
     */
    public static final String SEX_COLUMN = "sex_column";

    /**
     * 步骤/组件属性：缓存值存字段名
     */
    public static final String CACHE_TO_COLUMN = "cache_to_column";

    /**
     * 步骤/组件属性：目录名
     */
    public static final String DIR_NAME = "dir_name";

    /**
     * 步骤/组件属性：删除的字段值
     */
    public static final String DEL_VALUE = "del_value";

    /**
     * 步骤/组件属性：旷世服务器地址
     */
    public static final String KS_URL = "ks_url";

    /**
     * 步骤/组件属性：商汤服务器地址
     */
    public static final String ST_URL = "st_url";

    /**
     * 步骤/组件属性：是否格式化
     */
    public static final String FORMAT = "format";

    /**
     * 步骤/组件属性：是否删除文件
     */
    public static final String REMOVE_FILE = "remove_file";

    /**
     * 步骤/组件属性：是否打印
     */
    public static final String PRINT = "print";

    /**
     * 步骤/组件属性：缓存类型：list,set,hash
     */
    public static final String CACHE_TYPE = "cache_type";

    /**
     * Redis缓存类型
     */
    public static final String CACHE_TYPE_LIST = "list";
    public static final String CACHE_TYPE_SET = "set";
    public static final String CACHE_TYPE_HASH = "hash";

    /**
     * 步骤/组件属性：持续时间
     */
    public static final String EXPIRE = "expire";

    /**
     * 步骤/组件属性：数据字段
     */
    public static final String DATA_COLUMN = "data_column";

}
