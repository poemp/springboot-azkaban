package org.poem.vo;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * @author sangfor
 */
@Data
public class ExecTaskDetailPlanVO {


    /**
     * 需要执行的sql
     */
    private String beforeSql;

    /**
     * 执行sql
     */
    private String aroundSql;

    /**
     * 数据源的地址
     */
    private String sourceIp;
    /**
     * 数据源的端口
     */
    private Integer sourcePort;

    /**
     * 数据库
     */
    private String sourceSchema;
    /**
     * 数据源的密码
     */
    private String sourcePasswd;

    /**
     * 数据源的用户信息
     */
    private String sourceUserName;

    /**
     * 目标库的数据的类型
     * TDataSourceType
     */
    private String sourceSourceType;


    /**
     * 目标数据源的url
     */
    private String targetIp;

    /**
     * 数据源的端口
     */
    private Integer targetPort;

    /**
     * 目标数据源的密码
     */
    private String targetPasswd;

    /**
     * 数据库
     */
    private String targetSchema;

    /**
     * 目标数据源的用户信息
     */
    private String targetUserName;
    /**
     * 目标库的数据的类型
     * TDataSourceType
     */
    private String targetSourceType;

    /**
     *
     */
    public static void clearData() {
        ExecTaskDetailPlanVO clearDate = new ExecTaskDetailPlanVO();
        clearDate.setAroundSql("delete from  wedding_view_bigdata where view_date= DATE_FORMAT(now(), \"%Y-%m-%d\")");
        clearDate.setTargetUserName("root");
        clearDate.setTargetSourceType("1");
        clearDate.setTargetSchema("xxl_job");
        clearDate.setTargetPort(3306);
        clearDate.setTargetPasswd("123456");
        clearDate.setTargetIp("192.168.51.152");
        System.out.println(JSONObject.toJSONString(clearDate));
    }

    /**
     * `id` varchar(36) NOT NULL COMMENT '主键',
     * `view_count` int(11) DEFAULT '0' COMMENT '访问量',
     * `view_people_count` int(11) DEFAULT '0' COMMENT '访问人数',
     * `view_date` datetime DEFAULT NULL COMMENT '访问日期',
     * `attr` varchar(255) DEFAULT NULL COMMENT '扩展字段',
     */
    public static void pushData() {
        ExecTaskDetailPlanVO clearDate = new ExecTaskDetailPlanVO();
        clearDate.setTargetUserName("root");
        clearDate.setTargetSourceType("1");
        clearDate.setTargetSchema("xxl_job");
        clearDate.setTargetPort(3306);
        clearDate.setTargetPasswd("123456");
        clearDate.setTargetIp("192.168.51.152");

        clearDate.setSourceUserName("root");
        clearDate.setSourceSourceType("1");
        clearDate.setSourceSchema("zgdc_warehouse_source");
        clearDate.setSourcePort(43306);
        clearDate.setSourcePasswd("zgdc");
        clearDate.setSourceIp("test");
        clearDate.setBeforeSql("SELECT\n" +
                "\t`code`,\n" +
                "\tDATE_FORMAT(time, \"%Y-%m-%d\") AS `time`,\n" +
                "\tcount(1) AS count,\n" +
                "\tCOUNT(DISTINCT user_id) as user_id\n" +
                "FROM\n" +
                "\tt_event_tracking_frequency_info_origin\n" +
                "where 1=1\n" +
                "AND code = \"10001\"\n" +
                "and time >  CONCAT(DATE_FORMAT(NOW(), \"%Y-%m-%d\"),\" 00:00:00\")\n" +
                "and time <= CONCAT(DATE_FORMAT(NOW(), \"%Y-%m-%d\"),\" 23:59:59\")\n" +
                "group by `code`,DATE_FORMAT(time, \"%Y-%m-%d\")   \n" +
                "order by DATE_FORMAT(time, \"%Y-%m-%d\") desc");
        clearDate.setAroundSql("insert into wedding_view_bigdata (id, view_count,view_people_count,view_date,attr) values('1', '@count','@user_id', '@time' ,null)");
        System.out.println(JSONObject.toJSONString(clearDate));
    }

    public static void main(String[] args) {
        clearData();
        pushData();
    }
}
