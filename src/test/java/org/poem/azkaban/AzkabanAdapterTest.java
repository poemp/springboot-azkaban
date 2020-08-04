package org.poem.azkaban;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.poem.vo.ExecTaskDetailPlanVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class AzkabanAdapterTest {

    private static final Logger logger = LoggerFactory.getLogger(AzkabanAdapterTest.class);
    @Autowired
    private AzkabanAdapter azkabanAdapter;

    private static String projectName = "PROJECT_NAME";

    @Test
    public void createProject() throws IOException {
        String flowName = "PROJECT_NAME_FLOW_NAME";
        azkabanAdapter.deleteProject(projectName);
        azkabanAdapter.createProject(projectName, flowName);
    }

    @Test
    public void fetchProjectFlows(){
        JsonNode jsonNode = azkabanAdapter.fetchProjectFlows(projectName);
        System.out.println(jsonNode);
    }

    @Test
    public void updateFile(){
        String path = "C:\\Users\\Administrator\\Desktop\\target-test-package.zip";
        try {
            azkabanAdapter.uploadZip(path,projectName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void schedulePeriodBasedFlow() {
        String flowName = "job_test";
        String scheduleDate = "07/22/2014";
        String scheduleTime = "12,00,pm,PDT";
        String period = "5w";
        azkabanAdapter.schedulePeriodBasedFlow(projectName, flowName, scheduleDate, scheduleTime, period);
    }

    /**
     * 开始执行
     */
    @Test
    public void startFlow(){
        try {
            ExecTaskDetailPlanVO clearDate = new ExecTaskDetailPlanVO();
            clearDate.setAroundSql("delete from  wedding_view_bigdata where view_date= DATE_FORMAT(now(), \"%Y-%m-%d\")");
            clearDate.setTargetUserName("root");
            clearDate.setTargetSourceType("1");
            clearDate.setTargetSchema("xxl_job");
            clearDate.setTargetPort(3306);
            clearDate.setTargetPasswd("123456");
            clearDate.setTargetIp("192.168.51.152");
            Map<String, Object> optionalParams = Maps.newHashMap();
            optionalParams.put("clear_json", URLEncoder.encode(JSONObject.toJSONString(clearDate), "utf-8"));


            ExecTaskDetailPlanVO pushData = new ExecTaskDetailPlanVO();
            pushData.setTargetUserName("root");
            pushData.setTargetSourceType("1");
            pushData.setTargetSchema("xxl_job");
            pushData.setTargetPort(3306);
            pushData.setTargetPasswd("123456");
            pushData.setTargetIp("192.168.51.152");

            pushData.setSourceUserName("root");
            pushData.setSourceSourceType("1");
            pushData.setSourceSchema("zgdc_warehouse_source");
            pushData.setSourcePort(43306);
            pushData.setSourcePasswd("zgdc");
            pushData.setSourceIp("test");
            pushData.setBeforeSql("SELECT\n" +
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
            System.out.println(pushData.getBeforeSql());
            pushData.setAroundSql("insert into wedding_view_bigdata (id, view_count,view_people_count,view_date,attr) values('1', '@count','@user_id', '@time' ,null)");

            optionalParams.put("push_json", URLEncoder.encode(JSONObject.toJSONString(pushData), "utf-8"));

            String  execId = azkabanAdapter.startFlow(projectName,"DataPush",optionalParams);
            System.err.println(execId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取执行id
     */
    @Test
    public void executionInfo(){
        String  execId = "22";
        logger.info(azkabanAdapter.executionInfo(execId));
    }

    @Test
    public void scheduleFlow() {
        String flowName = "azkaban-training";
        String cronExpression = "0 23/30 5,7-10 ? * 6#3";

        try {
            azkabanAdapter.scheduleFlow(projectName, flowName, cronExpression);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}