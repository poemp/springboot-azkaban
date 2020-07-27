package org.poem.azkaban;

import com.alibaba.fastjson.JSONObject;
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
        azkabanAdapter.fetchProjectFlows(projectName);
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
            String  execId = azkabanAdapter.startFlow(projectName,"DataCleart");
            System.err.println(execId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 开始执行
     */
    @Test
    public void executeFLow(){
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
            optionalParams.put("json",JSONObject.toJSONString(clearDate));
            String  execId = azkabanAdapter.executeFLow(projectName,"DataCleart", optionalParams);
            System.err.println(execId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取执行id
     */
    @Test
    public void executionInfo(){
        String  execId = "5";
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