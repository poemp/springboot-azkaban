package org.poem.azkaban;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

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
        String path = "C:\\Users\\Administrator\\Desktop\\job_test.zip";
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
    @Test
    public void startFlow(){
        try {
            String  execId = azkabanAdapter.startFlow(projectName,"job_test");
            System.err.println(execId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void executionInfo(){
        String  execId = "1";
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