package org.poem.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class AzkabanServiceTest {

    @Autowired
    private AzkabanService azkabanService;

    @Test
    public void scheduleFlow() {
        String projectName = "wtwt";
        String flowName = "azkaban-training";
        String cronExpression = "0 23/30 5,7-10 ? * 6#3";

        try {
            azkabanService.scheduleFlow(projectName, flowName, cronExpression);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}