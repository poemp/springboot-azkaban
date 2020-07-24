package org.poem.service;

import org.poem.azkaban.AzkabanAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author Yorke
 */
@Service
public class AzkabanService {

    @Autowired
    private AzkabanAdapter azkabanAdapter;

    /**
     * 获取一个调度器job的信息 根据project的id 和 flowId
     * Flexible scheduling using Cron
     * 通过cron表达式调度执行 创建调度任务
     *
     * @param projectName    The name of the project
     * @param flowName       The name of the flow
     * @param cronExpression A CRON expression is a string comprising 6 or 7 fields separated by white space that represents a set of times
     */
    public void scheduleFlow(String projectName, String flowName, String cronExpression) throws IOException {
        azkabanAdapter.scheduleFlow(projectName, flowName, cronExpression);
    }
}
