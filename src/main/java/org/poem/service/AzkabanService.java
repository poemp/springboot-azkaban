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

    public void scheduleFlow(String projectName, String flowName, String cronExpression) throws IOException {
        azkabanAdapter.scheduleFlow(projectName, flowName, cronExpression);
    }
}
