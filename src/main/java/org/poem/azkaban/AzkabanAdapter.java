package org.poem.azkaban;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.poem.config.AzkabanConfiguration;
import org.poem.exception.AzkabanException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Administrator
 */
@Component
public class AzkabanAdapter {

    private static final Logger log = LoggerFactory.getLogger(AzkabanAdapter.class);

    private static String SESSION_ID;
    @Autowired
    private AzkabanConfiguration config;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private RestTemplate restTemplate;

    /**
     * 登录
     */
    public void login() {
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("action", "login");
        params.add("username", config.getUsername());
        params.add("password", config.getPassword());

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl(), httpEntity, String.class);
        log.info("Result: " + respResult);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                SESSION_ID = respRoot.get("session.id").asText();
                log.info("Azkaban login success:{}", respRoot);
            } else {
                log.warn("Azkaban login failure:{}", respRoot);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban login failure: %s !", e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 创建项目
     *
     * @param projectName 项目名称
     * @param description 项目描述
     */
    public void createProject(String projectName, String description) {
        LinkedMultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("session.id", SESSION_ID);
        parameters.add("action", "create");
        parameters.add("name", projectName);
        parameters.add("description", description);

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(parameters, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl() + "/manager", httpEntity, String.class);
        log.info("Result: " + respResult);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                log.info("Azcaban create a Project: {}", projectName);
            } else {
                String errorMessage = respRoot.hasNonNull("message") ? respRoot.get("message").asText() : "No message.";
                log.error("Azcaban create Project {} failure: {}}", projectName, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azcaban create Project %s failure: %s", projectName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 删除项目
     *
     * @param projectName 项目名称
     * @return 删除结果
     */
    public void deleteProject(String projectName) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");
        Map<String, Object> params = new HashMap<>();
        params.put("id", SESSION_ID);
        params.put("project", projectName);

        restTemplate.getForObject(config.getUrl() + "/manager?session.id={id}&delete=true&project={project}", String.class, params);
        log.info("Azkaban delete project: {}", projectName);
    }

    /**
     * 上传zip 上传依赖文件 zip包
     *
     * @param zipFilePath zip路径
     * @param projectName 项目名称
     * @return 结果
     */
    public void uploadZip(String zipFilePath, String projectName) throws Exception {
        FileSystemResource file = new FileSystemResource(new File(zipFilePath));
        LinkedMultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "upload");
        params.add("project", projectName);
        params.add("file", file);

        String respResult = restTemplate.postForObject(config.getUrl() + "/manager", params, String.class);
        log.info("Result: " + respResult);
        JsonNode respRoot = null;
        try {
            respRoot = objectMapper.readTree(respResult);
            if (!respRoot.hasNonNull("error")) {
                log.info("Azcaban Upload a Project Zip to {}: {}", projectName, zipFilePath);
            } else {
                log.error(String.format("Azcaban upload Project Zip to %s failure: %s", projectName, respRoot.get("error").asText()));
                throw new AzkabanException(respRoot.get("error").asText());
            }
        } catch (IOException e) {
            log.error(String.format("Azcaban upload Project Zip to %s failure: %s", projectName, e.getMessage()));
        }
    }

    /**
     * 获取一个project的流ID
     *
     * @param projectName 项目名称
     * @return 结果
     */
    public JsonNode fetchProjectFlows(String projectName) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");

        Map<String, String> params = new HashMap<>();
        params.put("id", SESSION_ID);
        params.put("project", projectName);

        String respResult = restTemplate.getForObject(config.getUrl() + "/manager?session.id={id}&ajax=fetchprojectflows&project={project}", String.class, params);
        log.info("Result: " + respResult);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (!respRoot.hasNonNull("error")) {
                log.info("Azkaban fetch flows of Project {}: {}", projectName, respResult);
                return respRoot;
            } else {
                String errorMessage = respRoot.get("error").asText();
                log.error(String.format("Azkaban fetch flows of Project {} failure: {}", projectName, errorMessage));
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban fetch of Project %s failure: %s", projectName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 获取一个job的流结构 依赖关系
     *
     * @param projectName
     * @param flowId
     * @return
     */
    public String fetchFlowJobs(String projectName, String flowId) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");
        Map<String, String> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("project", projectName);
        map.put("flow", flowId);

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/manager?session.id={id}&ajax=fetchflowgraph&project={project}&flow={flow}", HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class, map);

        log.info("Azkban fetch Jobs of a Flow:{}", exchange);
        return exchange.toString();
    }

    /**
     * @param projectName
     * @param flowId
     * @param start
     * @param length
     * @return
     */
    public String fetchFlowExecutions(String projectName, String flowId, int start, int length) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("project", projectName);
        map.put("flow", flowId);
        map.put("start", start);
        map.put("length", length);

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/manager?session.id={id}&ajax=fetchFlowExecutions&project={project}&flow={flow}&start={start}&length={length}", HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class, map);

        log.info("Azkban fetch Executions of a Flow:{}", exchange);
        return exchange.toString();
    }

    /**
     * 获取正在执行的流id
     *
     * @param projectName
     * @param flowId
     * @return
     */
    public String fetchFlowRunningExecutions(String projectName, String flowId) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("project", projectName);
        map.put("flow", flowId);

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/executor?session.id={id}&ajax=getRunning&project={project}&flow={flow}", HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class, map);

        log.info("Azkban fetch Running Executions of a Flow:{}", exchange);
        return exchange.toString();
    }

    /**
     * 执行
     *
     * @param projectName
     * @param flowId
     * @param optionalParams
     * @return
     */
    public String executeFLow(String projectName, String flowId, Map<String, Object> optionalParams) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8");
        Map<String, Object> map = new HashMap<>();
        if (optionalParams != null) {
            map.putAll(optionalParams);
        }
        map.put("session.id", SESSION_ID);
        map.put("ajax", "getRunning");
        map.put("project", projectName);
        map.put("flow", flowId);
        //flowOverride[type]=apple
        String paramStr = map.keySet().stream()
                .map(key -> "flowOverride[" + key + "]=" + map.get(key))
                .collect(Collectors.joining("&"));

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/executor?" + paramStr, HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class);

        log.info("Azkban execute a Flow:{}", exchange);
        return exchange.toString();
    }

    /**
     * 取消执行
     *
     * @param execId
     * @return
     */
    public String cancelFlowExecution(String execId) {
        HttpHeaders httpHeaders = getAzkabanHeaders();
        httpHeaders.add("Accept", "text/plain;charset=utf-8");
        Map<String, Object> map = new HashMap<>();
        map.put("id", SESSION_ID);
        map.put("execid", execId);

        ResponseEntity<String> exchange = restTemplate.exchange(config.getUrl() + "/executor?session.id={id}&ajax=cancelFlow&execid={execid}", HttpMethod.GET,
                new HttpEntity<String>(httpHeaders), String.class, map);

        log.info("Azkban cancel a Flow Execution:{}", exchange);
        return exchange.toString();
    }

    /**
     * Set a SLA 设置调度任务 执行的时候 或者执行成功失败等等的规则匹配 发邮件或者...
     * Schedule a period-based Flow
     *
     * @param projectName  The name of the project
     * @param flowName     The name of the flow
     * @param scheduleTime The time to schedule the flow. Example: 12,00,pm,PDT (Unless UTC is specified, Azkaban will take current server’s default timezone instead)
     * @param scheduleDate The date to schedule the flow. Example: 07/22/2014
     * @param period       Specifies the recursion period. Depends on the “is_recurring” flag being set. Example: 5w
     */
    public void schedulePeriodBasedFlow(String projectName, String flowName, String scheduleDate, String scheduleTime, String period) {
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "scheduleFlow");
        params.add("projectName", projectName);
        String projectId = Optional.ofNullable(fetchProjectFlows(projectName))
                .map(pn -> pn.get("projectId").asText())
                .orElseThrow(() -> new AzkabanException(String.format("项目 %s 不存在！", projectName)));
        params.add("projectId", projectId);
        params.add("flow", flowName);
        params.add("scheduleTime", scheduleTime);
        params.add("scheduleDate", scheduleDate);
        if (!StringUtils.isEmpty(period)) {
            // 是否循环
            params.add("is_recurring", "on");
            // 循环周期 天 年 月等
            // M Months
            // w Weeks
            // d Days
            // h Hours
            // m Minutes
            // s Seconds
            params.add("period", period);
        }

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);

        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                log.info("Azkaban schedule a period-based FLow: {}", respRoot.hasNonNull("message") ? respRoot.get("message").asText() : "No message.");
                if (respRoot.hasNonNull("error")) {
                    log.warn("Azkaban schedule period-base Flow error: {}", respRoot.get("error").asText());
                }
            } else {
                String errorMessage = respRoot.hasNonNull("message")
                        ? respRoot.get("message").asText()
                        : (
                        respRoot.hasNonNull("error")
                                ? respRoot.get("error").asText()
                                : "No message."
                );
                log.error("Azkaban schedule period-based FLow {} failure: {}", flowName, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban schedule period-based Flow %s failure: %s", flowName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 通过cron表达式调度执行 创建调度任务
     *
     * @param projectName
     * @param flowName
     * @param cronExpression
     */
    public void scheduleCronBasedFlow(String projectName, String flowName, String cronExpression) {
        LinkedMultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "scheduleCronFlow");
        params.add("projectName", projectName);
        params.add("flow", flowName);
        params.add("cronExpression", cronExpression);

        HttpEntity<LinkedMultiValueMap<String, Object>> httpEntity = new HttpEntity<>(params, getAzkabanHeaders());

        String respResult = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);
        log.info("Result: " + respResult);
        try {
            JsonNode respRoot = objectMapper.readTree(respResult);
            if (respRoot.hasNonNull("status") && "success".equals(respRoot.get("status").asText())) {
                log.info("Azkaban schedule a Cron Flow: {}", respRoot.hasNonNull("message") ? respRoot.get("message").asText() : "No message.");
                if (respRoot.hasNonNull("error")) {
                    log.warn("Azkaban schedule Cron Flow with error: {}", respRoot.get("error").asText());
                }
            } else {
                String errorMessage = respRoot.hasNonNull("message")
                        ? respRoot.get("message").asText()
                        : (respRoot.hasNonNull("error") ? respRoot.get("error").asText() : "No message.");
                log.error("Azkaban schedule Cron Flow {} failure: {}", flowName, errorMessage);
                throw new AzkabanException(errorMessage);
            }
        } catch (IOException e) {
            log.error(String.format("Azkaban schedule Cron Flow {} failure: {}", flowName, e.getMessage()), e);
            throw new AzkabanException(e.getMessage());
        }
    }

    /**
     * 获取一个调度器job的信息 根据project的id 和 flowId
     * Flexible scheduling using Cron
     * 通过cron表达式调度执行 创建调度任务
     *
     * @param projectName    The name of the project
     * @param flowName       The name of the flow
     * @param cronExpression A CRON expression is a string comprising 6 or 7 fields separated by white space that represents a set of times
     * @return Response data
     */
    public String scheduleFlow(String projectName, String flowName, String cronExpression) throws IOException {
        HttpHeaders httpHeaders = getAzkabanHeaders();

        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("session.id", SESSION_ID);
        params.add("ajax", "scheduleCronFlow");
        params.add("projectName", projectName);
        params.add("flow", flowName);
        params.add("cronExpression", cronExpression);

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(params, httpHeaders);

        String respData = restTemplate.postForObject(config.getUrl() + "/schedule", httpEntity, String.class);
        log.info("Result: " + respData);
        log.info("Azkaban flexible scheduling using Cron: {}", respData);

        return respData;
    }

    /**
     * 执行 flow
     *
     * @param projectName 项目名称
     * @param flowName    flow 名称
     * @return 执行 ID
     */
    public String startFlow(String projectName, String flowName, Map<String, Object> optionalParams) throws IOException {
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<String, Object>();
        linkedMultiValueMap.add("session.id", SESSION_ID);
        linkedMultiValueMap.add("ajax", "executeFlow");
        linkedMultiValueMap.add("project", projectName);
        linkedMultiValueMap.add("flow", flowName);
        for (String s : optionalParams.keySet()) {
            linkedMultiValueMap.add("flowOverride[" + s + "]", optionalParams.get(s));
        }

        String res = restTemplate.postForObject(config.getUrl() + "/executor", linkedMultiValueMap, String.class);
        log.info("azkaban start flow:{}", res);
        JsonNode objectNode = objectMapper.readTree(res);
        return objectNode.get("execid").asText();
    }

    /**
     * 执行信息
     *
     * @param execId 执行ID
     * @return 结果
     */
    public String executionInfo(String project, String execId) {
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", SESSION_ID);
        linkedMultiValueMap.add("ajax", "fetchexecflow");
        linkedMultiValueMap.add("execid", execId);
        linkedMultiValueMap.add("project",project);
        String res = restTemplate.postForObject(config.getUrl() + "/executor", linkedMultiValueMap, String.class);
        log.info("azkaban execution info:{}", res);
        return res;
    }

    /**
     * 获取一个执行流的日志
     *
     * @param execId 执行ID
     * @param jobId  job ID
     * @param offset 起始位置
     * @param length 长度
     * @return 结果
     */
    public String fetchExecutionJobLogs(String execId, String jobId, int offset, int length) {
        String res = restTemplate
                .getForObject(config.getUrl() + "/executor?ajax=fetchExecJobLogs&session.id={1}&execid={2}&jobId={3}&offset={4}&length={5}"
                        , String.class, SESSION_ID, execId, jobId, offset, length
                );
        log.info("azkban execution job logs:{}", res);
        return res;
    }

    /**
     * 查询 flow 执行情况
     *
     * @param execId 执行ID
     * @return 结果
     */
    public String fetchFlowExecution(String execId) {
        String res = restTemplate
                .getForObject(config.getUrl() + "/executor?ajax=fetchexecflow&session.id={1}&execid={2}"
                        , String.class, SESSION_ID, execId
                );
        log.info("azkban execution flow:{}", res);

        return res;
    }

    /**
     * 重新执行一个执行流
     *
     * @param execId
     * @return
     */
    public String fetchPauseFlow(String execId) {
        String res = restTemplate
                .getForObject(config.getUrl() + "/executor?ajax=pauseFlow&session.id={1}&execid={2}"
                        , String.class, SESSION_ID, execId
                );
        log.info("azkban execution flow:{}", res);
        return res;
    }

    /**
     * 重新执行一个执行流
     *
     * @param execId
     * @return
     */
    public String fetchResumeFlow(String execId) {
        String res = restTemplate
                .getForObject(config.getUrl() + "/executor?ajax=resumeFlow&session.id={1}&execid={2}"
                        , String.class, SESSION_ID, execId
                );
        log.info("azkban execution flow:{}", res);

        return res;
    }

    /**
     * hdeader
     *
     * @return
     */
    private HttpHeaders getAzkabanHeaders() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded; charset=utf-8");
        httpHeaders.add("X-Requested-With", "XMLHttpRequest");
        return httpHeaders;
    }
}