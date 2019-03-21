package org.poem.advice;

import org.poem.azkaban.AzkabanAdapter;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author Yorke
 */
@Slf4j
@Aspect
@Component
public class AzkabanAdvice {

    @Autowired
    private AzkabanAdapter azkabanAdapter;

    @Pointcut("execution(* org.poem.azkaban.AzkabanAdapter.*(..)))")
    public void azkabanPointcut(){}

    @Before("azkabanPointcut()")
    public void login(JoinPoint joinPoint) throws IOException {
        if (!"login".equals(joinPoint.getSignature().getName())) {
            azkabanAdapter.login();
        }
    }
}
