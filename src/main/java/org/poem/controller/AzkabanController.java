package org.poem.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/azkaban")
@Api(tags = {
        "01-任务管理"
})
public class AzkabanController {
    /**
     * 
     * @return
     */
    @ApiOperation(value = "1_测试接口", notes = "可用记录")
    @ApiResponses({@ApiResponse(code = 400, message = "请求参数没有填好"), @ApiResponse(code = 404, message = "请求路径没有找到")})
    @PostMapping(value = "/get")
    public String availability(){
        return "Hello Docker Azkaban";
    }
}
