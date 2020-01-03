/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.dashboard.rule.zookeeper;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.rule.*;
import com.alibaba.csp.sentinel.dashboard.util.MapUtils;
import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
public class ZookeeperConfig {

    @Value("${datasource.zk.serverAddr}")
    private String connectString;

    @Bean
    public Converter<List<FlowRuleEntity>, String> flowRuleEntityEncoder() {
        return JSON::toJSONString;
    }

    @Bean
    public Converter<String, List<FlowRuleEntity>> flowRuleEntityDecoder() {
        return s -> JSON.parseArray(s, FlowRuleEntity.class);
    }

    @Bean
    public Converter<List<AuthorityRuleEntity>, String> authorityRuleEntityEncoder() {

        return (rules ->
                JSON.toJSONString(
                        rules.stream()
                                .map(rule -> {

                                    HashMap<String, Object> paramMap = new HashMap<>();
                                    Map<String, Object> beanMap = null;
                                    try {
                                        beanMap = MapUtils.objectToMap(rule.getRule());
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    paramMap.putAll(beanMap);
                                    paramMap.put("app", rule.getApp());
                                    paramMap.put("gmtCreate", rule.getGmtCreate());
                                    paramMap.put("gmtModified", rule.getGmtModified());
                                    paramMap.put("id", rule.getId());
                                    paramMap.put("ip", rule.getIp());
                                    paramMap.put("port", rule.getPort());
                                    return paramMap;
                                })
                                .collect(Collectors.toList())
                )
        );

    }

    @Bean
    public Converter<String, List<AuthorityRuleEntity>> authorityRuleEntityDecoder() {
        return (s ->
                JSON.parseArray(s, HashMap.class).stream()
                        .map(e -> {
                            String app = (String) e.get("app");
                            String ip = (String) e.get("ip");
                            Integer port = (Integer) e.get("port");
                            AuthorityRule authorityRule = JSON.parseObject(JSON.toJSONString(e), AuthorityRule.class);
                            return AuthorityRuleEntity.fromAuthorityRule(app, ip, port, authorityRule);
                        })
                        .collect(Collectors.toList())
        );
    }

    @Bean
    public Converter<List<DegradeRuleEntity>, String> degradeRuleEntityEncoder() {
        return JSON::toJSONString;
    }

    @Bean
    public Converter<String, List<DegradeRuleEntity>> degradeRuleEntityDecoder() {
        return s -> JSON.parseArray(s, DegradeRuleEntity.class);
    }

    @Bean
    public Converter<List<ParamFlowRuleEntity>, String> paramFlowRuleEntityEncoder() {
        return (rules ->
                JSON.toJSONString(
                        rules.stream()
                                .map(rule -> {

                                    HashMap<String, Object> paramMap = new HashMap<>();
                                    Map<String, Object> beanMap = null;
                                    try {
                                        beanMap = MapUtils.objectToMap(rule.getRule());
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    paramMap.putAll(beanMap);
                                    paramMap.put("app", rule.getApp());
                                    paramMap.put("gmtCreate", rule.getGmtCreate());
                                    paramMap.put("gmtModified", rule.getGmtModified());
                                    paramMap.put("id", rule.getId());
                                    paramMap.put("ip", rule.getIp());
                                    paramMap.put("port", rule.getPort());
                                    return paramMap;
                                })
                                .collect(Collectors.toList())
                )
        );
    }

    @Bean
    public Converter<String, List<ParamFlowRuleEntity>> paramFlowRuleEntityDecoder() {

        return (s ->
            JSON.parseArray(s, HashMap.class).stream()
                    .map(e -> {
                        String app = (String) e.get("app");
                        String ip = (String) e.get("ip");
                        Integer port = (Integer) e.get("port");
                        ParamFlowRule paramFlowRule = JSON.parseObject(JSON.toJSONString(e), ParamFlowRule.class);
                        return ParamFlowRuleEntity.fromAuthorityRule(app, ip, port, paramFlowRule);
                    })
                    .collect(Collectors.toList())
        );
    }

    @Bean
    public Converter<List<SystemRuleEntity>, String> systemRuleEntityEncoder() {
        return JSON::toJSONString;
    }

    @Bean
    public Converter<String, List<SystemRuleEntity>> systemRuleEntityDecoder() {
        return s -> JSON.parseArray(s, SystemRuleEntity.class);
    }



    @Bean
    public CuratorFramework zkClient() {
        CuratorFramework zkClient =
                CuratorFrameworkFactory.newClient(connectString,
                        new ExponentialBackoffRetry(ZookeeperConfigUtil.SLEEP_TIME, ZookeeperConfigUtil.RETRY_TIMES));
        zkClient.start();

        return zkClient;
    }
}