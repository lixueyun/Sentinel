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
package com.alibaba.csp.sentinel.dashboard.rule.zookeeper.authority;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.rule.AuthorityRuleEntity;
import com.alibaba.csp.sentinel.dashboard.rule.DynamicRuleProvider;
import com.alibaba.csp.sentinel.dashboard.rule.zookeeper.ZookeeperConfigUtil;
import com.alibaba.csp.sentinel.datasource.Converter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("authorityRuleZookeeperProvider")
public class AuthorityRuleZookeeperProvider implements DynamicRuleProvider<List<AuthorityRuleEntity>> {

    @Autowired
    private CuratorFramework zkClient;
    @Autowired
    private Converter<String, List<AuthorityRuleEntity>> converter;

    @Value("${datasource.authority.dataId}")
    private String dataId;

    @Override
    public List<AuthorityRuleEntity> getRules(String appName) throws Exception {
        String zkPath = ZookeeperConfigUtil.getPath(appName, dataId);
        byte[] bytes;
        try {
            bytes = zkClient.getData().forPath(zkPath);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
        if (null == bytes || bytes.length == 0) {
            return new ArrayList<>();
        }
        String s = new String(bytes);

        return converter.convert(s);
    }
}