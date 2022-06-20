// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.NotifyUpdateStoragePolicyTask;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Policy resource for olap table
 *
 * Syntax:
 * CREATE RESOURCE "storage_policy_name"
 * PROPERTIES(
 *      "type"="storage_policy",
 *      "cooldown_datetime" = "2022-06-01", // time when data is transfter to medium
 *      "cooldown_ttl" = "3600"， // data is transfter to medium after 1 hour
 *      "s3_*"
 * );
 */
public class StoragePolicyResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(StoragePolicyResource.class);

    // optional
    public static final String COOLDOWN_DATETIME = "cooldown_datetime";
    public static final String COOLDOWN_TTL = "cooldown_ttl";
    public static final String S3_MAXCONN = "s3_max_connections";
    public static final String S3_REQUESTTIMEOUTMS = "s3_request_timeout_ms";
    public static final String S3_CONNTIMEOUTMS = "s3_connection_timeout_ms";

    private static final String DEFAULT_COOLDOWN_DATETIME = "9999-01-01 00:00:00";
    private static final String DEFAULT_COOLDOWN_TTL = "3600";
    private static final String DEFAULT_S3_MAXCONN = "50";
    private static final String DEFAULT_S3_REQUESTTIMEOUTMS = "3000";
    private static final String DEFAULT_S3_CONNTIMEOUTMS = "1000";

    // required
    public static final String S3_ENDPOINT = "s3_endpoint";
    public static final String S3_REGION = "s3_region";
    public static final String S3_ROOTPATH = "s3_rootpath";
    public static final String S3_AK = "s3_access_key";
    public static final String S3_SK = "s3_secret_key";
    public static final String S3_BUCKET = "s3_bucket";

    public static final String MD5_CHECKSUM = "md5_checksum";

    public static StoragePolicyResource DEFAULT_STORAGE_POLICY_PROPERTY
        = new StoragePolicyResource(Config.default_storage_policy);

    public static void setDefaultStoragePolicyProperties(Map<String, String> properties) {
        DEFAULT_STORAGE_POLICY_PROPERTY.properties = properties;
    }

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public StoragePolicyResource(String name) {
        this(name, Maps.newHashMap());
    }

    public StoragePolicyResource(String name, Map<String, String> properties) {
        super(name, ResourceType.STORAGE_POLICY);
        this.properties = properties;
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        this.properties = properties;
        // check properties
        // required
        checkRequiredProperty(S3_ENDPOINT);
        checkRequiredProperty(S3_REGION);
        checkRequiredProperty(S3_ROOTPATH);
        checkRequiredProperty(S3_AK);
        checkRequiredProperty(S3_SK);
        checkRequiredProperty(S3_BUCKET);
        // optional
        checkOptionalProperty(COOLDOWN_DATETIME, DEFAULT_COOLDOWN_DATETIME);
        checkOptionalProperty(COOLDOWN_TTL, DEFAULT_COOLDOWN_TTL);
        checkOptionalProperty(S3_MAXCONN, DEFAULT_S3_MAXCONN);
        checkOptionalProperty(S3_REQUESTTIMEOUTMS, DEFAULT_S3_REQUESTTIMEOUTMS);
        checkOptionalProperty(S3_CONNTIMEOUTMS, DEFAULT_S3_CONNTIMEOUTMS);

        this.properties.put(MD5_CHECKSUM, calcPropertiesMd5());
    }

    private void checkRequiredProperty(String propertyKey) throws DdlException {
        String value = properties.get(propertyKey);

        if (Strings.isNullOrEmpty(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    private void checkOptionalProperty(String propertyKey, String defaultValue) {
        this.properties.putIfAbsent(propertyKey, defaultValue);
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        // modify properties
        replaceIfEffectiveValue(this.properties, COOLDOWN_DATETIME, properties.get(COOLDOWN_DATETIME));
        replaceIfEffectiveValue(this.properties, COOLDOWN_TTL, properties.get(COOLDOWN_TTL));
        replaceIfEffectiveValue(this.properties, S3_MAXCONN, properties.get(S3_MAXCONN));
        replaceIfEffectiveValue(this.properties, S3_REQUESTTIMEOUTMS, properties.get(S3_REQUESTTIMEOUTMS));
        replaceIfEffectiveValue(this.properties, S3_CONNTIMEOUTMS, properties.get(S3_CONNTIMEOUTMS));
        replaceIfEffectiveValue(this.properties, S3_AK, properties.get(S3_AK));
        replaceIfEffectiveValue(this.properties, S3_SK, properties.get(S3_SK));

        if (Config.use_default_storage_policy && name.equalsIgnoreCase(Config.default_storage_policy)) {
            replaceIfEffectiveValue(this.properties, S3_ENDPOINT, properties.get(S3_ENDPOINT));
            replaceIfEffectiveValue(this.properties, S3_REGION, properties.get(S3_REGION));
            replaceIfEffectiveValue(this.properties, S3_ROOTPATH, properties.get(S3_ROOTPATH));
            replaceIfEffectiveValue(this.properties, S3_BUCKET, properties.get(S3_BUCKET));
            checkDefaultPolicyPropertiesEnough();
            setDefaultStoragePolicyProperties(this.properties);
        }

        this.properties.put(MD5_CHECKSUM, calcPropertiesMd5());
        LOG.info("notifyUpdate properties: {}", this.properties);
        notifyUpdate();
    }

    public void checkDefaultPolicyPropertiesEnough() throws DdlException {
        //check required
        checkRequiredProperty(S3_ENDPOINT);
        checkRequiredProperty(S3_REGION);
        checkRequiredProperty(S3_ROOTPATH);
        checkRequiredProperty(S3_AK);
        checkRequiredProperty(S3_SK);
        checkRequiredProperty(S3_BUCKET);

        //check option, it not exist, use default
        checkOptionalProperty(COOLDOWN_DATETIME, DEFAULT_COOLDOWN_DATETIME);
        checkOptionalProperty(COOLDOWN_TTL, DEFAULT_COOLDOWN_TTL);
        checkOptionalProperty(S3_MAXCONN, DEFAULT_S3_MAXCONN);
        checkOptionalProperty(S3_REQUESTTIMEOUTMS, DEFAULT_S3_REQUESTTIMEOUTMS);
        checkOptionalProperty(S3_CONNTIMEOUTMS, DEFAULT_S3_CONNTIMEOUTMS);
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        // check properties
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        copiedProperties.remove(COOLDOWN_DATETIME);
        copiedProperties.remove(COOLDOWN_TTL);
        copiedProperties.remove(S3_MAXCONN);
        copiedProperties.remove(S3_REQUESTTIMEOUTMS);
        copiedProperties.remove(S3_CONNTIMEOUTMS);
        copiedProperties.remove(S3_ENDPOINT);
        copiedProperties.remove(S3_REGION);
        copiedProperties.remove(S3_ROOTPATH);
        copiedProperties.remove(S3_AK);
        copiedProperties.remove(S3_SK);
        copiedProperties.remove(S3_BUCKET);

        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown S3 resource properties: " + copiedProperties);
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(S3_SK)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
                continue;
            }
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }

    private void notifyUpdate() {
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        AgentBatchTask batchTask = new AgentBatchTask();

        for (Long beId : systemInfoService.getBackendIds(true)) {
            LOG.info("notifyUpdate be: {}", beId);
            NotifyUpdateStoragePolicyTask createReplicaTask = new NotifyUpdateStoragePolicyTask(beId, name, properties);
            batchTask.addTask(createReplicaTask);
        }

        AgentTaskExecutor.submit(batchTask);
    }

    private String calcPropertiesMd5(){
        List<String> calcKey = Arrays.asList(COOLDOWN_DATETIME, COOLDOWN_TTL, S3_MAXCONN, S3_REQUESTTIMEOUTMS,
            S3_CONNTIMEOUTMS, S3_AK, S3_SK);
        return DigestUtils.md5Hex(calcKey.stream().map(iter -> "(" + iter + ":" + properties.get(iter) + ")").reduce("", String::concat));
    }
}
