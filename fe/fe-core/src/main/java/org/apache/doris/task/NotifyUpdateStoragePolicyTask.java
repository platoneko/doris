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

package org.apache.doris.task;

import org.apache.doris.thrift.TGetStoragePolicy;
import org.apache.doris.thrift.TS3StorageParam;
import org.apache.doris.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static org.apache.doris.catalog.StoragePolicyResource.COOLDOWN_DATETIME;
import static org.apache.doris.catalog.StoragePolicyResource.COOLDOWN_TTL;
import static org.apache.doris.catalog.StoragePolicyResource.S3_MAXCONN;
import static org.apache.doris.catalog.StoragePolicyResource.S3_REQUESTTIMEOUTMS;
import static org.apache.doris.catalog.StoragePolicyResource.S3_CONNTIMEOUTMS;
import static org.apache.doris.catalog.StoragePolicyResource.S3_ENDPOINT;
import static org.apache.doris.catalog.StoragePolicyResource.S3_REGION;
import static org.apache.doris.catalog.StoragePolicyResource.S3_ROOTPATH;
import static org.apache.doris.catalog.StoragePolicyResource.S3_AK;
import static org.apache.doris.catalog.StoragePolicyResource.S3_SK;
import static org.apache.doris.catalog.StoragePolicyResource.MD5_CHECKSUM;
import static org.apache.doris.catalog.StoragePolicyResource.S3_BUCKET;

public class NotifyUpdateStoragePolicyTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(NotifyUpdateStoragePolicyTask.class);
    private String policyName;

    private String md5CheckSum;
    private Map<String, String> properties;

    public NotifyUpdateStoragePolicyTask(long backendId, String name, Map<String, String> properties) {
        super(null, backendId, TTaskType.NOTIFY_UPDATE_STORAGE_POLICY, -1, -1, -1, -1, -1, -1, -1);
        this.policyName = name;
        this.properties = properties;
    }

    public TGetStoragePolicy toThrift() {
        TGetStoragePolicy ret = new TGetStoragePolicy();

        ret.policy_name = policyName;
        // 9999-01-01 00:00:00 => unix timestamp
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = dateFormat.parse(properties.get(COOLDOWN_DATETIME));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        LOG.info("toThrift map {}", properties);
        long secondTimestamp = date.getTime() / 1000;
        ret.cooldown_datetime = secondTimestamp;
        ret.cooldown_ttl = Long.parseLong(properties.get(COOLDOWN_TTL));
        ret.s3_storage_param = new TS3StorageParam();
        ret.s3_storage_param.s3_max_conn = Integer.parseInt(properties.get(S3_MAXCONN));
        ret.s3_storage_param.s3_request_timeout_ms = Integer.parseInt(properties.get(S3_REQUESTTIMEOUTMS));
        ret.s3_storage_param.s3_conn_timeout_ms = Integer.parseInt(properties.get(S3_CONNTIMEOUTMS));
        ret.s3_storage_param.s3_endpoint = properties.get(S3_ENDPOINT);
        ret.s3_storage_param.s3_region = properties.get(S3_REGION);
        ret.s3_storage_param.root_path = properties.get(S3_ROOTPATH);
        ret.s3_storage_param.s3_ak = properties.get(S3_AK);
        ret.s3_storage_param.s3_sk = properties.get(S3_SK);
        ret.s3_storage_param.bucket = properties.get(S3_BUCKET);
        ret.md5_checksum = properties.get(MD5_CHECKSUM);

        return ret;
    }
}
