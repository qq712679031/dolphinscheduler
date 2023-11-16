/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.api.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @Description 国密Sm4 算法加密器
 * @date 2022/3/7 14:17
 * @Version 1.0
 * @Author gezongyang
 */
@Slf4j
@Component("sM4ECBStringEncryptor")
public class SM4ECBStringEncryptor implements SM4StringEncryptor {

    @Value("${encrypt.passwordEncrypt}")
    private Boolean passwordEncrypt;

    @Value("${encrypt.encryptKey}")
    private String path;
    private static String encryptKey;

    @Override
    public String encrypt(String msg) {
        if (passwordEncrypt) {
            try {
                String key = this.getKey(path);
                msg = SM4Util.decryptEcb(key, msg);
            } catch (Exception e) {
                log.error("配置信息加密失败,{}", e.getStackTrace());
            }
        }
        return msg;
    }

    @Override
    public String decrypt(String msg) {
        if (passwordEncrypt) {
            try {
                String key = this.getKey(path);
                log.info("key {}", key);
                msg = SM4Util.decryptEcb(key, msg);
            } catch (Exception e) {
                log.error("配置信息解密失败,{}", e.getStackTrace());
            }
        }
        return msg;
    }

    public static String getKey(String path) throws Exception {
        // 用于接收校验结果
        if (StringUtils.isEmpty(encryptKey)) {
            try {
                Properties prop = new Properties();
                prop.load(new FileInputStream(path));
                encryptKey = prop.toString().replace("{", "").replace("=}", "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return encryptKey;
    }

}
