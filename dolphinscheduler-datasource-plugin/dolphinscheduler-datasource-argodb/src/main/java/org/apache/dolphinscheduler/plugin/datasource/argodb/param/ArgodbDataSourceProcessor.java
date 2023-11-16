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

package org.apache.dolphinscheduler.plugin.datasource.argodb.param;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.auto.service.AutoService;

@AutoService(DataSourceProcessor.class)
public class ArgodbDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, ArgodbDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        ArgodbDataSourceParamDTO argodbDataSourceParamDTO = new ArgodbDataSourceParamDTO();
        ArgodbConnectionParam argodbConnectionParam = (ArgodbConnectionParam) createConnectionParams(connectionJson);

        argodbDataSourceParamDTO.setDatabase(argodbConnectionParam.getDatabase());
        argodbDataSourceParamDTO.setUserName(argodbConnectionParam.getUser());
        argodbDataSourceParamDTO.setOther(parseOther(argodbConnectionParam.getOther()));

        String[] tmpArray = argodbConnectionParam.getAddress().split(Constants.DOUBLE_SLASH);
        StringBuilder hosts = new StringBuilder();
        String[] hostPortArray = tmpArray[tmpArray.length - 1].split(Constants.COMMA);
        for (String hostPort : hostPortArray) {
            hosts.append(hostPort.split(Constants.COLON)[0]).append(Constants.COMMA);
        }
        hosts.deleteCharAt(hosts.length() - 1);
        argodbDataSourceParamDTO.setHost(hosts.toString());
        argodbDataSourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        return argodbDataSourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        ArgodbDataSourceParamDTO argodbParam = (ArgodbDataSourceParamDTO) datasourceParam;
        String address = String.format("%s%s:%s", DataSourceConstants.JDBC_ARGODB, argodbParam.getHost(), argodbParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, argodbParam.getDatabase());
        ArgodbConnectionParam argodbConnectionParam = new ArgodbConnectionParam();
        argodbConnectionParam.setDatabase(argodbParam.getDatabase());
        argodbConnectionParam.setAddress(address.toString());
        argodbConnectionParam.setJdbcUrl(jdbcUrl);
        argodbConnectionParam.setUser(argodbParam.getUserName());
        argodbConnectionParam.setPassword(PasswordUtils.encodePassword(argodbParam.getPassword()));
        argodbConnectionParam.setDriverClassName(getDatasourceDriver());
        argodbConnectionParam.setValidationQuery(getValidationQuery());
        argodbConnectionParam.setOther(transformOther(argodbParam.getOther()));
        argodbConnectionParam.setProps(argodbParam.getOther());
        return argodbConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, ArgodbConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_ARGODB_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.ARGODB_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        ArgodbConnectionParam hiveConnectionParam = (ArgodbConnectionParam) connectionParam;
        String jdbcUrl = hiveConnectionParam.getJdbcUrl();
        String otherParams = filterOther(hiveConnectionParam.getOther());
        if (StringUtils.isNotEmpty(otherParams) && !"?".equals(otherParams.substring(0, 1))) {
            jdbcUrl += ";";
        }
        return jdbcUrl + otherParams;
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        Class.forName(getDatasourceDriver());
        return DriverManager.getConnection(getJdbcUrl(connectionParam));
    }

    @Override
    public DbType getDbType() {
        return DbType.ARGODB;
    }

    @Override
    public DataSourceProcessor create() {
        return new ArgodbDataSourceProcessor();
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        otherMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s;", key, value)));
        return stringBuilder.toString();
    }

    private String filterOther(String otherParams) {
        if (StringUtils.isBlank(otherParams)) {
            return "";
        }

        StringBuilder hiveConfListSb = new StringBuilder();
        hiveConfListSb.append("?");
        StringBuilder sessionVarListSb = new StringBuilder();

        String[] otherArray = otherParams.split(";", -1);

        for (String conf : otherArray) {
            sessionVarListSb.append(conf).append(";");
        }

        // remove the last ";"
        if (sessionVarListSb.length() > 0) {
            sessionVarListSb.deleteCharAt(sessionVarListSb.length() - 1);
        }

        if (hiveConfListSb.length() > 0) {
            hiveConfListSb.deleteCharAt(hiveConfListSb.length() - 1);
        }

        return sessionVarListSb.toString() + hiveConfListSb.toString();
    }

    private Map<String, String> parseOther(String other) {
        if (other == null) {
            return null;
        }
        Map<String, String> otherMap = new LinkedHashMap<>();
        String[] configs = other.split(";");
        for (String config : configs) {
            otherMap.put(config.split("=")[0], config.split("=")[1]);
        }
        return otherMap;
    }
}
