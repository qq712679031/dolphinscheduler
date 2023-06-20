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

package org.apache.dolphinscheduler.plugin.datasource.impala.param;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

@AutoService(DataSourceProcessor.class)
public class ImpalaDataSourceProcessor extends AbstractDataSourceProcessor {

    private final Logger logger = LoggerFactory.getLogger(ImpalaDataSourceProcessor.class);

    private static final String APPEND_PARAMS = "AuthMech=0";

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, ImpalaDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        ImpalaConnectionParam connectionParams = (ImpalaConnectionParam) createConnectionParams(connectionJson);
        ImpalaDataSourceParamDTO mysqlDatasourceParamDTO = new ImpalaDataSourceParamDTO();

        mysqlDatasourceParamDTO.setUserName(connectionParams.getUser());
        mysqlDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        mysqlDatasourceParamDTO.setOther(parseOther(connectionParams.getOther()));
        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);
        mysqlDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        mysqlDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);

        return mysqlDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        ImpalaDataSourceParamDTO mysqlDatasourceParam = (ImpalaDataSourceParamDTO) dataSourceParam;
        String address = String.format("%s%s:%s", DataSourceConstants.JDBC_IMPALA, mysqlDatasourceParam.getHost(),
                mysqlDatasourceParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, mysqlDatasourceParam.getDatabase());
        ImpalaConnectionParam impalaConnectionParam = new ImpalaConnectionParam();
        impalaConnectionParam.setJdbcUrl(jdbcUrl);
        impalaConnectionParam.setDatabase(mysqlDatasourceParam.getDatabase());
        impalaConnectionParam.setAddress(address);
        impalaConnectionParam.setUser(mysqlDatasourceParam.getUserName());
        impalaConnectionParam.setPassword(PasswordUtils.encodePassword(mysqlDatasourceParam.getPassword()));
        impalaConnectionParam.setDriverClassName(getDatasourceDriver());
        impalaConnectionParam.setValidationQuery(getValidationQuery());
        impalaConnectionParam.setOther(transformOther(mysqlDatasourceParam.getOther()));
        impalaConnectionParam.setProps(mysqlDatasourceParam.getOther());

        return impalaConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, ImpalaConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_IMPALA_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.IMPALA_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        ImpalaConnectionParam impalaConnectionParam = (ImpalaConnectionParam) connectionParam;
        String jdbcUrl = impalaConnectionParam.getJdbcUrl();
        if (!StringUtils.isEmpty(impalaConnectionParam.getOther())) {
            return String.format("%s;%s", jdbcUrl, impalaConnectionParam.getOther());
        }
        return String.format("%s;%s", jdbcUrl, APPEND_PARAMS);
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        Class.forName(getDatasourceDriver());
        return DriverManager.getConnection(getJdbcUrl(connectionParam));
    }

    @Override
    public DbType getDbType() {
        return DbType.IMPALA;
    }

    @Override
    public DataSourceProcessor create() {
        return new ImpalaDataSourceProcessor();
    }

    private String transformOther(Map<String, String> paramMap) {
        if (MapUtils.isEmpty(paramMap)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        paramMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s;", key, value)));
        return stringBuilder.toString();
    }

    private Map<String, String> parseOther(String other) {
        if (StringUtils.isEmpty(other)) {
            return null;
        }
        Map<String, String> otherMap = new LinkedHashMap<>();
        for (String config : other.split(";")) {
            otherMap.put(config.split("=")[0], config.split("=")[1]);
        }
        return otherMap;
    }

}
