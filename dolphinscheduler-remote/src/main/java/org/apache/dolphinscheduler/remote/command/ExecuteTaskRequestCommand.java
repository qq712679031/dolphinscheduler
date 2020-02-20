/* * Licensed to the Apache Software Foundation (ASF) under one or more * contributor license agreements.  See the NOTICE file distributed with * this work for additional information regarding copyright ownership. * The ASF licenses this file to You under the Apache License, Version 2.0 * (the "License"); you may not use this file except in compliance with * the License.  You may obtain a copy of the License at * *    http://www.apache.org/licenses/LICENSE-2.0 * * Unless required by applicable law or agreed to in writing, software * distributed under the License is distributed on an "AS IS" BASIS, * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. * See the License for the specific language governing permissions and * limitations under the License. */package org.apache.dolphinscheduler.remote.command;import org.apache.dolphinscheduler.remote.utils.FastJsonSerializer;import java.io.Serializable;/** *  execute task request command */public class ExecuteTaskRequestCommand implements Serializable {    private String taskInstanceJson;    public String getTaskInstanceJson() {        return taskInstanceJson;    }    public void setTaskInstanceJson(String taskInstanceJson) {        this.taskInstanceJson = taskInstanceJson;    }    public ExecuteTaskRequestCommand() {    }    public ExecuteTaskRequestCommand(String taskInstanceJson) {        this.taskInstanceJson = taskInstanceJson;    }    /**     *  package request command     *     * @return command     */    public Command convert2Command(){        Command command = new Command();        command.setType(CommandType.EXECUTE_TASK_REQUEST);        byte[] body = FastJsonSerializer.serialize(this);        command.setBody(body);        return command;    }}