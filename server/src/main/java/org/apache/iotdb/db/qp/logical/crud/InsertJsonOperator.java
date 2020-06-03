/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.qp.logical.crud;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;

/**
 * this class extends {@code RootOperator} and process insert statement.
 */
public class InsertJsonOperator extends SFWOperator {

  private long time;
  private String deviceId;
  private JSONObject value;

  public InsertJsonOperator(long time, String deviceId, String jsonString) {
    this(time, deviceId, JSONObject.parseObject(StringEscapeUtils.unescapeJava(jsonString.substring(1, jsonString.length()-1))));
  }

  public InsertJsonOperator(long time, String deviceId, JSONObject value) {
    this(SQLConstant.TOK_INSERT);
    this.time = time;
    this.deviceId = deviceId;
    this.value = value;
  }

  public InsertJsonOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.INSERT;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public JSONObject getValue() {
    return value;
  }

  public void setValue(JSONObject value) {
    this.value = value;
  }
}
