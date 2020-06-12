/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class CreateTimeSeriesPlan extends PhysicalPlan {

    protected Path path;
    protected String alias;
    protected Map<String, String> props;
    protected Map<String, String> tags;
    protected Map<String, String> attributes;

    public CreateTimeSeriesPlan(boolean isQuery, Operator.OperatorType operatorType) {
        super(isQuery, operatorType);
    }

    public Path getPath() {
      return path;
    }

    public void setPath(Path path) {
      this.path = path;
    }

    @Override
    public List<Path> getPaths() {
        return Collections.singletonList(path);
    }

    public Map<String, String> getAttributes() {
      return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
      this.attributes = attributes;
    }

    public String getAlias() {
      return alias;
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public void setTags(Map<String, String> tags) {
      this.tags = tags;
    }

    public Map<String, String> getProps() {
      return props;
    }

    public void setProps(Map<String, String> props) {
      this.props = props;
    }
}
