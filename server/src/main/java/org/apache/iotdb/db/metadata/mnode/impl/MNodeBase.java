/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.iotdb.db.metadata.mnode.impl;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.mnode.MNode;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public abstract class MNodeBase implements MNode {

  private static final long serialVersionUID = -770028375899514063L;

  /**
   * Name of the MNode
   */
  protected String name;

  protected MNode parent;

  /**
   * from root to this node, only be set when used once for InternalMNode
   */
  protected String fullPath;


  /**
   * Constructor of MNode.
   */
  public MNodeBase(MNode parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  /**
   * get full path
   */
  @Override
  public String getFullPath() {
    if (fullPath != null) {
      return fullPath;
    }
    fullPath = concatFullPath();
    return fullPath;
  }

  String concatFullPath() {
    StringBuilder builder = new StringBuilder(name);
    MNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.getName());
    }
    return builder.toString();
  }

  @Override
  public String toString() {
    return this.getName();
  }

  @Override
  public MNode getParent() {
    return parent;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }
}
