/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.exception.metadata.DeleteFailedException;

import java.io.Serializable;
import java.util.Map;

public interface MNode extends Serializable {
    /**
     * check whether the MNode has a child with the name
     */
    boolean hasChild(String name);

    /**
     * node key, name or alias
     */
    void addChild(String name, MNode child);

    /**
     * delete a child
     */
    void deleteChild(String name) throws DeleteFailedException;

    /**
     * delete the alias of a child
     */
    void deleteAliasChild(String alias) throws DeleteFailedException;

    /**
     * get the child with the name
     */
    MNode getChild(String name);

    /**
     * get the count of all leaves whose ancestor is current node
     */
    int getLeafCount();

    /**
     * add an alias
     */
    void addAlias(String alias, MNode child);

    /**
     * get full path
     */
    String getFullPath();

    @Override
    String toString();

    MNode getParent();

    Map<String, MNode> getChildren();

    String getName();

    void setName(String name);
}
