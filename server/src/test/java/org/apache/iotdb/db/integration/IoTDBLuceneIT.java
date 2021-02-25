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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.HashVirtualPartitioner;
import org.apache.iotdb.db.index.LuceneIndex;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBLuceneIT {

  @Before
  public void setUp() {
    // test different partition
    HashVirtualPartitioner.getInstance().setStorageGroupNum(16);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    HashVirtualPartitioner.getInstance()
        .setStorageGroupNum(IoTDBDescriptor.getInstance().getConfig().getVirtualStorageGroupNum());
  }

  @Test
  public void testLuceneIndex() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("INSERT INTO root.vehicle.deviceid(timestamp,text) VALUES (100,'Hallo wie geht es dir?')");

      assertEquals(0, LuceneIndex.getInstance().getRegistered());

      statement.execute("FLUSH");

      assertEquals(1, LuceneIndex.getInstance().getRegistered());
      final ScoreDoc[] results = LuceneIndex.getInstance().search("Ha*");

      assertEquals(1, results.length);

      final ScoreDoc[] results2 = LuceneIndex.getInstance().search("Tschüss");

      assertEquals(0, results2.length);

      assertEquals(0, LuceneIndex.getInstance()
          .search("timestamp:[0 TO 50]").length);
      assertEquals(1, LuceneIndex.getInstance()
          .search("timestamp:[0 TO 150]").length);

      System.out.println("======== RESULTS =======");
      for (ScoreDoc doc : results) {
        Document document = LuceneIndex.getInstance().getDocumentById(doc.doc);
        System.out.println("Content: " + document.get("content"));
        System.out.println("Path: " + document.get("path"));
        System.out.println("Timestamp: " + document.get("timestamp"));
      }
      System.out.println("========================");

    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
