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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBJsonIT {

  private static String[] sqls = new String[]{
      "SET STORAGE GROUP TO root.sg",

      "CREATE TIMESERIES root.sg.d1(speed) WITH DATATYPE=JSON, ENCODING=PLAIN",
  };

  private static final String TIMESTAMP_STR = "Time";
  private static final String TIMESEIRES_STR = "timeseries";
  private static final String VALUE_STR = "value";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }


  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void selectJsonTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // Show timeseries
      statement.execute("SHOW TIMESERIES");
      final ResultSet rs = statement.getResultSet();

      // Ensure that its stored as TEXT
      assertTrue(rs.next());
      assertEquals("TEXT", rs.getString(4));

      // Insert
      final boolean hasResult = statement.execute("INSERT INTO root.sg.d1(timestamp,speed) values(1, '{\"key\": 1}')");
      assertFalse(hasResult);

      boolean hasResultSet = statement.execute("select * from root.sg.d1");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        while (resultSet.next()) {
          final String json = resultSet.getString(2);
          assertEquals("{\"key\": 1}", json);
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void evenNumberTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      // Create timeseries
      statement.execute("CREATE TIMESERIES root.sg.d2(even) WITH DATATYPE=EVEN_NUMBER, ENCODING=PLAIN");

      // Show timeseries
      statement.execute("SHOW TIMESERIES");
      final ResultSet rs = statement.getResultSet();

      // Ensure that its stored as INT32
      while (rs.next()) {
        if (rs.getString(2).equals("even")) {
          assertEquals("INT32", rs.getString(4));
        }
      }

      // Insert
      final boolean hasResult = statement.execute("INSERT INTO root.sg.d2(timestamp,even) values(1, 2)");
      assertFalse(hasResult);

      boolean hasResultSet = statement.execute("select * from root.sg.d2");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        while (resultSet.next()) {
          final int even = resultSet.getInt(2);
          assertEquals(2, even);
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void evenNumberTest_withOddNumber() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      // Create timeseries
      statement.execute("CREATE TIMESERIES root.sg.d3(even) WITH DATATYPE=EVEN_NUMBER, ENCODING=PLAIN");

      // Show timeseries
      statement.execute("SHOW TIMESERIES");
      final ResultSet rs = statement.getResultSet();

      // Insert
      try {
        statement.execute("INSERT INTO root.sg.d3(timestamp,even) values(1, 3)");
      } catch (Exception e) {
        assertEquals("500: The value '3' is not applicable to type EVEN_NUMBER", e.getMessage());
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
