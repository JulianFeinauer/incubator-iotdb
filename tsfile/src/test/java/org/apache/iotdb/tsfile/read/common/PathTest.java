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
package org.apache.iotdb.tsfile.read.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PathTest {
  @Test
  public void testPath() {
    Path a = new Path("", true);
    Assert.assertEquals("", a.getDevice());
    Assert.assertEquals("", a.getMeasurement());
    Path b = new Path("root.\"sg\".\"d1\".\"s1\"", true);
    Assert.assertEquals("root.\"sg\".\"d1\"", b.getDevice());
    Assert.assertEquals("\"s1\"", b.getMeasurement());
    Path c = new Path("root.\"sg\".\"d1\".s1", true);
    Assert.assertEquals("root.\"sg\".\"d1\"", c.getDevice());
    Assert.assertEquals("s1", c.getMeasurement());
    Path d = new Path("s1", true);
    Assert.assertEquals("s1", d.getMeasurement());
    Assert.assertEquals("", d.getDevice());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongPath() {
    Path c = new Path("root.\"sg\".\"d1\".\"s1\"\"", true);
    System.out.println(c.getMeasurement());
  }

  @Test
  public void testNewRegex() {
    final List<String> segments = Path.generateSegments("a.\"b\\\"\".c");
    assertEquals("a", segments.get(0));
    assertEquals("b\"", segments.get(1));
    assertEquals("c", segments.get(2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNewRegexFails() {
    final List<String> segments = Path.generateSegments("a.");
    assertEquals("a", segments.get(0));
    assertEquals("b\"", segments.get(1));
    assertEquals("c", segments.get(2));
  }
}