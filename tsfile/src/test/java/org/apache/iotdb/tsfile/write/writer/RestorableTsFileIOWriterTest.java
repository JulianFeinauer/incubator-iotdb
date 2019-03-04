/**
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
package org.apache.iotdb.tsfile.write.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemTableFlushUtil;
import org.apache.iotdb.db.engine.memtable.MemTableTestUtils;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RestorableTsFileIOWriterTest {

  private RestorableTsFileIOWriter writer;
  private String processorName = "processor";
  private String insertPath = "insertfile";
  private String restorePath = insertPath + ".restore";

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanDir(insertPath);
    EnvironmentUtils.cleanDir(restorePath);
  }

  @Test
  public void testInitResource() throws IOException {
    writer = new RestorableTsFileIOWriter(processorName, insertPath);

    Pair<Long, List<ChunkGroupMetaData>> pair = writer.readRestoreInfo();
    Assert.assertEquals(true, new File(restorePath).exists());

    Assert.assertEquals(TsFileIOWriter.magicStringBytes.length, (long) pair.left);
    Assert.assertEquals(0, pair.right.size());
    writer.endFile(new FileSchema());
    deleteInsertFile();
    Assert.assertEquals(false, new File(restorePath).exists());
  }

  @Test
  public void testAbnormalRecover() throws IOException {
    writer = new RestorableTsFileIOWriter(processorName, insertPath);
    File insertFile = new File(insertPath);
    File restoreFile = new File(restorePath);
    FileOutputStream fileOutputStream = new FileOutputStream(insertFile);
    // mkdir
    fileOutputStream.write(new byte[400]);
    fileOutputStream.close();
    Assert.assertEquals(true, insertFile.exists());
    Assert.assertEquals(true, restoreFile.exists());
    Assert.assertEquals(400, insertFile.length());
    writer.endFile(new FileSchema());

    FileOutputStream out = new FileOutputStream(new File(restorePath));
    // write tsfile position using byte[8] which is present one long
    writeRestoreFile(out, 2);
    writeRestoreFile(out, 3);
    byte[] lastPositionBytes = BytesUtils.longToBytes(200);
    out.write(lastPositionBytes);
    out.close();
    writer = new RestorableTsFileIOWriter(processorName, insertPath);

    Assert.assertEquals(true, insertFile.exists());
    Assert.assertEquals(200, insertFile.length());
    Assert.assertEquals(insertPath, writer.getInsertFilePath());
    Assert.assertEquals(restorePath, writer.getRestoreFilePath());
    writer.endFile(new FileSchema());
    deleteInsertFile();
  }

  @Test
  public void testRecover() throws IOException {
    File insertFile = new File(insertPath);
    FileOutputStream fileOutputStream = new FileOutputStream(insertFile);
    fileOutputStream.write(new byte[200]);
    fileOutputStream.close();

    File restoreFile = new File(insertPath + ".restore");
    FileOutputStream out = new FileOutputStream(new File(restorePath));
    // write tsfile position using byte[8] which is present one long
    writeRestoreFile(out, 2);
    writeRestoreFile(out, 3);
    byte[] lastPositionBytes = BytesUtils.longToBytes(200);
    out.write(lastPositionBytes);
    out.close();

    writer = new RestorableTsFileIOWriter(processorName, insertPath);
    // writer.endFile(new FileSchema());

    Assert.assertEquals(true, insertFile.exists());
    Assert.assertEquals(true, restoreFile.exists());

    RestorableTsFileIOWriter tempbufferwriteResource = new RestorableTsFileIOWriter(processorName,
        insertPath);

    Assert.assertEquals(true, insertFile.exists());
    Assert.assertEquals(200, insertFile.length());
    Assert.assertEquals(insertPath, tempbufferwriteResource.getInsertFilePath());
    Assert.assertEquals(restorePath, tempbufferwriteResource.getRestoreFilePath());

    tempbufferwriteResource.endFile(new FileSchema());
    writer.endFile(new FileSchema());
    deleteInsertFile();
  }

  @Test
  public void testWriteAndRecover() throws IOException {
    writer = new RestorableTsFileIOWriter(processorName, insertPath);
    FileSchema schema = new FileSchema();
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.RLE));

    // TsFileWriter fileWriter = new TsFileWriter();
    PrimitiveMemTable memTable = new PrimitiveMemTable();
    memTable.write("d1", "s1", TSDataType.INT32, 1, "1");
    memTable.write("d1", "s1", TSDataType.INT32, 2, "1");
    memTable.write("d1", "s2", TSDataType.INT32, 1, "1");
    memTable.write("d1", "s2", TSDataType.INT32, 3, "1");
    memTable.write("d2", "s2", TSDataType.INT32, 2, "1");
    memTable.write("d2", "s2", TSDataType.INT32, 4, "1");
    MemTableFlushUtil.flushMemTable(schema, writer, memTable, 0);
    writer.flush();
    writer.appendMetadata();
    writer.getOutput().close();

    // recover
    writer = new RestorableTsFileIOWriter(processorName, insertPath);
    writer.endFile(schema);

    TsFileSequenceReader reader = new TsFileSequenceReader(insertPath);
    TsFileMetaData metaData = reader.readFileMetadata();
    Assert.assertEquals(2, metaData.getDeviceMap().size());
    List<ChunkGroupMetaData> chunkGroups = reader
        .readTsDeviceMetaData(metaData.getDeviceMap().get("d1"))
        .getChunkGroupMetaDataList();
    Assert.assertEquals(1, chunkGroups.size());

    List<ChunkMetaData> chunks = chunkGroups.get(0).getChunkMetaDataList();
    Assert.assertEquals(2, chunks.size());
    // d1.s1
    Assert.assertEquals(chunks.get(0).getStartTime(), 1);
    Assert.assertEquals(chunks.get(0).getEndTime(), 2);
    Assert.assertEquals(chunks.get(0).getNumOfPoints(), 2);
    // d1.s2
    Assert.assertEquals(chunks.get(1).getStartTime(), 1);
    Assert.assertEquals(chunks.get(1).getEndTime(), 3);
    Assert.assertEquals(chunks.get(1).getNumOfPoints(), 2);

    chunkGroups = reader.readTsDeviceMetaData(metaData.getDeviceMap().get("d2")).getChunkGroupMetaDataList();
    Assert.assertEquals(1, chunkGroups.size());
    chunks = chunkGroups.get(0).getChunkMetaDataList();
    Assert.assertEquals(1, chunks.size());
    // da.s2
    Assert.assertEquals(chunks.get(0).getStartTime(), 2);
    Assert.assertEquals(chunks.get(0).getEndTime(), 4);
    Assert.assertEquals(chunks.get(0).getNumOfPoints(), 2);

    reader.close();
  }

  @Test
  public void testFlushAndGetMetadata() throws IOException {
    writer = new RestorableTsFileIOWriter(processorName, insertPath);

    Assert.assertEquals(0,
        writer.getMetadatas(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0).size());

    IMemTable memTable = new PrimitiveMemTable();
    MemTableTestUtils.produceData(memTable, 10, 100, MemTableTestUtils.deviceId0,
        MemTableTestUtils.measurementId0,
        MemTableTestUtils.dataType0);

    MemTableFlushUtil.flushMemTable(MemTableTestUtils.getFileSchema(), writer, memTable, 0);
    writer.flush();

    Assert.assertEquals(0,
        writer.getMetadatas(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0).size());
    writer.appendMetadata();
    Assert.assertEquals(1,
        writer.getMetadatas(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0).size());
    MemTableTestUtils.produceData(memTable, 200, 300, MemTableTestUtils.deviceId0,
        MemTableTestUtils.measurementId0,
        MemTableTestUtils.dataType0);
    writer.appendMetadata();
    Assert.assertEquals(1,
        writer.getMetadatas(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0).size());

    writer.endFile(MemTableTestUtils.getFileSchema());
    deleteInsertFile();
  }

  private void writeRestoreFile(OutputStream out, int metadataNum) throws IOException {
    TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
    List<ChunkGroupMetaData> appendRowGroupMetaDatas = new ArrayList<>();
    for (int i = 0; i < metadataNum; i++) {
      appendRowGroupMetaDatas.add(new ChunkGroupMetaData("d1", new ArrayList<>(), 0));
    }
    tsDeviceMetadata.setChunkGroupMetadataList(appendRowGroupMetaDatas);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    tsDeviceMetadata.serializeTo(baos);
    // write metadata size using int
    int metadataSize = baos.size();
    out.write(BytesUtils.intToBytes(metadataSize));
    // write metadata
    out.write(baos.toByteArray());
  }

  private void deleteInsertFile() {
    try {
      Files.delete(Paths.get(insertPath));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
