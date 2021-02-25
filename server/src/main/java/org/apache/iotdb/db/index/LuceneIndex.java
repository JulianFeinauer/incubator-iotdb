/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.index;

import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LuceneIndex {

  private static final Logger logger = LoggerFactory.getLogger(LuceneIndex.class);

  private static final LuceneIndex INSTANCE = new LuceneIndex();
  public static final String FIELD_CONTENT = "content";
  public static final String FIELD_TIMESTAMP = "timestamp";
  public static final String FIELD_PATH = "path";

  private int count = 0;
  private final Directory directory;
  private final Analyzer analyzer;
  private final IndexWriter iwriter;
  private DirectoryReader ireader;
  private IndexSearcher isearcher;
  private QueryParser parser;
  private boolean initialized;

  public LuceneIndex() {
    analyzer = new StandardAnalyzer();

    try {
      Path indexPath = Files.createTempDirectory("tempIndex");
      directory = FSDirectory.open(indexPath);
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      iwriter = new IndexWriter(directory, config);
    } catch (IOException e) {
      throw new IllegalStateException("Oops", e);
    }
  }

  private void initReader() {
    try {
      ireader = DirectoryReader.open(directory);
      isearcher = new IndexSearcher(ireader);
      // Parse a simple query that searches for "text":
      parser = new QueryParser(FIELD_CONTENT, analyzer);
    } catch (IOException e) {
      throw new IllegalStateException("Oops", e);
    }
  }

  public static LuceneIndex getInstance() {
    return LuceneIndex.INSTANCE;
  }

  public static void register(MeasurementSchema schema, long time, Binary binary) {
    LuceneIndex.INSTANCE._register(schema, time, binary);
  }

  private void _register(MeasurementSchema schema, long time, Binary binary) {
    logger.info("Registering timepoint {} with value {} for schema {}", time, binary, schema);
    Document doc = new Document();
    doc.add(new Field(FIELD_CONTENT, binary.toString(), TextField.TYPE_STORED));
    doc.add(new LongPoint(FIELD_TIMESTAMP, time));
    doc.add(new StoredField(FIELD_TIMESTAMP, time));
    doc.add(new Field(FIELD_PATH, schema.getMeasurementId(), TextField.TYPE_STORED));
    try {
      iwriter.addDocument(doc);
      iwriter.commit();
    } catch (IOException e) {
      throw new IllegalStateException("Oops", e);
    }
    this.count++;
  }

  public int getRegistered() {
    return this.count;
  }

  public ScoreDoc[] search(String queryString) {
    if (!initialized) {
      this.initReader();
      this.initialized = true;
    }
    // Now search the index:
    try {
      Query query = parser.parse(queryString);
      ScoreDoc[] hits = isearcher.search(query, 10).scoreDocs;

      return hits;
    } catch (IOException | ParseException e) {
      throw new IllegalStateException("Oops", e);
    }
  }

  public Document getDocumentById(int i) {
    try {
      return isearcher.doc(i);
    } catch (IOException e) {
      throw new IllegalStateException("Oops", e);
    }
  }

  public void close() {
    analyzer.close();
    try {
      iwriter.close();
    } catch (IOException e) {
      // Intentionally do nothing
    }
    try {
      directory.close();
    } catch (IOException e) {
      // Intentionally do nothing
    }
    try {
      ireader.close();
    } catch (IOException e) {
      // Intentionally do nothing
    }
    try {
      directory.close();
    } catch (IOException e) {
      // Intentionally do nothing
    }
  }
}

