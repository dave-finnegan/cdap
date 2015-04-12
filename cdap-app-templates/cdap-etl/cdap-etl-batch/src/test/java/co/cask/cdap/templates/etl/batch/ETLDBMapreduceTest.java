/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sources.DBSource;
import co.cask.cdap.templates.etl.common.Properties;
import co.cask.cdap.templates.etl.transforms.DBRecordToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToByteArrayTransform;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for ETL using databases
 */
public class ETLDBMapreduceTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static Connection conn;
  private static Statement stmt;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static String hsqlConnectionString;

  @BeforeClass
  public static void setup() throws SQLException, IOException {
    String hsqlDBDir = temporaryFolder.newFolder("hsqldb").getAbsolutePath();
    hsqlConnectionString = String.format("jdbc:hsqldb:%s/testdb", hsqlDBDir);
    conn = DriverManager.getConnection(String.format("%s;create=true", hsqlConnectionString));
    stmt = conn.createStatement();
    stmt.execute("CREATE TABLE my_table" +
                   "(" +
                   "id INT, " +
                   "name VARCHAR(40), " +
                   "score DOUBLE, " +
                   "graduated BOOLEAN, " +
                   "not_imported BIGINT" +
                   ")");

    PreparedStatement pStmt1 = conn.prepareStatement("INSERT INTO my_table VALUES(?, ?, ?, ?, ?)");
    pStmt1.setInt(1, 1);
    pStmt1.setString(2, "bob");
    pStmt1.setDouble(3, 3.3);
    pStmt1.setBoolean(4, false);
    pStmt1.setLong(5, 3L);
    pStmt1.executeUpdate();

    pStmt1.setInt(1, 2);
    pStmt1.setString(2, "alice");
    pStmt1.setDouble(3, 3.9);
    pStmt1.setBoolean(4, true);
    pStmt1.setLong(5, 4L);
    pStmt1.executeUpdate();
  }

  @Test
  @Category(SlowTests.class)
  public void testDBSource() throws Exception {
    addDatasetInstance("keyValueTable", "table1").create();

    ApplicationManager applicationManager = deployApplication(ETLBatchTemplate.class);

    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();
    ETLStage source = new ETLStage(DBSource.class.getSimpleName(),
                                   ImmutableMap.of(Properties.DB.DRIVER_CLASS, "org.hsqldb.jdbcDriver",
                                                   Properties.DB.CONNECTION_STRING, hsqlConnectionString,
                                                   Properties.DB.TABLE_NAME, "my_table",
                                                   Properties.DB.COLUMNS, "id, name, score, graduated"
                                   ));
    ETLStage sink = new ETLStage("KVTableSink", ImmutableMap.of("name", "table1"));
    ETLStage structuredRecordTransform = new ETLStage(DBRecordToStructuredRecordTransform.class.getSimpleName(),
                                                      ImmutableMap.<String, String>of());
    ETLStage byteArrayTransform = new ETLStage(StructuredRecordToByteArrayTransform.class.getSimpleName(),
                                               ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(structuredRecordTransform, byteArrayTransform);
    ETLBatchConfig adapterConfig = new ETLBatchConfig("", source, sink, transformList);
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);
    Map<String, String> mapReduceArgs = Maps.newHashMap(adapterConfigurer.getArguments());
    MapReduceManager mrManager = applicationManager.startMapReduce("ETLMapReduce", mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    applicationManager.stopAll();
    DataSetManager<KeyValueTable> table1 = getDataset("table1");
    CloseableIterator<KeyValue<byte [], byte []>> scanner = table1.get().scan(null, null);
    // must have two records
    Assert.assertTrue(scanner.hasNext());
    KeyValue<byte [], byte[]> first = scanner.next();
    Assert.assertTrue(scanner.hasNext());
    KeyValue<byte [], byte []> second = scanner.next();
    Assert.assertFalse(scanner.hasNext());
    String firstValue = Bytes.toString(first.getValue());
    String secondValue = Bytes.toString(second.getValue());
    Type mapStringObjectType = new TypeToken<Map<String, Object>>() { }.getType();
    Map<String, Object> firstRecord = GSON.fromJson(firstValue, mapStringObjectType);
    Map<String, Object> secondRecord = GSON.fromJson(secondValue, mapStringObjectType);
    // GSON deserializes integer as Double
    Assert.assertEquals(1.0, firstRecord.get("ID"));
    Assert.assertEquals(2.0, secondRecord.get("ID"));
    Assert.assertEquals("bob", firstRecord.get("NAME"));
    Assert.assertEquals("alice", secondRecord.get("NAME"));
    Assert.assertEquals(3.3, firstRecord.get("SCORE"));
    Assert.assertEquals(3.9, secondRecord.get("SCORE"));
    Assert.assertEquals(false, firstRecord.get("GRADUATED"));
    Assert.assertEquals(true, secondRecord.get("GRADUATED"));
    Assert.assertFalse(firstRecord.containsKey("NOT_IMPORTED"));
    Assert.assertFalse(secondRecord.containsKey("NOT_IMPORTED"));
    scanner.close();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute("DROP TABLE my_table");
    stmt.close();
    conn.close();
  }
}
