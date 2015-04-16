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
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for ETL using databases
 */
public class BatchETLDBAdapterTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static final long currentTs = System.currentTimeMillis();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static HSQLDBServer hsqlDBServer;

  @BeforeClass
  public static void setup() throws Exception {
    String hsqlDBDir = temporaryFolder.newFolder("hsqldb").getAbsolutePath();
    hsqlDBServer = new HSQLDBServer(hsqlDBDir, "testdb");
    hsqlDBServer.start();
    Connection conn = hsqlDBServer.getConnection();
    try {
      createTestTable(conn);
      prepareTestData(conn);
    } finally {
      conn.close();
    }
  }

  private static void createTestTable(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    try {
      stmt.execute("CREATE TABLE my_table" +
                     "(" +
                     "id INT NOT NULL, " +
                     "name VARCHAR(40) NOT NULL, " +
                     "score DOUBLE, " +
                     "graduated BOOLEAN, " +
                     "not_imported VARCHAR(30), " +
                     "tiny TINYINT, " +
                     "small SMALLINT, " +
                     "big BIGINT, " +
                     "float FLOAT, " +
                     "real REAL, " +
                     "numeric NUMERIC, " +
                     "decimal DECIMAL, " +
                     "bit BIT, " +
                     "date DATE, " +
                     "time TIME, " +
                     "timestamp TIMESTAMP, " +
                     "binary BINARY(100)," +
                     "blob BLOB(100), " +
                     "clob CLOB" +
                     ")");
    } finally {
      stmt.close();
    }
  }

  private static void prepareTestData(Connection conn) throws SQLException {
    PreparedStatement pStmt =
      conn.prepareStatement("INSERT INTO my_table VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    try {
      for (int i = 1; i <= 5; i++) {
        String name = "user" + i;
        pStmt.setInt(1, i);
        pStmt.setString(2, name);
        pStmt.setDouble(3, i);
        pStmt.setBoolean(4, (i % 2 == 0));
        pStmt.setString(5, "random" + i);
        pStmt.setShort(6, (short) i);
        pStmt.setShort(7, (short) i);
        pStmt.setLong(8, (long) i);
        pStmt.setFloat(9, (float) i);
        pStmt.setFloat(10, (float) i);
        pStmt.setDouble(11, i);
        if ((i % 2 == 0)) {
          pStmt.setNull(12, Types.DOUBLE);
        } else {
          pStmt.setDouble(12, i);
        }
        pStmt.setBoolean(13, (i % 2 == 1));
        pStmt.setDate(14, new Date(currentTs));
        pStmt.setTime(15, new Time(currentTs));
        pStmt.setTimestamp(16, new Timestamp(currentTs));
        pStmt.setBytes(17, name.getBytes(Charsets.UTF_8));
        pStmt.setBlob(18, new ByteArrayInputStream(name.getBytes(Charsets.UTF_8)));
        pStmt.setClob(19, new InputStreamReader(new ByteArrayInputStream(name.getBytes(Charsets.UTF_8))));
        pStmt.executeUpdate();
      }
    } finally {
      pStmt.close();
    }
  }

  @Test
  @Category(SlowTests.class)
  public void testDBSource() throws Exception {
    addDatasetInstance("keyValueTable", "table1").create();

    String path = Resources.getResource("org/hsqldb/jdbcDriver.class").getPath();
    File hsqldbJar = new File(URI.create(path.substring(0, path.indexOf('!'))));

    ApplicationManager applicationManager = deployApplication(ETLBatchTemplate.class, hsqldbJar);

    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();

    String importQuery = "SELECT id, name, score, graduated, tiny, small, big, float, real, numeric, decimal, bit, " +
      "binary, date, time, timestamp FROM my_table WHERE id < 3";
    String countQuery = "SELECT COUNT(id) from my_table WHERE id < 3";
    ETLStage source = new ETLStage(DBSource.class.getSimpleName(),
                                   ImmutableMap.of(Properties.DB.DRIVER_CLASS, hsqlDBServer.getHsqlDBDriver(),
                                                   Properties.DB.CONNECTION_STRING, hsqlDBServer.getConnectionUrl(),
                                                   Properties.DB.TABLE_NAME, "my_table",
                                                   Properties.DB.IMPORT_QUERY, importQuery,
                                                   Properties.DB.COUNT_QUERY, countQuery
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
    Assert.assertEquals("user1", firstRecord.get("NAME"));
    Assert.assertEquals("user2", secondRecord.get("NAME"));
    Assert.assertEquals(1.0, firstRecord.get("SCORE"));
    Assert.assertEquals(2.0, secondRecord.get("SCORE"));
    Assert.assertEquals(false, firstRecord.get("GRADUATED"));
    Assert.assertEquals(true, secondRecord.get("GRADUATED"));
    Assert.assertFalse(firstRecord.containsKey("NOT_IMPORTED"));
    Assert.assertFalse(secondRecord.containsKey("NOT_IMPORTED"));
    Assert.assertEquals(1.0, firstRecord.get("TINY"));
    Assert.assertEquals(2.0, secondRecord.get("TINY"));
    Assert.assertEquals(1.0, firstRecord.get("SMALL"));
    Assert.assertEquals(2.0, secondRecord.get("SMALL"));
    Assert.assertEquals(1.0, firstRecord.get("BIG"));
    Assert.assertEquals(2.0, secondRecord.get("BIG"));
    Assert.assertEquals(1.0, firstRecord.get("FLOAT"));
    Assert.assertEquals(2.0, secondRecord.get("FLOAT"));
    Assert.assertEquals(1.0, firstRecord.get("REAL"));
    Assert.assertEquals(2.0, secondRecord.get("REAL"));
    Assert.assertEquals(1.0, firstRecord.get("NUMERIC"));
    Assert.assertEquals(2.0, secondRecord.get("NUMERIC"));
    Assert.assertEquals(1.0, firstRecord.get("DECIMAL"));
    Assert.assertEquals(null, secondRecord.get("DECIMAL"));
    Assert.assertEquals(true, firstRecord.get("BIT"));
    Assert.assertEquals(false, secondRecord.get("BIT"));
    Assert.assertTrue(firstRecord.containsKey("BINARY"));
    Assert.assertTrue(secondRecord.containsKey("BINARY"));
    Assert.assertTrue(firstRecord.containsKey("DATE"));
    Assert.assertTrue(secondRecord.containsKey("DATE"));
    Assert.assertTrue(firstRecord.containsKey("TIME"));
    Assert.assertTrue(secondRecord.containsKey("TIME"));
    Assert.assertTrue(firstRecord.containsKey("TIMESTAMP"));
    Assert.assertTrue(secondRecord.containsKey("TIMESTAMP"));
    scanner.close();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    Connection conn = hsqlDBServer.getConnection();
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.execute("DROP TABLE my_table");
      } finally {
        stmt.close();
      }
      stmt.close();
    } finally {
      conn.close();
    }

    hsqlDBServer.stop();
  }

  private static class HSQLDBServer {

    private final String locationUrl;
    private final String database;
    private final String connectionUrl;
    private final Server server;
    private final String hsqlDBDriver = "org.hsqldb.jdbcDriver";

    HSQLDBServer(String location, String database) {
      this.locationUrl = String.format("%s/%s", location, database);
      this.database = database;
      this.connectionUrl = String.format("jdbc:hsqldb:hsql://localhost/%s", database);
      this.server = new Server();
    }

    public int start() throws IOException, ServerAcl.AclFormatException {
      HsqlProperties props = new HsqlProperties();
      props.setProperty("server.database.0", locationUrl);
      props.setProperty("server.dbname.0", database);
      server.setProperties(props);
      return server.start();
    }

    public int stop() {
      return server.stop();
    }

    public Connection getConnection() {
      try {
        Class.forName(hsqlDBDriver);
        return DriverManager.getConnection(connectionUrl);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public String getConnectionUrl() {
      return this.connectionUrl;
    }

    public String getHsqlDBDriver() {
      return this.hsqlDBDriver;
    }
  }
}
