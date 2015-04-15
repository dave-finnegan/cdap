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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.DBRecord;
import co.cask.cdap.templates.etl.common.ETLDBInputFormat;
import co.cask.cdap.templates.etl.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.mysql.jdbc.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Batch source to read from a Database table
 */
public class DBSource extends BatchSource<LongWritable, DBRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSource.class);

  Driver driver;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setDescription("Batch source for database");
    configurer.addProperty(new Property(Properties.DB.DRIVER_CLASS, "Driver class to connect to the database", true));
    configurer.addProperty(
      new Property(Properties.DB.CONNECTION_STRING, "JDBC connection string including database name", true));
    configurer.addProperty(new Property(Properties.DB.TABLE_NAME, "Table name to import", true));
    configurer.addProperty(
      new Property(Properties.DB.COLUMNS,
                   "Comma-separated list of columns to import. When unspecified, defaults to all columns.", false));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    Map<String, String> properties = stageConfig.getProperties();
    String dbDriverClass = properties.get(Properties.DB.DRIVER_CLASS);
    String dbConnectionString = properties.get(Properties.DB.CONNECTION_STRING);
    String dbTableName = properties.get(Properties.DB.TABLE_NAME);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbDriverClass), "dbDriverClass cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbConnectionString), "dbConnectionString cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbTableName), "dbTableName cannot be null");
  }

  @Override
  public void prepareJob(BatchSourceContext context) {
    Map<String, String> runtimeArguments = context.getRuntimeArguments();
    String dbDriverClass = runtimeArguments.get(Properties.DB.DRIVER_CLASS);
    String dbConnectionString = runtimeArguments.get(Properties.DB.CONNECTION_STRING);
    String dbTableName = runtimeArguments.get(Properties.DB.TABLE_NAME);
    String dbColumns = runtimeArguments.get(Properties.DB.COLUMNS);

    LOG.debug("dbTableName = {}; dbDriverClass = {}; dbConnectionString = {}; dbColumns = {}",
              dbTableName, dbDriverClass, dbConnectionString, dbColumns);

    String countQueryColumn;
    if (dbColumns == null) {
      dbColumns = "*";
      countQueryColumn = "*";
    } else {
      countQueryColumn = dbColumns.substring(0, dbColumns.indexOf(","));
    }

    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    job.setInputFormatClass(ETLDBInputFormat.class);
    DBConfiguration.configureDB(conf, dbDriverClass, dbConnectionString);
    ETLDBInputFormat.setInput(job, DBRecord.class, String.format("SELECT %s FROM %s", dbColumns, dbTableName),
                              String.format("SELECT COUNT(%s) from %s", countQueryColumn, dbTableName));
    job.setInputFormatClass(ETLDBInputFormat.class);
  }
}
