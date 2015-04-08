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

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.batch.sinks.FileSetSink;
import co.cask.cdap.templates.etl.batch.sources.FileSetSource;
import co.cask.cdap.templates.etl.common.config.ETLStage;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by rsinha on 4/7/15.
 */
public class FileSetSourceSinkTest extends TestBase {
  private static final String sourceFileset = "inputFileset";
  private static final String sinkFileset = "outputFileset";

  @Test
  public void testConfig() throws Exception {


    addDatasetInstance("fileSet", sourceFileset, FileSetProperties.builder()
      .setBasePath(sourceFileset)
      .setInputProperty(FileSetArguments.INPUT_PATHS, "some?File1")
      .setOutputProperty(FileSetArguments.OUTPUT_PATH, "some?File1")
      .build());

    DataSetManager<FileSet> table1 = getDataset(sourceFileset);
    FileSet inputFileset = table1.get();

    addDatasetInstance("fileSet", sinkFileset, FileSetProperties.builder()
      .setBasePath(sinkFileset)
      .build());

    Map<String, String> fileset2FileArgs = Maps.newHashMap();
    FileSetArguments.setOutputPath(fileset2FileArgs, "some?File2");
    FileSetArguments.setInputPath(fileset2FileArgs, "some?File2");
    DataSetManager<FileSet> table2 = getDataset(sinkFileset);
    FileSet outputFileset = table2.get();

    ApplicationManager batchManager = deployApplication(ETLBatchTemplate.class);



    OutputStream out = inputFileset.getInputLocations().get(0).getOutputStream();
    out.write(42);
    out.close();

    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();
    ETLBatchConfig adapterConfig = constructETLBatchConfig();
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);
    Map<String, String> mapReduceArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : adapterConfigurer.getArguments().entrySet()) {
      mapReduceArgs.put(entry.getKey(), entry.getValue());
    }
    MapReduceManager mrManager = batchManager.startMapReduce("ETLMapReduce", mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    batchManager.stopAll();

    InputStream in = outputFileset.getInputLocations().get(0).getInputStream();
    Assert.assertEquals(42, in.read());
    in.close();
  }


  private ETLBatchConfig constructETLBatchConfig() {
    ETLStage source = new ETLStage(FileSetSource.class.getSimpleName(), ImmutableMap.of("name", sourceFileset));
    ETLStage sink = new ETLStage(FileSetSink.class.getSimpleName(), ImmutableMap.of("name", sinkFileset));
    List<ETLStage> transformList = Lists.newArrayList();
    return new ETLBatchConfig("", source, sink, transformList);
  }
}
