/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.tephra.runtime.TransactionModules;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

import java.net.URI;

/**
 * Debugging tool for queues in hbase.
 */
public class HBaseQueueDebugger {

  private final HBaseQueueAdmin queueAdmin;

  @Inject
  public HBaseQueueDebugger(HBaseQueueAdmin queueAdmin) {
    this.queueAdmin = queueAdmin;
  }

  private void scanQueue(URI queueNameURI) throws Exception {
    QueueName queueName = QueueName.from(queueNameURI);
    HBaseConsumerStateStore stateStore = queueAdmin.getConsumerStateStore(queueName);

    Table table = stateStore.getInternalTable();
    Scanner scanner = table.scan(null, null);

    Row row;
    while ((row = scanner.next()) != null) {
      System.out.println("got a row");
    }
  }

  public static void main(String[] args) throws Exception {
    /**
     *
     HBaseTableUtil tableUtil,
     DatasetFramework datasetFramework,
     TransactionExecutorFactory txExecutorFactory
     */
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new TransactionModules().getDistributedModules()
    );

    HBaseQueueDebugger debugger = injector.getInstance(HBaseQueueDebugger.class);
    debugger.scanQueue(URI.create("queue:///default/PurchaseHistory/PurchaseFlow/reader/out"));
  }

}
