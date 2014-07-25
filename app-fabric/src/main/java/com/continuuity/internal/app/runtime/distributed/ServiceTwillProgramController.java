/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *  A ProgramController for Services that are launched through Twill.
 */
final class ServiceTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceTwillProgramController.class);

  private final Lock lock;
  private final DistributedServiceRunnableInstanceUpdater instanceUpdater;

  ServiceTwillProgramController(String programId, TwillController controller,
                                DistributedServiceRunnableInstanceUpdater instanceUpdater) {
    super(programId, controller);
    this.lock = new ReentrantLock();
    this.instanceUpdater = instanceUpdater;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doCommand(String name, Object value) throws Exception {
    if (!ProgramOptionConstants.RUNNABLE_INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }

    Map<String, String> command = (Map<String, String>) value;
    lock.lock();
    try {
      changeInstances(command.get("runnable"),
                      Integer.valueOf(command.get("newInstances")),
                      Integer.valueOf(command.get("oldInstances")));
    } catch (Throwable t) {
      LOG.error("Failed to change instances: {}", command, t);
    } finally {
      lock.unlock();
    }
  }

  private synchronized void changeInstances(String runnableId, int newInstanceCount, int oldInstanceCount)
    throws Exception {
    instanceUpdater.update(runnableId, newInstanceCount, oldInstanceCount);
  }
}
