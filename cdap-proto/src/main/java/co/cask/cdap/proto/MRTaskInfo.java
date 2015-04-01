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

package co.cask.cdap.proto;

import java.util.Map;


/**
 * Simplified (filtered) representation of a MapReduce Task
 */
public class MRTaskInfo {
  private final String taskId;
  private final String state;
  private final long startTime;
  private final long finishTime;
  private final float progress;
  private final Map<String, Long> counters;

  public MRTaskInfo(String taskId, String state, long startTime, long finishTime, float progress,
                    Map<String, Long> counters) {
    this.taskId = taskId;
    this.state = state;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.progress = progress;
    this.counters = counters;
  }
}
