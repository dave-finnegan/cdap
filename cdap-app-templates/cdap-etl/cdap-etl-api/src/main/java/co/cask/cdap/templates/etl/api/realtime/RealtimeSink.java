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

package co.cask.cdap.templates.etl.api.realtime;

import co.cask.cdap.api.ProgramLifecycle;

/**
 * Realtime Sink.
 *
 * @param <I> Object sink operates on
 */
public abstract class RealtimeSink<I> implements ProgramLifecycle<SinkContext> {

  private SinkContext context;

  /**
   * Configure the Sink.
   *
   * @param configurer {@link RealtimeConfigurer}
   */
  public void configure(RealtimeConfigurer configurer) {
    // no-op
  }

  /**
   * Initialize the Sink.
   *
   * @param context {@link SinkContext}
   */
  public void initialize(SinkContext context) {
    this.context = context;
  }

  /**
   * Write the given object.
   *
   * @param object object to be written
   */
  public abstract void write(I object);

  /**
   * Invoked when source is suspended.
   */
  public void onSuspend() {
    // no-op
  }

  /**
   * Resume/reconfigure from the state of suspension.
   */
  public void onResume() {
    // no-op
  }

  @Override
  public void destroy() {
    //no-op
  }

  protected SinkContext getContext() {
    return context;
  }
}
