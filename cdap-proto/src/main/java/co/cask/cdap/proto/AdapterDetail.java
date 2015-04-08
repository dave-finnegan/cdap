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

import com.google.common.base.Objects;
import com.google.gson.JsonElement;

/**
 * Adapter details that come back from get calls.
 * TODO: finalize what should be present here
 */
public final class AdapterDetail {
  private final String name;
  private final String description;
  private final String template;
  private final JsonElement config;

  public AdapterDetail(String name, String description, String template, JsonElement config) {
    this.name = name;
    this.description = description;
    this.template = template;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getTemplate() {
    return template;
  }

  public JsonElement getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdapterDetail that = (AdapterDetail) o;

    return Objects.equal(name, that.name) &&
      Objects.equal(description, that.description) &&
      Objects.equal(template, that.template) &&
      Objects.equal(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, description, template, config);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("description", description)
      .add("template", template)
      .add("config", config)
      .toString();
  }
}
