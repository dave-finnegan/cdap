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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Transform;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Transforms a {@link StructuredRecord} to a byte array containing its JSON representation
 * Only used for testing. Can be moved to test once plugin management is added
 */
public class StructuredRecordToByteArrayTransform extends Transform<LongWritable, StructuredRecord, byte[], byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(DBRecordToStructuredRecordTransform.class);
  private static final Gson GSON = new Gson();

  @Override
  public void transform(@Nullable LongWritable inputKey, StructuredRecord structuredRecord,
                        Emitter<byte [], byte []> emitter) throws Exception {
    if (inputKey == null) {
      LOG.debug("Found null input key. Ignoring.");
      return;
    }
    emitter.emit(String.valueOf(inputKey.get()).getBytes(),
                 GSON.toJson(getData(structuredRecord)).getBytes(Charsets.UTF_8));
  }

  private Map<String, Object> getData(StructuredRecord structuredRecord) {
    Map<String, Object> data = Maps.newHashMap();
    Schema schema = structuredRecord.getSchema();
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      data.put(field.getName(), structuredRecord.get(field.getName()));
    }
    return data;
  }
}
