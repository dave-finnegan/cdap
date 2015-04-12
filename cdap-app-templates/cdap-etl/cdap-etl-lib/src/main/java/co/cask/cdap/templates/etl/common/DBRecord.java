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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Writable class for DB Source/Sink
 *
 * {@see DBWritable}, {@see DBInputFormat}, {@see DBOutputFormat}
 */
public class DBRecord implements Writable, DBWritable {

  private StructuredRecord record;

  public void readFields(DataInput in) throws IOException {
  }

  /**
   * @return the {@link StructuredRecord} contained in this object
   */
  public StructuredRecord getRecord() {
    return record;
  }

  /**
   * Builds the {@link #record} using the specified {@link ResultSet}
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  public void readFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> fields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      String columnType = metadata.getColumnClassName(i);
      Schema.Field field = Schema.Field.of(columnName, Schema.of(getType(columnType)));
      fields.add(field);
    }
    Schema schema = Schema.recordOf("dbRecord", fields);
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (Schema.Field field : fields) {
      recordBuilder.set(field.getName(), resultSet.getObject(field.getName()));
    }
    record = recordBuilder.build();
  }

  public void write(DataOutput out) throws IOException {
  }

  /**
   * Writes the {@link #record} to the specified {@link PreparedStatement}
   *
   * @param stmt the {@link PreparedStatement} to write the {@link StructuredRecord} to
   */
  public void write(PreparedStatement stmt) throws SQLException {
    Schema recordSchema = record.getSchema();
    List<Schema.Field> schemaFields = recordSchema.getFields();
    for (int i = 0; i < schemaFields.size(); i++) {
      // In JDBC, field indices start with 1
      int jdbcFieldIndex = i + 1;
      Schema.Field field = schemaFields.get(i);
      String fieldName = field.getName();
      Schema.Type fieldType = field.getSchema().getType();
      Object fieldValue = record.get(fieldName);
      writeValue(stmt, fieldType, fieldValue, jdbcFieldIndex);
    }
  }

  private Schema.Type getType(String columnType) {
    if (columnType == null) {
      return Schema.Type.NULL;
    } else if (String.class.getName().equals(columnType)) {
      return Schema.Type.STRING;
    } else if (Boolean.class.getName().equals(columnType)) {
      return Schema.Type.BOOLEAN;
    } else if (Integer.class.getName().equals(columnType)) {
      return Schema.Type.INT;
    } else if (Long.class.getName().equals(columnType)) {
      return Schema.Type.LONG;
    } else if (Float.class.getName().equals(columnType)) {
      return Schema.Type.FLOAT;
    } else if (Double.class.getName().equals(columnType)) {
      return Schema.Type.DOUBLE;
    } else {
      throw new IllegalArgumentException("Unsupported datatype: " + columnType);
    }
  }

  private void writeValue(PreparedStatement stmt, Schema.Type fieldType, Object fieldValue,
                          int fieldIndex) throws SQLException {
    switch (fieldType) {
      case NULL:
        stmt.setNull(fieldIndex, fieldIndex);
        break;
      case STRING:
        stmt.setString(fieldIndex, (String) fieldValue);
        break;
      case BOOLEAN:
        stmt.setBoolean(fieldIndex, (Boolean) fieldValue);
        break;
      case INT:
        stmt.setInt(fieldIndex, (Integer) fieldValue);
        break;
      case LONG:
        stmt.setLong(fieldIndex, (Long) fieldValue);
        break;
      case FLOAT:
        stmt.setFloat(fieldIndex, (Float) fieldValue);
        break;
      case DOUBLE:
        stmt.setDouble(fieldIndex, (Double) fieldValue);
        break;
      default:
        throw new SQLException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }
}
