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

package co.cask.cdap.data2.util.hbase;

/**
 * Common utility methods for dealing with HBase table name conversions.
 */
public abstract class HTableNameConverter {
  /**
   * Gets the system configuration table prefix
   * @param tableName Full table name.
   * @return System configuration table prefix (full table name minus the table qualifier).
   * Example input: "cdap_ns.table.name"  -->  output: "cdap_system."   (hbase 94)
   * Example input: "cdap.table.name"     -->  output: "cdap_system."   (hbase 94. input table is in default namespace)
   * Example input: "cdap_ns:table.name"  -->  output: "cdap_system:"   (hbase 96, 98)
   */
  public abstract String getSysConfigTablePrefix(String tableName);

}
