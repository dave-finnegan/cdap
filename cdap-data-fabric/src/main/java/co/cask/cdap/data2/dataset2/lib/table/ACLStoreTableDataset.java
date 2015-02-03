/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedObjectStore;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.common.authorization.ACLEntry;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import java.util.List;
import java.util.Set;

/**
 * Simple dataset implementation of {@link co.cask.common.authorization.ACLStore}.
 *
 * TODO: Optimize
 */
public class ACLStoreTableDataset extends AbstractDataset implements ACLStoreTable {

  private static final Gson GSON = new Gson();

  private final IndexedObjectStore<ACLEntry> store;

  public ACLStoreTableDataset(DatasetSpecification spec,
                              @EmbeddedDataset("acls") IndexedObjectStore<ACLEntry> store) {
    super(spec.getName(), store);
    this.store = store;
  }

  @Override
  public void write(ACLEntry entry) throws Exception {
    byte[][] secondaryKeys = getSecondaryKeysForWrite(entry);
    store.write(getKey(entry), entry, secondaryKeys);
  }

  @Override
  public boolean exists(ACLEntry entry) throws Exception {
    return store.read(getKey(entry)) != null;
  }

  @Override
  public void delete(ACLEntry entry) throws Exception {
    store.delete(getKey(entry));
  }

  @Override
  public Set<ACLEntry> search(Iterable<Query> queries) throws Exception {
    Set<ACLEntry> result = Sets.newHashSet();
    for (Query query : queries) {
      byte[] secondaryKey = getSecondaryKey(query);
      List<ACLEntry> entries = store.readAllByIndex(secondaryKey);
      result.addAll(entries);
    }
    return result;
  }

  @Override
  public void delete(Iterable<Query> queries) throws Exception {
    for (Query query : queries) {
      store.deleteAllByIndex(getSecondaryKey(query));
    }
  }

  private byte[] getKey(ACLEntry entry) {
    return Bytes.toBytes(GSON.toJson(entry));
  }

  private byte[][] getSecondaryKeysForWrite(ACLEntry entry) {
    byte[][] keys = new byte[8][];
    // TODO: bad, but probably OK for now
    int i = 0;
    keys[i++] = getKey(new ACLEntry(entry.getObject(), null, null));
    keys[i++] = getKey(new ACLEntry(entry.getObject(), entry.getSubject(), null));
    keys[i++] = getKey(new ACLEntry(entry.getObject(), entry.getSubject(), entry.getPermission()));
    keys[i++] = getKey(new ACLEntry(entry.getObject(), null, entry.getPermission()));
    keys[i++] = getKey(new ACLEntry(null, entry.getSubject(), null));
    keys[i++] = getKey(new ACLEntry(null, entry.getSubject(), entry.getPermission()));
    keys[i++] = getKey(new ACLEntry(null, null, entry.getPermission()));
    keys[i++] = getKey(new ACLEntry(null, null, null));
    return keys;
  }

  private byte[] getSecondaryKey(ACLEntry entry) {
    return getKey(entry);
  }

  private byte[] getSecondaryKey(Query query) {
    return getSecondaryKey(new ACLEntry(query.getObjectId(), query.getSubjectId(), query.getPermission()));
  }
}
