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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Upgrades Dataset instances MDS
 */
public final class DatasetInstanceMDSUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceMDSUpgrader.class);
  private final LocationFactory locationFactory;
  private final CConfiguration cConf;
  private final Transactional<UpgradeMdsStores<DatasetInstanceMDS>, DatasetInstanceMDS> datasetInstanceMds;

  @Inject
  private DatasetInstanceMDSUpgrader(final TransactionExecutorFactory executorFactory,
                                     @Named("dsFramework") final DatasetFramework dsFramework,
                                     LocationFactory locationFactory, CConfiguration cConf) {
    this.locationFactory = locationFactory;
    this.cConf = cConf;
    this.datasetInstanceMds = Transactional.of(executorFactory,
       new Supplier<UpgradeMdsStores<DatasetInstanceMDS>>() {
         @Override
         public UpgradeMdsStores<DatasetInstanceMDS> get() {
           String dsName = Joiner.on(".").join(Constants.SYSTEM_NAMESPACE, DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
           Id.DatasetInstance datasetId = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, dsName);
           DatasetInstanceMDS oldMds;
           DatasetInstanceMDS newMds;
           try {
             oldMds =
               DatasetsUtil.getOrCreateDataset(dsFramework, datasetId, DatasetInstanceMDS.class.getName(),
                                               DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
           } catch (Exception e) {
             LOG.error("Failed to access table of Dataset: {}", datasetId, e);
             throw Throwables.propagate(e);
           }
           try {
             newMds = new DatasetMetaTableUtil(dsFramework).getInstanceMetaTable();
           } catch (Exception e) {
             LOG.error("Failed to access Datasets instances meta table.");
             throw Throwables.propagate(e);
           }
           return new UpgradeMdsStores<DatasetInstanceMDS>(oldMds, newMds);
         }
       });
  }

  public void upgrade() throws Exception {
    // Moves dataset instance meta entries into new table (in system namespace)
    // Also updates the spec's name ('cdap.user.foo' -> 'foo')
    LOG.info("Upgrading Dataset instance mds.");
    datasetInstanceMds.execute(new TransactionExecutor.Function<UpgradeMdsStores<DatasetInstanceMDS>, Void>() {
      @Override
      public Void apply(UpgradeMdsStores<DatasetInstanceMDS> ctx) throws Exception {
        MDSKey key = new MDSKey(Bytes.toBytes(DatasetInstanceMDS.INSTANCE_PREFIX));
        DatasetInstanceMDS newMds = ctx.getNewMds();
        List<DatasetSpecification> dsSpecs = ctx.getOldMds().list(key, DatasetSpecification.class);
        LOG.info("Upgrading {} Dataset Specifications", dsSpecs.size());
        for (DatasetSpecification dsSpec: dsSpecs) {
          LOG.info("Migrating Dataset Spec: {}", dsSpec);
          Id.Namespace namespace = namespaceFromDatasetName(dsSpec.getName());
          DatasetSpecification migratedDsSpec = migrateDatasetSpec(dsSpec);
          LOG.info("Writing new dataset Spec: {}", migratedDsSpec);
          newMds.write(namespace, migratedDsSpec);
        }
        return null;
      }
    });
  }

  /**
   * Checks if the the given dataset spec is of a {@link FileSet}
   *
   * @param dsSpec the {@link DatasetSpecification} of the dataset
   * @return a boolean which is true if its a {@link FileSet} else false
   * @throws IOException
   */
  private boolean isFileSet(DatasetSpecification dsSpec) throws IOException {
    String dsType = dsSpec.getType();
    return (FileSet.class.getName().equals(dsType) || "fileSet".equals(dsType));
  }

  /**
   * Upgrades the {@link FileSet} by moving the base path under namespaced directory
   *
   * @param dsSpec the {@link DatasetSpecification} of the {@link FileSet} to be upgraded
   * @throws IOException
   */
  private void upgradeFileSet(DatasetSpecification dsSpec) throws IOException {
    String basePath = FileSetProperties.getBasePath(dsSpec.getProperties());
    Location oldLocation = locationFactory.create(basePath);
    Location newlocation = locationFactory.create(Constants.DEFAULT_NAMESPACE)
      .append(cConf.get(Constants.Dataset.DATA_DIR)).append(basePath);
    renameLocation(oldLocation, newlocation);
  }

  /**
   * Construct a {@link Id.DatasetInstance} from a pre-2.8.0 CDAP Dataset name
   *
   * @param datasetName the dataset/table name to construct the {@link Id.DatasetInstance} from
   * @return the {@link Id.DatasetInstance} object for the specified dataset/table name
   */
  private static Id.DatasetInstance from(String datasetName) {
    Preconditions.checkArgument(datasetName != null, "Dataset name should not be null");
    // Dataset/Table name is expected to be in the format <table-prefix>.<namespace>.<name>
    String invalidFormatError = String.format("Invalid format for dataset '%s'. " +
                                                "Expected - <table-prefix>.<namespace>.<dataset-name>", datasetName);
    String [] parts = datasetName.split("\\.", 3);
    Preconditions.checkArgument(parts.length == 3, invalidFormatError);
    // Ignore the prefix in the input name.
    return Id.DatasetInstance.from(parts[1], parts[2]);
  }

  private DatasetSpecification migrateDatasetSpec(DatasetSpecification oldSpec) throws IOException {
    Id.DatasetInstance dsId = from(oldSpec.getName());
    String newDatasetName = dsId.getId();
    DatasetSpecification.Builder builder = DatasetSpecification.builder(newDatasetName, oldSpec.getType())
      .properties(oldSpec.getProperties());
    for (DatasetSpecification embeddedDsSpec : oldSpec.getSpecifications().values()) {
      LOG.debug("Migrating embedded Dataset spec: {}", embeddedDsSpec);
      DatasetSpecification migratedEmbeddedSpec = migrateDatasetSpec(embeddedDsSpec);
      LOG.debug("New embedded Dataset spec: {}", migratedEmbeddedSpec);
      builder.datasets(migratedEmbeddedSpec);
    }
    if (isFileSet(oldSpec)) {
      upgradeFileSet(oldSpec);
    }
    return builder.build();
  }

  private Id.Namespace namespaceFromDatasetName(String dsName) {
    // input of the form: 'cdap.user.foo', or 'cdap.system.app.meta'
    Id.DatasetInstance dsId = from(dsName);
    String namespace = dsId.getNamespaceId();
    if (Constants.SYSTEM_NAMESPACE.equals(namespace)) {
      return Constants.SYSTEM_NAMESPACE_ID;
    } else if ("user".equals(namespace)) {
      return Constants.DEFAULT_NAMESPACE_ID;
    } else {
      throw new IllegalArgumentException(String.format("Expected Dataset namespace to be either 'system' or 'user': %s",
                                                       dsId));
    }
  }

  private static final class UpgradeMdsStores<T> implements Iterable<T> {
    private final List<T> stores;

    private UpgradeMdsStores(T oldMds, T newMds) {
      this.stores = ImmutableList.of(oldMds, newMds);
    }

    private T getOldMds() {
      return stores.get(0);
    }

    private T getNewMds() {
      return stores.get(1);
    }

    @Override
    public Iterator<T> iterator() {
      return stores.iterator();
    }
  }

  /**
   * Renames the old location to new location if old location exists and the new one does not
   *
   * @param oldLocation the old {@link Location}
   * @param newLocation the new {@link Location}
   * @return new location if and only if the file or directory is successfully moved; null otherwise.
   * @throws IOException
   */
  @Nullable
  protected Location renameLocation(Location oldLocation, Location newLocation) throws IOException {
    // if the newLocation does not exists or the oldLocation does we try to rename. If either one of them is false then
    // the underlying call to renameTo will throw IOException which we propagate.
    if (!newLocation.exists() || oldLocation.exists()) {
      Locations.getParent(newLocation).mkdirs();
      try {
        return oldLocation.renameTo(newLocation);
      } catch (IOException ioe) {
        newLocation.delete();
        LOG.warn("Failed to rename {} to {}", oldLocation, newLocation);
        throw ioe;
      }
    } else {
      LOG.debug("New location {} already exists and old location {} does not exists. The location might already be " +
                  "updated.", newLocation, oldLocation);
      return null;
    }
  }
}
