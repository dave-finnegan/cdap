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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationDeployScope;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Adapters.
 */
public class AdapterService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);
  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final CConfiguration configuration;
  private final Scheduler scheduler;
  private final Store store;
  private final PreferencesStore preferencesStore;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private Map<String, ApplicationTemplateInfo> appTemplateInfos;

  @Inject
  public AdapterService(CConfiguration configuration, Scheduler scheduler, Store store,
                        ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory,
                        PreferencesStore preferencesStore, NamespacedLocationFactory namespacedLocationFactory) {
    this.configuration = configuration;
    this.scheduler = scheduler;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.store = store;
    this.managerFactory = managerFactory;
    this.appTemplateInfos = Maps.newHashMap();
    this.preferencesStore = preferencesStore;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting AdapterService");
    registerTemplates();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down AdapterService");
  }

  /**
   * Get the {@link ApplicationTemplateInfo} for a given application template.
   *
   * @param templateName the template name
   * @return instance of {@link ApplicationTemplateInfo} if available, null otherwise
   */
  @Nullable
  public ApplicationTemplateInfo getApplicationTemplateInfo(String templateName) {
    return appTemplateInfos.get(templateName);
  }

  /**
   * Retrieves the {@link AdapterSpecification} specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested {@link AdapterSpecification} or null if no such AdapterInfo exists
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public <T> AdapterSpecification<T> getAdapter(Id.Namespace namespace, String adapterName,
                                                Type type) throws AdapterNotFoundException {
    AdapterSpecification<T> adapterSpec = store.getAdapter(namespace, adapterName, type);
    if (adapterSpec == null) {
      throw new AdapterNotFoundException(adapterName);
    }
    return adapterSpec;
  }

  /**
   * Retrieves the status of an Adapter specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested Adapter's status
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public AdapterStatus getAdapterStatus(Id.Namespace namespace, String adapterName) throws AdapterNotFoundException {
    AdapterStatus adapterStatus = store.getAdapterStatus(namespace, adapterName);
    if (adapterStatus == null) {
      throw new AdapterNotFoundException(adapterName);
    }
    return adapterStatus;
  }

  /**
   * Sets the status of an Adapter specified by the name in a given namespace.
   *
   * @param namespace namespace of the adapter
   * @param adapterName name of the adapter
   * @return specified Adapter's previous status
   * @throws AdapterNotFoundException if the specified adapter is not found
   */
  public AdapterStatus setAdapterStatus(Id.Namespace namespace, String adapterName, AdapterStatus status)
    throws AdapterNotFoundException {
    AdapterStatus existingStatus = store.setAdapterStatus(namespace, adapterName, status);
    if (existingStatus == null) {
      throw new AdapterNotFoundException(adapterName);
    }
    return existingStatus;
  }

  /**
   * Get all adapters in a given namespace.
   *
   * @param namespace the namespace to look up the adapters
   * @return {@link Collection} of {@link AdapterSpecification}
   */
  public Collection<AdapterSpecification<Object>> getAdapters(Id.Namespace namespace) {
    return store.getAllAdapters(namespace, Object.class);
  }

  /**
   * Retrieves an Collection of {@link AdapterSpecification} in a given namespace that use the given template.
   *
   * @param namespace namespace to lookup the adapter
   * @param template the template of requested adapters
   * @return Collection of requested {@link AdapterSpecification}
   */
  public Collection<AdapterSpecification<Object>> getAdapters(Id.Namespace namespace, final String template) {
    // Alternative is to construct the key using adapterType as well, when storing the the adapterSpec. That approach
    // will make lookup by adapterType simpler, but it will increase the complexity of lookup by namespace + adapterName
    List<AdapterSpecification<Object>> adaptersByType = Lists.newArrayList();
    Collection<AdapterSpecification<Object>> adapters = store.getAllAdapters(namespace, Object.class);
    for (AdapterSpecification<Object> adapterSpec : adapters) {
      if (adapterSpec.getTemplate().equals(template)) {
        adaptersByType.add(adapterSpec);
      }
    }
    return adaptersByType;
  }

  /**
   * Creates an adapter.
   *
   * @param namespace namespace to create the adapter
   * @param adapterSpec specification of the adapter to create
   * @throws AdapterAlreadyExistsException if an adapter with the same name already exists.
   * @throws IllegalArgumentException on other input errors.
   * @throws SchedulerException on errors related to scheduling.
   */
  public <T> void createAdapter(Id.Namespace namespace, AdapterSpecification<T> adapterSpec)
    throws IllegalArgumentException, AdapterAlreadyExistsException, SchedulerException {

    ApplicationTemplateInfo applicationTemplateInfo = appTemplateInfos.get(adapterSpec.getTemplate());
    Preconditions.checkArgument(applicationTemplateInfo != null,
                                "Adapter type %s not found", adapterSpec.getTemplate());
    String adapterName = adapterSpec.getName();
    if (store.getAdapter(namespace, adapterName, Object.class) != null) {
      throw new AdapterAlreadyExistsException(adapterName);
    }

    ApplicationSpecification appSpec = deployApplication(namespace, applicationTemplateInfo);

    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED, "true");
    preferencesStore.setProperties(namespace.getId(), appSpec.getName(), properties);
    schedule(namespace, appSpec, applicationTemplateInfo, adapterSpec);
    store.addAdapter(namespace, adapterSpec);
  }

  /**
   * Remove adapter identified by the namespace and name.
   *
   * @param namespace namespace id
   * @param adapterName adapter name
   * @throws AdapterNotFoundException if the adapter to be removed is not found.
   * @throws SchedulerException on errors related to scheduling.
   */
  public void removeAdapter(Id.Namespace namespace, String adapterName) throws NotFoundException, SchedulerException {
    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName, Object.class);
    ApplicationSpecification appSpec =
      store.getApplication(Id.Application.from(namespace, adapterSpec.getTemplate()));
    unschedule(namespace, appSpec, appTemplateInfos.get(adapterSpec.getTemplate()), adapterSpec);
    store.removeAdapter(namespace, adapterName);

    // TODO: Delete the application if this is the last adapter
  }

  // Suspends all schedules for this adapter
  public void stopAdapter(Id.Namespace namespace, String adapterName)
    throws NotFoundException, InvalidAdapterOperationException, SchedulerException {
    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    if (AdapterStatus.STOPPED.equals(adapterStatus)) {
      throw new InvalidAdapterOperationException("Adapter is already stopped.");
    }

    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName, Object.class);
    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespace, adapterSpec.getTemplate()));

    ProgramType programType = appTemplateInfos.get(adapterSpec.getTemplate()).getProgramType();
    Preconditions.checkArgument(programType.equals(ProgramType.WORKFLOW),
                                String.format("Unsupported program type %s for adapter", programType.toString()));
    Map<String, WorkflowSpecification> workflowSpecs = appSpec.getWorkflows();
    for (Map.Entry<String, WorkflowSpecification> entry : workflowSpecs.entrySet()) {
      Id.Program programId = Id.Program.from(namespace.getId(), appSpec.getName(), ProgramType.WORKFLOW,
                                             entry.getValue().getName());
      scheduler.suspendSchedule(programId, SchedulableProgramType.WORKFLOW,
                                constructScheduleName(programId, adapterName));
    }

    setAdapterStatus(namespace, adapterName, AdapterStatus.STOPPED);
  }

  // Resumes all schedules for this adapter
  public void startAdapter(Id.Namespace namespace, String adapterName)
    throws NotFoundException, InvalidAdapterOperationException, SchedulerException {
    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    if (AdapterStatus.STARTED.equals(adapterStatus)) {
      throw new InvalidAdapterOperationException("Adapter is already started.");
    }

    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName, Object.class);
    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespace, adapterSpec.getTemplate()));

    ProgramType programType = appTemplateInfos.get(adapterSpec.getTemplate()).getProgramType();
    Preconditions.checkArgument(programType.equals(ProgramType.WORKFLOW),
                                String.format("Unsupported program type %s for adapter", programType.toString()));
    Map<String, WorkflowSpecification> workflowSpecs = appSpec.getWorkflows();
    for (Map.Entry<String, WorkflowSpecification> entry : workflowSpecs.entrySet()) {
      Id.Program programId = Id.Program.from(namespace.getId(), appSpec.getName(), ProgramType.WORKFLOW,
                                             entry.getValue().getName());
      scheduler.resumeSchedule(programId, SchedulableProgramType.WORKFLOW,
                               constructScheduleName(programId, adapterName));
    }

    setAdapterStatus(namespace, adapterName, AdapterStatus.STARTED);
  }

  // Deploys adapter application if it is not already deployed.
  private ApplicationSpecification deployApplication(Id.Namespace namespace,
                                                     ApplicationTemplateInfo applicationTemplateInfo) {
    try {
      ApplicationSpecification spec = store.getApplication(
        Id.Application.from(namespace, applicationTemplateInfo.getName()));
      // Application is already deployed.
      if (spec != null) {
        return spec;
      }

      Manager<DeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Namespace id, Id.Program programId, ProgramType type) throws ExecutionException {
          // no-op
        }
      });

      Location namespaceHomeLocation = namespacedLocationFactory.get(namespace);
      if (!namespaceHomeLocation.exists()) {
        String msg = String.format("Home directory %s for namespace %s not found",
                                   namespaceHomeLocation.toURI().getPath(), namespace);
        LOG.error(msg);
        throw new FileNotFoundException(msg);
      }

      String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
      Location destination = namespaceHomeLocation.append(appFabricDir)
        .append(Constants.ARCHIVE_DIR).append(applicationTemplateInfo.getFile().getName());
      DeploymentInfo deploymentInfo = new DeploymentInfo(applicationTemplateInfo.getFile(), destination,
                                                         ApplicationDeployScope.SYSTEM);
      ApplicationWithPrograms applicationWithPrograms =
        manager.deploy(namespace, applicationTemplateInfo.getName(), deploymentInfo).get();
      return applicationWithPrograms.getSpecification();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Schedule all the programs needed for the adapter. Currently, only scheduling of workflow is supported.
  private <T> void schedule(Id.Namespace namespace, ApplicationSpecification spec,
                            ApplicationTemplateInfo applicationTemplateInfo,
                            AdapterSpecification<T> adapterSpec) throws SchedulerException {
    ProgramType programType = applicationTemplateInfo.getProgramType();
    // Only Workflows are supported to be scheduled in the current implementation
    Preconditions.checkArgument(programType.equals(ProgramType.WORKFLOW),
                                String.format("Unsupported program type %s for adapter", programType.toString()));
    Map<String, WorkflowSpecification> workflowSpecs = spec.getWorkflows();
    for (Map.Entry<String, WorkflowSpecification> entry : workflowSpecs.entrySet()) {
      Id.Program programId = Id.Program.from(namespace.getId(), spec.getName(), ProgramType.WORKFLOW,
                                             entry.getValue().getName());
      addSchedule(programId, SchedulableProgramType.WORKFLOW, adapterSpec);
    }
  }

  // Unschedule all the programs needed for the adapter. Currently, only unscheduling of workflow is supported.
  private <T> void unschedule(Id.Namespace namespace, ApplicationSpecification spec,
                              ApplicationTemplateInfo applicationTemplateInfo,
                              AdapterSpecification<T> adapterSpec) throws NotFoundException, SchedulerException {
    // Only Workflows are supported to be scheduled in the current implementation
    ProgramType programType = applicationTemplateInfo.getProgramType();
    Preconditions.checkArgument(programType.equals(ProgramType.WORKFLOW),
                                String.format("Unsupported program type %s for adapter", programType.toString()));
    Map<String, WorkflowSpecification> workflowSpecs = spec.getWorkflows();
    for (Map.Entry<String, WorkflowSpecification> entry : workflowSpecs.entrySet()) {
      Id.Program programId = Id.Program.from(namespace.getId(), adapterSpec.getTemplate(), ProgramType.WORKFLOW,
                                             entry.getValue().getName());
      deleteSchedule(programId, SchedulableProgramType.WORKFLOW,
                     constructScheduleName(programId, adapterSpec.getName()));
    }
  }

  // Adds a schedule to the scheduler as well as to the appspec
  private void addSchedule(Id.Program programId, SchedulableProgramType programType, AdapterSpecification adapterSpec)
    throws SchedulerException {
    String cronExpr = "*/10 * * * *";
    String adapterName = adapterSpec.getName();
    Schedule schedule = Schedules.createTimeSchedule(constructScheduleName(programId, adapterName),
                                                     adapterSpec.getName() + " schedule", cronExpr);
    ScheduleSpecification scheduleSpec =
      new ScheduleSpecification(schedule,
                                new ScheduleProgramInfo(programType, programId.getId()),
                                Collections.<String, String>emptyMap());

    // TODO: remove once we call configureTemplate() when an adapter is created, which should set the schedule
    // until then, always set a default schedule.
    scheduler.schedule(programId, scheduleSpec.getProgram().getProgramType(), scheduleSpec.getSchedule());
    //TODO: Scheduler API should also manage the MDS.
    store.addSchedule(programId, scheduleSpec);
  }

  // Deletes schedule from the scheduler as well as from the app spec
  private void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException, NotFoundException {
    scheduler.deleteSchedule(programId, programType, scheduleName);
    //TODO: Scheduler API should also manage the MDS.
    store.deleteSchedule(programId, programType, scheduleName);
  }

  // Reads all the jars from the adapter directory and sets up required internal structures.
  @VisibleForTesting
  void registerTemplates() {
    try {
      File baseDir = new File(configuration.get(Constants.AppFabric.APP_TEMPLATE_DIR));
      Collection<File> files = FileUtils.listFiles(baseDir, new String[]{"jar"}, true);
      for (File file : files) {
        try {
          Manifest manifest = new JarFile(file.getAbsolutePath()).getManifest();
          ApplicationTemplateInfo applicationTemplateInfo = createAppTemplateInfo(file, manifest);
          if (applicationTemplateInfo != null) {
            appTemplateInfos.put(applicationTemplateInfo.getName(), applicationTemplateInfo);
          } else {
            LOG.warn("Missing required information to create adapter {}", file.getAbsolutePath());
          }
        } catch (IOException e) {
          LOG.warn(String.format("Unable to read adapter jar %s", file.getAbsolutePath()));
        }
      }
    } catch (Exception e) {
      LOG.warn("Unable to read the plugins directory");
    }
  }

  // TODO: call configure on the application itself to get name and program type instead of requiring
  //       them in the manifest
  private ApplicationTemplateInfo createAppTemplateInfo(File file, Manifest manifest) {
    if (manifest != null) {
      Attributes mainAttributes = manifest.getMainAttributes();

      String name = mainAttributes.getValue(ApplicationTemplateManifestAttributes.NAME);
      String programType = mainAttributes.getValue(ApplicationTemplateManifestAttributes.PROGRAM_TYPE);
      return new ApplicationTemplateInfo(file, name, ProgramType.valueOf(programType.toUpperCase()));
    }
    return null;
  }

  private static class ApplicationTemplateManifestAttributes {
    private static final String NAME = "CDAP-Adapter-Type";
    private static final String PROGRAM_TYPE = "CDAP-Adapter-Program-Type";
  }

  /**
   * @return construct a name of a schedule, given a programId and adapterName
   */
  public String constructScheduleName(Id.Program programId, String adapterName) {
    // For now, simply schedule the adapter's program with the name of the program being scheduled + name of the adapter
    return String.format("%s.%s", adapterName, programId.getId());
  }
}
