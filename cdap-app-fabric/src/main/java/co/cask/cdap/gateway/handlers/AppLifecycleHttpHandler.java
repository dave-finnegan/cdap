/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.HttpExceptionHandler;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.AppLifecycleService;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.adapter.AdapterAlreadyExistsException;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.adapter.AdapterTypeInfo;
import co.cask.cdap.internal.app.runtime.adapter.InvalidAdapterOperationException;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for managing application lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppLifecycleHttpHandler.class);

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  private final Scheduler scheduler;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final DiscoveryServiceClient discoveryServiceClient;
  private final PreferencesStore preferencesStore;
  private final AdapterService adapterService;
  private final NamespaceAdmin namespaceAdmin;
  private final AppLifecycleService appLifecycleService;
  private final HttpExceptionHandler exceptionHandler;

  @Inject
  public AppLifecycleHttpHandler(Authenticator authenticator, CConfiguration configuration,
                                 ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory,
                                 LocationFactory locationFactory, Scheduler scheduler,
                                 ProgramRuntimeService runtimeService, StoreFactory storeFactory,
                                 DiscoveryServiceClient discoveryServiceClient, PreferencesStore preferencesStore,
                                 AdapterService adapterService, AppLifecycleService appLifecycleService,
                                 NamespaceAdmin namespaceAdmin, HttpExceptionHandler exceptionHandler) {
    super(authenticator);
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.locationFactory = locationFactory;
    this.scheduler = scheduler;
    this.runtimeService = runtimeService;
    this.store = storeFactory.create();
    this.discoveryServiceClient = discoveryServiceClient;
    this.preferencesStore = preferencesStore;
    this.adapterService = adapterService;
    this.appLifecycleService = appLifecycleService;
    this.exceptionHandler = exceptionHandler;
  }

  /**
   * Deploys an application with the specified name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @PathParam("app-id") final String appId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName)
    throws IOException, NamespaceNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    return deployApplication(request, responder, namespace, appId, archiveName);
  }

  /**
   * Deploys an application.
   */
  @POST
  @Path("/apps")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName)
    throws IOException, NamespaceNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    return deployApplication(request, responder, namespace, null, archiveName);
  }

  /**
   * Gets the {@link ApplicationRecord}s describing the applications in the specified namespace.
   */
  @GET
  @Path("/apps")
  public void listApps(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) throws NamespaceNotFoundException {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Collection<ApplicationSpecification> appSpecs = appLifecycleService.listApps(namespace);
    responder.sendJson(HttpResponseStatus.OK, makeAppRecords(appSpecs));
  }

  /**
   * Gets the {@link ApplicationRecord} describing an application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getApp(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("app-id") final String appId) throws ApplicationNotFoundException {
    Id.Application app = Id.Application.from(namespaceId, appId);
    ApplicationSpecification appSpec = appLifecycleService.getApp(app);
    responder.sendJson(HttpResponseStatus.OK, makeAppRecord(appSpec));
  }

  /**
   * Deletes an application.
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") final String appId) throws Exception {
    Id.Application id = Id.Application.from(namespaceId, appId);

    // Deletion of a particular application is not allowed if that application is used by an adapter
    if (adapterService.getAdapterTypeInfo(appId) != null) {
      responder.sendString(HttpResponseStatus.CONFLICT,
                           String.format("Cannot delete Application '%s' because it's an Adapter Type", appId));
      return;
    }

    AppFabricServiceStatus appStatus = appLifecycleService.deleteApp(id);
    responder.sendString(appStatus.getCode(), appStatus.getMessage());
  }

  /**
   * Deletes all applications in the specified namespace.
   */
  @DELETE
  @Path("/apps")
  public void deleteApps(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    Id.Namespace id = Id.Namespace.from(namespaceId);
    AppFabricServiceStatus status = appLifecycleService.deleteApps(id);
    responder.sendString(status.getCode(), status.getMessage());
  }

  /**
   * Retrieves all adapters in a given namespace.
   */
  @GET
  @Path("/adapters")
  public void listAdapters(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws NamespaceNotFoundException {
    responder.sendJson(HttpResponseStatus.OK, adapterService.getAdapters(Id.Namespace.from(namespaceId)));
  }

  /**
   * Retrieves an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}")
  public void getAdapter(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("adapter-id") String adapterName) throws AdapterNotFoundException {
    AdapterSpecification adapterSpec = adapterService.getAdapter(namespaceId, adapterName);
    responder.sendJson(HttpResponseStatus.OK, adapterSpec);
  }

  /**
   * Starts/stops an adapter
   */
  @POST
  @Path("/adapters/{adapter-id}/{action}")
  public void startStopAdapter(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId,
                               @PathParam("action") String action) {
    try {
      if ("start".equals(action)) {
        adapterService.startAdapter(namespaceId, adapterId);
      } else if ("stop".equals(action)) {
        adapterService.stopAdapter(namespaceId, adapterId);
      } else {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             String.format("Invalid adapter action: %s. Possible actions: ['start', 'stop'].", action));
        return;
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (InvalidAdapterOperationException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (SchedulerException e) {
      LOG.error("Scheduler error in namespace '{}' for adapter '{}' with action '{}'",
                namespaceId, adapterId, action, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } catch (Throwable t) {
      LOG.error("Error in namespace '{}' for adapter '{}' with action '{}'", namespaceId, adapterId, action, t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Retrieves the status of an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}/status")
  public void getAdapterStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId) throws AdapterNotFoundException {
    responder.sendString(HttpResponseStatus.OK, adapterService.getAdapterStatus(namespaceId, adapterId).toString());
  }

  /**
   * Deletes an adapter
   */
  @DELETE
  @Path("/adapters/{adapter-id}")
  public void deleteAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterName) throws NotFoundException, SchedulerException {
    adapterService.removeAdapter(namespaceId, adapterName);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Create an adapter.
   */
  @POST
  @Path("/adapters/{adapter-id}")
  public void createAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterName) {

    try {
      if (!namespaceAdmin.hasNamespace(Id.Namespace.from(namespaceId))) {
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             String.format("Create adapter failed - namespace '%s' does not exist.", namespaceId));
        return;
      }

      AdapterConfig config = parseBody(request, AdapterConfig.class);
      if (config == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Insufficient parameters to create adapter");
        return;
      }

      // Validate the adapter
      String adapterType = config.getType();
      AdapterTypeInfo adapterTypeInfo = adapterService.getAdapterTypeInfo(adapterType);
      if (adapterTypeInfo == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Adapter type %s not found", adapterType));
        return;
      }

      AdapterSpecification spec = convertToSpec(adapterName, config, adapterTypeInfo);
      adapterService.createAdapter(namespaceId, spec);
      responder.sendString(HttpResponseStatus.OK, String.format("Adapter: %s is created", adapterName));
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (AdapterAlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (Throwable th) {
      LOG.error("Failed to deploy adapter", th);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, th.getMessage());
    }
  }

  private AdapterSpecification convertToSpec(String name, AdapterConfig config, AdapterTypeInfo typeInfo) {
    Map<String, String> sourceProperties = Maps.newHashMap(typeInfo.getDefaultSourceProperties());
    if (config.source.properties != null) {
      sourceProperties.putAll(config.source.properties);
    }
    Set<Source> sources = ImmutableSet.of(new Source(config.source.name, typeInfo.getSourceType(), sourceProperties));
    Map<String, String> sinkProperties = Maps.newHashMap(typeInfo.getDefaultSinkProperties());
    if (config.sink.properties != null) {
      sinkProperties.putAll(config.sink.properties);
    }
    Set<Sink> sinks = ImmutableSet.of(
      new Sink(config.sink.name, typeInfo.getSinkType(), sinkProperties));
    Map<String, String> adapterProperties = Maps.newHashMap(typeInfo.getDefaultAdapterProperties());
    if (config.properties != null) {
      adapterProperties.putAll(config.properties);
    }
    return new AdapterSpecification(name, config.getType(), adapterProperties, sources, sinks);
  }

  /**
   *
   * @param request
   * @param responder
   * @param namespace
   * @param appId if null, use appId from application specification
   * @param archiveName
   * @return
   * @throws IOException
   * @throws NamespaceNotFoundException
   */
  private BodyConsumer deployApplication(final HttpRequest request, final HttpResponder responder,
                                         final Id.Namespace namespace, @Nullable final String appId,
                                         final String archiveName) throws IOException, NamespaceNotFoundException {
    if (!namespaceAdmin.hasNamespace(namespace)) {
      throw new NamespaceNotFoundException(namespace);
    }

    Location namespaceHomeLocation = locationFactory.create(namespace.getId());
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found",
                                 namespaceHomeLocation.toURI().getPath(), namespace.getId());
      LOG.error(msg);
      responder.sendString(HttpResponseStatus.NOT_FOUND, msg);
      return null;
    }


    if (archiveName == null || archiveName.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, ARCHIVE_NAME_HEADER + " header not present",
                           ImmutableMultimap.of(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE));
      return null;
    }

    // Store uploaded content to a local temp file
    String tempBase = String.format("%s/%s", configuration.get(Constants.CFG_LOCAL_DATA_DIR), namespace.getId());
    File tempDir = new File(tempBase, configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    // note: cannot create an appId subdirectory under the namespace directory here because appId could be null here
    final Location archive =
      namespaceHomeLocation.append(appFabricDir).append(Constants.ARCHIVE_DIR).append(archiveName);

    File tempFile = File.createTempFile("app-", ".jar", tempDir);
    LOG.debug("Creating temporary app jar for deploy at '{}'", tempFile.getAbsolutePath());
    return new AbstractBodyConsumer(tempFile) {
      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try {
          DeploymentInfo deploymentInfo = new DeploymentInfo(uploadedFile, archive);
          appLifecycleService.deploy(namespace, appId, deploymentInfo);
          responder.sendStatus(HttpResponseStatus.OK);
        } catch (Throwable t) {
          exceptionHandler.handle(t, request, responder);
        }
      }
    };
  }

  /**
   * Temporarily protected only to support v2 APIs. Currently used in unrecoverable/reset. Should become private once
   * the reset API has a v3 version
   */
  protected void deleteMetrics(String namespaceId, String applicationId)
    throws IOException, NamespaceNotFoundException, ApplicationNotFoundException {

    Id.Namespace namespace = new Id.Namespace(namespaceId);
    Collection<ApplicationSpecification> applications = Lists.newArrayList();
    if (applicationId == null) {
      applications = this.store.getAllApplications(namespace);
    } else {
      ApplicationSpecification spec = this.store.getApplication(new Id.Application(namespace, applicationId));
      applications.add(spec);
    }
    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.METRICS);
    Discoverable discoverable = new RandomEndpointStrategy(discovered).pick(DISCOVERY_TIMEOUT_SECONDS,
                                                                            TimeUnit.SECONDS);

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      throw new IOException("Can't find Metrics endpoint");
    }

    for (ApplicationSpecification application : applications) {
      String url = String.format("http://%s:%d%s/metrics/%s/apps/%s",
                                 discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(),
                                 Constants.Gateway.API_VERSION_2,
                                 "ignored",
                                 application.getName());
      sendMetricsDelete(url);
    }

    if (applicationId == null) {
      String url = String.format("http://%s:%d%s/metrics", discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(), Constants.Gateway.API_VERSION_2);
      sendMetricsDelete(url);
    }
  }

  private void sendMetricsDelete(String url) {
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
      .build();

    try {
      client.delete().get(METRICS_SERVER_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("exception making metrics delete call", e);
      Throwables.propagate(e);
    } finally {
      client.close();
    }
  }

  private Iterable<ProgramSpecification> getProgramSpecs(Id.Application appId) throws ApplicationNotFoundException {
    ApplicationSpecification appSpec = store.getApplication(appId);
    Iterable<ProgramSpecification> programSpecs = Iterables.concat(appSpec.getFlows().values(),
                                                                   appSpec.getMapReduce().values(),
                                                                   appSpec.getProcedures().values(),
                                                                   appSpec.getWorkflows().values());
    return programSpecs;
  }

  /**
   * Delete the jar location of the program.
   *
   * @param appId        applicationId.
   * @throws IOException if there are errors with location IO
   */
  private void deleteProgramLocations(Id.Application appId) throws IOException, ApplicationNotFoundException {
    Iterable<ProgramSpecification> programSpecs = getProgramSpecs(appId);
    String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    for (ProgramSpecification spec : programSpecs) {
      ProgramType type = ProgramTypes.fromSpecification(spec);
      Id.Program programId = Id.Program.from(appId, spec.getName());
      try {
        Location location = Programs.programLocation(locationFactory, appFabricDir, programId, type);
        location.delete();
      } catch (FileNotFoundException e) {
        LOG.warn("Program jar for program {} not found.", programId.toString(), e);
      }
    }

    // Delete webapp
    // TODO: this will go away once webapp gets a spec
    try {
      Id.Program programId = Id.Program.from(appId.getNamespaceId(), appId.getId(),
                                             ProgramType.WEBAPP.name().toLowerCase());
      Location location = Programs.programLocation(locationFactory, appFabricDir, programId, ProgramType.WEBAPP);
      location.delete();
    } catch (FileNotFoundException e) {
      // expected exception when webapp is not present.
    }
  }

  /**
   * Delete stored Preferences of the application and all its programs.
   * @param appId applicationId
   */
  private void deletePreferences(Id.Application appId) throws ApplicationNotFoundException {
    Iterable<ProgramSpecification> programSpecs = getProgramSpecs(appId);
    for (ProgramSpecification spec : programSpecs) {

      preferencesStore.deleteProperties(appId.getNamespaceId(), appId.getId(),
                                        ProgramTypes.fromSpecification(spec).getCategoryName(), spec.getName());
      LOG.trace("Deleted Preferences of Program : {}, {}, {}, {}", appId.getNamespaceId(), appId.getId(),
                ProgramTypes.fromSpecification(spec).getCategoryName(), spec.getName());
    }
    preferencesStore.deleteProperties(appId.getNamespaceId(), appId.getId());
    LOG.trace("Deleted Preferences of Application : {}, {}", appId.getNamespaceId(), appId.getId());
  }

  /**
   * Check if any program that satisfy the given {@link Predicate} is running.
   * Protected only to support v2 APIs
   *
   * @param predicate Get call on each running {@link Id.Program}.
   * @param types Types of program to check
   * returns True if a program is running as defined by the predicate.
   */
  protected boolean checkAnyRunning(Predicate<Id.Program> predicate, ProgramType... types) {
    for (ProgramType type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  runtimeService.list(type).entrySet()) {
        ProgramController.State programState = entry.getValue().getController().getState();
        if (programState == ProgramController.State.STOPPED || programState == ProgramController.State.ERROR) {
          continue;
        }
        Id.Program programId = entry.getValue().getProgramId();
        if (predicate.apply(programId)) {
          LOG.trace("Program still running in checkAnyRunning: {} {} {} {}",
                    programId.getApplicationId(), type, programId.getId(), entry.getValue().getController().getRunId());
          return true;
        }
      }
    }
    return false;
  }

  private static ApplicationRecord makeAppRecord(ApplicationSpecification appSpec) {
    return new ApplicationRecord("App", appSpec.getName(), appSpec.getName(), appSpec.getDescription());
  }

  private static List<ApplicationRecord> makeAppRecords(Iterable<ApplicationSpecification> appSpecs) {
    return Lists.newArrayList(Iterables.transform(
      appSpecs, new Function<ApplicationSpecification, ApplicationRecord>() {
      @Nullable
      @Override
      public ApplicationRecord apply(@Nullable ApplicationSpecification appSpec) {
        return makeAppRecord(appSpec);
      }
    }));
  }
}
