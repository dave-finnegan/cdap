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

package co.cask.cdap.client;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.ProgramNotFoundException;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Provides ways to get/set Preferences.
 */
public class PreferencesClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public PreferencesClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Returns the Preferences stored at the Instance Level.
   *
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public Map<String, String> getInstancePreferences() throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("configuration/preferences");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Instance Level.
   *
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void setInstancePreferences(Map<String, String> preferences) throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("configuration/preferences");
    restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null, config.getAccessToken());
  }

  /**
   * Deletes Preferences at the Instance Level.
   *
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void deleteInstancePreferences() throws IOException, UnauthorizedException {
    URL url = config.resolveURLV3("configuration/preferences");
    restClient.execute(HttpMethod.DELETE, url, config.getAccessToken());
  }

  /**
   * Returns the Preferences stored at the Namespace Level.
   *
   * @param namespaceId Namespace Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public Map<String, String> getNamespacePreferences(String namespaceId, boolean resolved)
    throws IOException, UnauthorizedException, NotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    String res = Boolean.toString(resolved);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s?resolved=%s", namespace.getId(), res);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespace);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Namespace Level.
   *
   * @param namespaceId Namespace Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void setNamespacePreferences(String namespaceId, Map<String, String> preferences)
    throws IOException, UnauthorizedException, NotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s", namespace.getId());
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespace);
    }
  }

  /**
   * Deletes Preferences at the Namespace Level.
   *
   * @param namespaceId Namespace Id
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws NotFoundException if the requested namespace is not found
   */
  public void deleteNamespacePreferences(String namespaceId) throws IOException, UnauthorizedException,
    NotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s", namespace.getId());
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespace);
    }
  }

  /**
   * Returns the Preferences stored at the Application Level.
   *
   * @param namespaceId Namespace Id
   * @param applicationId Application Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ApplicationNotFoundException if the requested application is not found
   */
  public Map<String, String> getApplicationPreferences(String namespaceId, String applicationId, boolean resolved)
    throws IOException, UnauthorizedException, ApplicationNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Application app = Id.Application.from(namespace, applicationId);
    String res = Boolean.toString(resolved);
    URL url = config.resolveURLV3(String.format("configuration/preferences/namespaces/%s/apps/%s?resolved=%s",
                                                app.getNamespace().getId(), app.getId(), res));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(app);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Application Level.
   *
   * @param namespaceId Namespace Id
   * @param applicationId Application Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ApplicationNotFoundException if the requested application is not found
   */
  public void setApplicationPreferences(String namespaceId, String applicationId, Map<String, String> preferences)
    throws IOException, UnauthorizedException, ApplicationNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Application app = Id.Application.from(namespace, applicationId);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s/apps/%s",
                                  app.getNamespace().getId(), app.getId());
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(app);
    }
  }

  /**
   * Deletes Preferences at the Application Level.
   *
   * @param namespaceId Namespace Id
   * @param applicationId Application Id
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ApplicationNotFoundException if the request application or namespace is not found
   */
  public void deleteApplicationPreferences(String namespaceId, String applicationId)
    throws IOException, UnauthorizedException, ApplicationNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Application app = Id.Application.from(namespace, applicationId);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s/apps/%s",
                                  app.getNamespace().getId(), app.getId());
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(app);
    }
  }

  /**
   * Returns the Preferences stored at the Program Level.
   *
   * @param namespaceId Namespace Id
   * @param applicationId Application Id
   * @param programType Program Type
   * @param programId Program Id
   * @param resolved Set to True if collapsed/resolved properties are desired
   * @return map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public Map<String, String> getProgramPreferences(String namespaceId, String applicationId, ProgramType programType,
                                                   String programId, boolean resolved)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Application app = Id.Application.from(namespace, applicationId);
    Id.Program program = Id.Program.from(app, programType, programId);
    String res = Boolean.toString(resolved);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s/apps/%s/%s/%s?resolved=%s",
                                  program.getNamespaceId(), program.getApplicationId(),
                                  program.getType().getCategoryName(), program.getId(), res);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets Preferences at the Program Level.
   *
   * @param namespaceId Namespace Id
   * @param applicationId Application Id
   * @param programType Program Type
   * @param programId Program Id
   * @param preferences map of key-value pairs
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void setProgramPreferences(String namespaceId, String applicationId,
                                    ProgramType programType, String programId,
                                    Map<String, String> preferences)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Application app = Id.Application.from(namespace, applicationId);
    Id.Program program = Id.Program.from(app, programType, programId);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s/apps/%s/%s/%s",
                                  program.getNamespaceId(), program.getApplicationId(),
                                  programType.getCategoryName(), program.getId());
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, GSON.toJson(preferences), null,
                                               config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }

  /**
   * Deletes Preferences at the Program Level.
   *
   * @param namespaceId Namespace Id
   * @param applicationId Application Id
   * @param programType Program Type
   * @param programId Program Id
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws ProgramNotFoundException if the requested program is not found
   */
  public void deleteProgramPreferences(String namespaceId, String applicationId, ProgramType programType,
                                       String programId)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Application app = Id.Application.from(namespace, applicationId);
    Id.Program program = Id.Program.from(app, programType, programId);
    URL url = config.resolveURLV3("configuration/preferences/namespaces/%s/apps/%s/%s/%s",
                                  program.getNamespaceId(), program.getApplicationId(),
                                  programType.getCategoryName(), program.getId());
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }
}
