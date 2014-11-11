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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.common.internal.guava.ClassPath;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.ws.rs.Path;

/**
 * Package local helper class to maintain list of resources that are visible to user programs.
 */
final class ProgramResources {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramResources.class);


  private static final List<String> PROVIDED_PACKAGES = ImmutableList.of("org.apache.hadoop", "org.apache.spark",
                                                                         "scala");
  private static final List<String> CDAP_API_PACKAGES = ImmutableList.of("co.cask.cdap.api", "co.cask.cdap.internal");
  private static final List<String> JAVAX_WS_RS_PACKAGES = ImmutableList.of("javax.ws.rs");

  private static final Predicate<URI> JAR_ONLY_URI = new Predicate<URI>() {
    @Override
    public boolean apply(URI input) {
      return input.getPath().endsWith(".jar");
    }
  };
  private static final Function<ClassPath.ClassInfo, String> CLASS_INFO_TO_CLASS_NAME =
    new Function<ClassPath.ClassInfo, String>() {
    @Override
    public String apply(ClassPath.ClassInfo input) {
      return input.getName();
    }
  };
  private static final Function<ClassPath.ClassInfo, String> CLASS_INFO_TO_RESOURCE_NAME =
    new Function<ClassPath.ClassInfo, String>() {
    @Override
    public String apply(ClassPath.ClassInfo input) {
      return input.getResourceName();
    }
  };

  private static Set<String> visibleResources;

  /**
   * Returns a Set of resource names that are visible through to user proram.
   */
  static synchronized Set<String> getVisibleResources() {
    if (visibleResources != null) {
      return visibleResources;
    }

    try {
      visibleResources = createVisibleResources();
    } catch (IOException e) {
      LOG.error("Failed to determine resources that are visible to user program", e);
      visibleResources = ImmutableSet.of();
    }
    return visibleResources;
  }


  /**
   * Returns a Set of resources name that are visible through the cdap-api module as well as Hadoop classes.
   * This includes all classes+resources in cdap-api plus all classes+resources that cdap-api
   * depends on (for example, sl4j, guava, gson, etc).
   */
  private static Set<String> createVisibleResources() throws IOException {
    // Everything should be traceable in the same ClassLoader of this class, which is the CDAP system ClassLoader
    ClassLoader classLoader = ProgramResources.class.getClassLoader();

    // Gather resources information for cdap-api classes
    Set<ClassPath.ClassInfo> apiResources = getResources(getClassPath(classLoader, Application.class),
                                                         CDAP_API_PACKAGES, Sets.<ClassPath.ClassInfo>newHashSet());
    // Trace dependencies for cdap-api classes
    Set<String> result = findClassDependencies(classLoader,
                                               Iterables.transform(apiResources, CLASS_INFO_TO_CLASS_NAME),
                                               Sets.<String>newHashSet());

    // Gather resources for javax.ws.rs classes. They are not traceable from the api classes.
    getResources(getClassPath(classLoader, Path.class), JAVAX_WS_RS_PACKAGES, CLASS_INFO_TO_RESOURCE_NAME, result);

    return getResources(ClassPath.from(classLoader, JAR_ONLY_URI),
                        PROVIDED_PACKAGES, CLASS_INFO_TO_RESOURCE_NAME, result);
  }


  /**
   * Finds all resources that are accessible in a given {@link ClassPath} that starts with certain package prefixes.
   */
  private static <T extends Collection<ClassPath.ClassInfo>> T getResources(ClassPath classPath,
                                                                            Iterable<String> packages,
                                                                            final T result) throws IOException {
    return getResources(classPath, packages, Functions.<ClassPath.ClassInfo>identity(), result);
  }

  /**
   * Finds all resources that are accessible in a given {@link ClassPath} that starts with certain package prefixes.
   * Resources information presented in the result collection is transformed by the given result transformation
   * function.
   */
  private static <V, T extends Collection<V>> T getResources(ClassPath classPath,
                                                             Iterable<String> packages,
                                                             Function<ClassPath.ClassInfo, V> resultTransform,
                                                             final T result) throws IOException {
    for (String pkg : packages) {
      Set<ClassPath.ClassInfo> packageClasses = classPath.getAllClassesRecursive(pkg);
      for (ClassPath.ClassInfo cls : packageClasses) {
        result.add(resultTransform.apply(cls));
      }
    }
    return result;
  }

  /**
   * Returns a {@link ClassPath} instance that represents the classpath that the given class is loaded from the given
   * ClassLoader.
   */
  private static ClassPath getClassPath(ClassLoader classLoader, Class<?> cls) throws IOException {
    String resourceName = cls.getName().replace('.', '/') + ".class";
    URL url = classLoader.getResource(resourceName);
    if (url == null) {
      throw new IOException("Resource not found for " + resourceName);
    }

    try {
      URI classPathURI = getClassPathURL(resourceName, url).toURI();
      return ClassPath.from(classPathURI, classLoader);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Find the URL of the classpath that contains the given resource.
   */
  private static URL getClassPathURL(String resourceName, URL resourceURL) {
    try {
      if ("file".equals(resourceURL.getProtocol())) {
        String path = resourceURL.getFile();
        // Compute the directory container the class.
        int endIdx = path.length() - resourceName.length();
        if (endIdx > 1) {
          // If it is not the root directory, return the end index to remove the trailing '/'.
          endIdx--;
        }
        return new URL("file", "", -1, path.substring(0, endIdx));
      }
      if ("jar".equals(resourceURL.getProtocol())) {
        String path = resourceURL.getFile();
        return URI.create(path.substring(0, path.indexOf("!/"))).toURL();
      }
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
    throw new IllegalStateException("Unsupported class URL: " + resourceURL);
  }

  /**
   * Finds all resource names that the given set of classes depends on.
   *
   * @param classLoader class loader for looking up .class resources
   * @param classes set of class names that need to trace dependencies from
   * @param result collection to store the resulting resource names
   * @param <T> type of the result collection
   * @throws IOException if fails to load class bytecode during tracing
   */
  private static <T extends Collection<String>> T findClassDependencies(final ClassLoader classLoader,
                                                                        Iterable<String> classes,
                                                                        final T result) throws IOException {
    final Set<String> bootstrapClassPaths = getBootstrapClassPaths();
    final Set<URL> classPathSeen = Sets.newHashSet();

    Dependencies.findClassDependencies(classLoader, new Dependencies.ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        // Ignore bootstrap classes
        if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
          return false;
        }

        if (!classPathSeen.add(classPathUrl)) {
          return true;
        }

        // Add all resources in the given class path
        try {
          ClassPath classPath = ClassPath.from(classPathUrl.toURI(), classLoader);
          for (ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
            result.add(resourceInfo.getResourceName());
          }
        } catch (Exception e) {
          // If fail to get classes/resources from the classpath, ignore this classpath.
        }
        return true;
      }
    }, classes);

    return result;
  }

  /**
   * Returns a Set containing all bootstrap classpaths as defined in the {@code sun.boot.class.path} property.
   */
  private static Set<String> getBootstrapClassPaths() {
    // Get the bootstrap classpath. This is for exclusion while tracing class dependencies.
    Set<String> bootstrapPaths = Sets.newHashSet();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      bootstrapPaths.add(file.getAbsolutePath());
      try {
        bootstrapPaths.add(file.getCanonicalPath());
      } catch (IOException e) {
        // Ignore the exception and proceed.
      }
    }
    return bootstrapPaths;
  }

  private ProgramResources() {
  }
}
