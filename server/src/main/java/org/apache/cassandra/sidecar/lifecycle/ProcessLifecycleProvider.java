/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.lifecycle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Manage the lifecycle of Cassandra instances running on local processes
 */
public class ProcessLifecycleProvider implements LifecycleProvider
{
    static final String OPT_CASSANDRA_HOME = "cassandra_home";
    static final String OPT_CASSANDRA_CONF_DIR = "cassandra_conf_dir";
    static final String OPT_CASSANDRA_LOG_DIR = "cassandra_log_dir";
    static final String OPT_STATE_DIR = "state_dir";

    protected static final Logger LOG = LoggerFactory.getLogger(ProcessLifecycleProvider.class);
    public static final long CASSANDRA_PROCESS_TIMEOUT_MS = Long.getLong("cassandra.sidecar.lifecycle.process.timeout.ms", 120_000L);

    private final String lifecycleDir;
    private final String defaultCassandraHome;
    private final Map<String, String> defaultJvmProperties = new HashMap<>();
    private final Map<String, String> defaultEnvVars = new HashMap<>();

    public ProcessLifecycleProvider(Map<String, String> params)
    {
        // Extract any JVM properties or environment variables from the params
        for (Map.Entry<String, String> entry : params.entrySet())
        {
            if (entry.getKey().startsWith("cassandra."))
            {
                defaultJvmProperties.put(entry.getKey(), entry.getValue());
            }
            else if (entry.getKey().startsWith("env."))
            {
                defaultEnvVars.put(entry.getKey().replaceAll("env.", ""), entry.getValue());
            }
        }
        this.lifecycleDir = params.get(OPT_STATE_DIR);
        this.defaultCassandraHome = params.get(OPT_CASSANDRA_HOME);
        if (lifecycleDir == null || lifecycleDir.isEmpty())
        {
            throw new ConfigurationException("Configuration property '" + OPT_STATE_DIR + "' must be set for ProcessLifecycleProvider");
        }
        if (defaultCassandraHome == null || defaultCassandraHome.isEmpty())
        {
            throw new ConfigurationException("Configuration property '" + OPT_CASSANDRA_HOME + "' must be set for ProcessLifecycleProvider");
        }
    }

    @Override
    public void start(InstanceMetadata instance)
    {
        if (isCassandraProcessRunning(instance))
        {
            LOG.info("Cassandra instance {} is already running.", instance.host());
            return;
        }
        startCassandra(instance);
    }

    @Override
    public void stop(InstanceMetadata instance)
    {
        if (!isCassandraProcessRunning(instance))
        {
            LOG.info("Cassandra instance {} is already stopped.", instance.host());
            return;
        }
        stopCassandra(instance);
    }

    @Override
    public boolean isRunning(InstanceMetadata instance)
    {
        return isCassandraProcessRunning(instance);
    }

    private void startCassandra(InstanceMetadata instance)
    {
        ProcessRuntimeConfiguration runtimeConfig = getRuntimeConfiguration(instance);
        try
        {
            String stdoutLocation = getStdoutLocation(runtimeConfig.instanceName());
            String stderrLocation = getStderrLocation(runtimeConfig.instanceName());
            String pidFileLocation = getPidFileLocation(runtimeConfig.instanceName());
            ProcessBuilder processBuilder = runtimeConfig.buildStartCommand(pidFileLocation,
                                                                            stdoutLocation,
                                                                            stderrLocation);
            LOG.info("Starting Cassandra instance {} with command: {}", runtimeConfig.instanceName(), processBuilder.command());

            Process process = processBuilder.start();
            process.waitFor(CASSANDRA_PROCESS_TIMEOUT_MS, TimeUnit.MILLISECONDS); // blocking call, make async?

            if (isCassandraProcessRunning(instance))
            {
                LOG.info("Started Cassandra instance {} with PID {}", runtimeConfig.instanceName(), readPidFromFile(Path.of(pidFileLocation)));
            }
            else
            {
                throw new RuntimeException("Failed to start Cassandra instance " + runtimeConfig.instanceName() +
                                           ". Check stdout at " + stdoutLocation + " and stderr at " + stderrLocation);
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to start Cassandra instance " + runtimeConfig.instanceName() + " due to " + t.getMessage(), t);
        }
    }

    private void stopCassandra(InstanceMetadata instance)
    {
        ProcessRuntimeConfiguration casCfg = getRuntimeConfiguration(instance);
        try
        {
            String pidFileLocation = getPidFileLocation(casCfg.instanceName());
            Long pid = readPidFromFile(Path.of(pidFileLocation));
            Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);
            if (processHandle.isPresent())
            {
                LOG.info("Stopping process of Cassandra instance {} with PID {}.", casCfg.instanceName(), pid);
                CompletableFuture<ProcessHandle> terminationFuture = processHandle.get().onExit();
                processHandle.get().destroy();
                try
                {
                    terminationFuture.get(CASSANDRA_PROCESS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e)
                {
                    LOG.warn("Process {} did not terminate within timeout, forcing destroy.", pid);
                    boolean destroyed = processHandle.get().destroyForcibly();
                    if (!destroyed)
                    {
                        throw new RuntimeException("Failed to forcibly destroy process " + pid +
                                                   " for Cassandra instance " + casCfg.instanceName(), e);
                    }
                    LOG.info("Process {} was forcibly destroyed.", pid);
                }
                Files.deleteIfExists(Path.of(pidFileLocation));
            }
            else
            {
                LOG.warn("No process running for Cassandra instance {} with PID {}.", casCfg.instanceName(), pid);
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to stop process for Cassandra instance " + casCfg.instanceName() + " due to " + t.getMessage(), t);
        }
    }

    @VisibleForTesting
    protected ProcessRuntimeConfiguration getRuntimeConfiguration(InstanceMetadata instance)
    {
        String cassandraHome = Optional.ofNullable(instance.lifecycleOptions().get(OPT_CASSANDRA_HOME))
                                       .orElse(defaultCassandraHome);
        String cassandraConfDir = instance.lifecycleOptions().get(OPT_CASSANDRA_CONF_DIR);
        String cassandraLogDir = instance.lifecycleOptions().get(OPT_CASSANDRA_LOG_DIR);
        return new ProcessRuntimeConfiguration.Builder()
                                        .withHost(instance.host())
                                        .withCassandraHome(cassandraHome)
                                        .withCassandraConfDir(cassandraConfDir)
                                        .withCassandraLogDir(cassandraLogDir)
                                        .withStorageDir(instance.storageDir())
                                        .withJvmOptions(defaultJvmProperties)
                                        .withEnvVars(defaultEnvVars)
                                        .build();
    }

    /**
     * Checks whether a Cassandra instance is currently running as a local process
     * and automatically cleans up stale PID files.
     *
     * Performs four validation steps:
     * 1. Verifies the PID file exists and is readable. Returns false if not found.
     * 2. Reads the PID and checks if the process is alive. Returns false and deletes the
     *    PID file if the process no longer exists or is not alive.
     * 3. Verifies the process is a Cassandra instance by checking for
     *    org.apache.cassandra.service.CassandraDaemon in the command line. Returns true if
     *    the command line contains the Cassandra daemon class or cannot be determined.
     * 4. If the process is running but is not a Cassandra process, returns false and deletes
     *    the stale PID file.
     *
     * @param instance the instance metadata containing host information
     * @return true if the instance is running as a Cassandra process, false otherwise
     */
    private boolean isCassandraProcessRunning(InstanceMetadata instance)
    {
        Path pidFilePath = Path.of(getPidFileLocation(instance.host()));
        try
        {
            // Case 1: PID file does not exist or is not readable
            if (!Files.isRegularFile(pidFilePath) || !Files.isReadable(pidFilePath))
            {
                LOG.debug("PID file does not exist or is not readable for instance {} at path {}", instance.host(), pidFilePath);
                return false;
            }

            Long pid = readPidFromFile(pidFilePath);
            Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);

            // Case 2: No process running with such PID or process is not alive
            if (processHandle.isEmpty() || !processHandle.get().isAlive())
            {
                LOG.debug("No running process found with PID {} for instance {}", pid, instance.host());
                deletePidFile(instance, pidFilePath);
                return false;
            }
            
            // Case 3: Process with such PID is running - check if it's a Cassandra process
            // If we can't determine the command line, we assume it's Cassandra
            Optional<String> cmdLine = getCommandLinePlatformIndependent(processHandle.get());
            if (cmdLine.isEmpty() || cmdLine.get().contains("org.apache.cassandra.service.CassandraDaemon"))
            {
                LOG.debug("Cassandra instance {} is running with PID {}", instance.host(), pid);
                return true;
            }
            
            // Case 4: Process with such PID is running but it's not a Cassandra process
            LOG.debug("Process with PID {} for instance {} is not a Cassandra process (command line: {}).",
                        pid, instance.host(), cmdLine);
            deletePidFile(instance, pidFilePath);
            return false;
        }
        catch (Exception e)
        {
            LOG.warn("Failed to read PID from file {} for instance {}: {}", pidFilePath, instance.host(), e.getMessage());
            return false;
        }
    }

    protected static void deletePidFile(InstanceMetadata instance, Path pidFilePath)
    {
        try
        {
            LOG.info("Deleting stale PID file {} for instance {}", pidFilePath, instance.host());
            Files.delete(pidFilePath);
        } 
        catch (Exception e)
        {
            LOG.warn("Failed to delete stale PID file {} for instance {}: {}", pidFilePath, instance.host(), e.getMessage());
        }
    }

    /*
     * Due to JDK-8345117 java can sometimes truncate the command line returned by ProcessHandle.info().commandLine()
     * on some platforms (ie. Linux). To work around this, we use the 'ps' command to get the full command line.
     * This method should be platform-independent as it relies on the 'ps' command which is available on most Unix-like systems.
     * For non-Unix systems, we fall back to the default implementation.
    */
    protected static Optional<String> getCommandLinePlatformIndependent(ProcessHandle processHandle)
    {
        long pid = processHandle.pid();
        try
        {
            ProcessBuilder pb = new ProcessBuilder("ps", "-p", String.valueOf(pid), "-o", "args=");
            Process proc = pb.start();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(proc.getInputStream())))
            {
                String line = reader.readLine();
                proc.waitFor(5, TimeUnit.SECONDS);
                if (line != null && !line.isEmpty())
                {
                    return Optional.of(line.trim());
                }
            }
        }
        catch (Exception e) 
        {
            LOG.warn("Failed to get command line via ps for PID {}", pid, e);
        }
        // Fallback to default implementation
        return processHandle.info().commandLine();
    }

    @VisibleForTesting
    public static Long readPidFromFile(Path pidFilePath)
    {
        try
        {
            String pidFileContent = Files.readString(pidFilePath).trim();
            return Long.parseLong(pidFileContent);
        }
        catch (IOException | NumberFormatException e)
        {
            throw new RuntimeException("Failed to read PID from file: " + pidFilePath, e);
        }
    }

    protected String getPidFileLocation(String host)
    {
        return getPidFileLocation(lifecycleDir, host);
    }

    protected String getStdoutLocation(String instanceName)
    {
        return Paths.get(lifecycleDir, "cassandra-" + instanceName + ".out").toString();
    }

    protected String getStderrLocation(String instanceName)
    {
        return Paths.get(lifecycleDir, "cassandra-" + instanceName + ".err").toString();
    }

    @VisibleForTesting
    public static String getPidFileLocation(String lifecycleDir, String instanceName)
    {
        return Paths.get(lifecycleDir, "cassandra-" + instanceName + ".pid").toString();
    }
}
