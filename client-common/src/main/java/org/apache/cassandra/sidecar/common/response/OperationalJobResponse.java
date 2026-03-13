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

package org.apache.cassandra.sidecar.common.response;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

/**
 * Response structure of the operational jobs API
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OperationalJobResponse
{
    private final UUID jobId;
    private final OperationalJobStatus status;
    private final String operation;
    private final String reason;
    private final String startTime;
    private final List<UUID> nodesPending;
    private final List<UUID> nodesExecuting;
    private final List<UUID> nodesSucceeded;
    private final List<UUID> nodesFailed;
    private final String lastUpdate;

    @JsonCreator
    public OperationalJobResponse(@JsonProperty("jobId") UUID jobId,
                                  @JsonProperty("jobStatus") OperationalJobStatus status,
                                  @JsonProperty("operation") String operation,
                                  @JsonProperty("reason") String reason,
                                  @JsonProperty("startTime") String startTime,
                                  @JsonProperty("nodesPending") List<UUID> nodesPending,
                                  @JsonProperty("nodesExecuting") List<UUID> nodesExecuting,
                                  @JsonProperty("nodesSucceeded") List<UUID> nodesSucceeded,
                                  @JsonProperty("nodesFailed") List<UUID> nodesFailed,
                                  @JsonProperty("lastUpdate") String lastUpdate)
    {
        this.jobId = jobId;
        this.status = status;
        this.operation = operation;
        this.reason = reason;
        this.startTime = startTime;
        this.nodesPending = nodesPending;
        this.nodesExecuting = nodesExecuting;
        this.nodesSucceeded = nodesSucceeded;
        this.nodesFailed = nodesFailed;
        this.lastUpdate = lastUpdate;
    }

    /**
     * @return job id of operational job
     */
    @JsonProperty("jobId")
    public UUID jobId()
    {
        return jobId;
    }

    /**
     * @return status of the job
     */
    @JsonProperty("jobStatus")
    public OperationalJobStatus status()
    {
        return status;
    }

    /**
     * @return operation of the job
     */
    @JsonProperty("operation")
    public String operation()
    {
        return operation;
    }

    /**
     * @return reason for job failure
     */
    @JsonProperty("reason")
    public String reason()
    {
        return reason;
    }

    /**
     * @return the time the job execution started
     */
    @JsonProperty("startTime")
    public String startTime()
    {
        return startTime;
    }

    /**
     * @return list of node IDs pending execution
     */
    @JsonProperty("nodesPending")
    public List<UUID> nodesPending()
    {
        return nodesPending;
    }

    /**
     * @return list of node IDs currently executing
     */
    @JsonProperty("nodesExecuting")
    public List<UUID> nodesExecuting()
    {
        return nodesExecuting;
    }

    /**
     * @return list of node IDs that have succeeded
     */
    @JsonProperty("nodesSucceeded")
    public List<UUID> nodesSucceeded()
    {
        return nodesSucceeded;
    }

    /**
     * @return list of node IDs that have failed
     */
    @JsonProperty("nodesFailed")
    public List<UUID> nodesFailed()
    {
        return nodesFailed;
    }

    /**
     * @return a human-readable status message
     */
    @JsonProperty("lastUpdate")
    public String lastUpdate()
    {
        return lastUpdate;
    }
}
