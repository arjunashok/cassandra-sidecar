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

package org.apache.cassandra.sidecar.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.db.schema.RestoreJobsSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;

/**
 * RestoreJobs is the data accessor to Cassandra.
 * It encapsulates the CRUD operations for RestoreJob
 */
@Singleton
public class RestoreJobDatabaseAccessor extends DatabaseAccessor
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final RestoreJobsSchema restoreJobsSchema;

    @Inject
    public RestoreJobDatabaseAccessor(SidecarSchema sidecarSchema,
                                      RestoreJobsSchema restoreJobsSchema,
                                      CQLSessionProvider cqlSessionProvider)
    {
        super(sidecarSchema, cqlSessionProvider);
        this.restoreJobsSchema = restoreJobsSchema;
    }

    public RestoreJob create(CreateRestoreJobRequestPayload payload, QualifiedTableName qualifiedTableName)
    throws DataObjectMappingException
    {
        sidecarSchema.ensureInitialized();

        UUID jobIdFromRequest = payload.jobId();
        UUID jobId = jobIdFromRequest == null ? UUIDs.timeBased() : jobIdFromRequest;
        RestoreJob job = RestoreJob.builder()
                                   .createdAt(RestoreJob.toLocalDate(jobId))
                                   .jobId(jobId)
                                   .keyspace(qualifiedTableName.keyspace())
                                   .table(qualifiedTableName.tableName())
                                   .jobAgent(payload.jobAgent())
                                   .jobStatus(RestoreJobStatus.CREATED)
                                   .jobSecrets(payload.secrets())
                                   .sstableImportOptions(payload.importOptions())
                                   .expireAt(payload.expireAtAsDate())
                                   .build();
        ByteBuffer secrets = serializeValue(job.secrets, "secrets");
        ByteBuffer importOptions = serializeValue(job.importOptions, "sstable import options");
        BoundStatement statement = restoreJobsSchema.insertJob()
                                                    .bind(job.createdAt,
                                                          job.jobId,
                                                          job.keyspaceName,
                                                          job.tableName,
                                                          job.jobAgent,
                                                          job.status.toString(),
                                                          secrets,
                                                          importOptions,
                                                          job.expireAt);

        execute(statement);
        return job;
    }

    public RestoreJob update(UpdateRestoreJobRequestPayload payload, QualifiedTableName qualifiedTableName, UUID jobId)
    throws DataObjectMappingException
    {
        sidecarSchema.ensureInitialized();
        LocalDate createdAt = RestoreJob.toLocalDate(jobId);

        RestoreJobSecrets secrets = payload.secrets();
        RestoreJobStatus status = payload.status();
        String jobAgent = payload.jobAgent();
        Date expireAt = payload.expireAtAsDate();
        // all updates are going to the same partition. We use unlogged explicitly.
        // Cassandra internally combine those updates into the same mutation.
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        ByteBuffer wrappedSecrets;
        if (secrets != null)
        {
            try
            {
                byte[] secretBytes = MAPPER.writeValueAsBytes(secrets);
                wrappedSecrets = ByteBuffer.wrap(secretBytes);
                batchStatement.add(restoreJobsSchema.updateBlobSecrets()
                                                    .bind(createdAt, jobId, wrappedSecrets));
            }
            catch (JsonProcessingException e)
            {
                throw new DataObjectMappingException("Failed to serialize secrets", e);
            }
        }
        if (status != null)
        {
            batchStatement.add(restoreJobsSchema.updateStatus().bind(createdAt, jobId, status.name()));
        }
        if (jobAgent != null)
        {
            batchStatement.add(restoreJobsSchema.updateJobAgent().bind(createdAt, jobId, jobAgent));
        }
        if (expireAt != null)
        {
            batchStatement.add(restoreJobsSchema.updateExpireAt().bind(createdAt, jobId, expireAt));
        }

        execute(batchStatement);
        return RestoreJob.forUpdates(jobId, jobAgent, status, secrets, expireAt);
    }

    public void abort(UUID jobId)
    {
        sidecarSchema.ensureInitialized();

        LocalDate createdAt = RestoreJob.toLocalDate(jobId);
        BoundStatement statement = restoreJobsSchema.updateStatus()
                                                    .bind(createdAt, jobId, RestoreJobStatus.ABORTED.name());
        execute(statement);
    }

    public RestoreJob find(UUID jobId)
    {
        sidecarSchema.ensureInitialized();

        BoundStatement statement = restoreJobsSchema.selectJob().bind(RestoreJob.toLocalDate(jobId), jobId);
        ResultSet resultSet = execute(statement);
        Row row = resultSet.one();
        if (row == null)
        {
            return null;
        }

        return RestoreJob.from(row);
    }

    public boolean exists(UUID jobId)
    {
        return find(jobId) != null;
    }

    /**
     * Find all restore jobs created in a day
     * @param date creation date of the jobs
     * @return the list of restore jobs in that day
     */
    public List<RestoreJob> findAllByCreationDate(LocalDate date)
    {
        sidecarSchema.ensureInitialized();

        BoundStatement statement = restoreJobsSchema.findAllByCreatedAt().bind(date);
        ResultSet resultSet = execute(statement);
        List<RestoreJob> result = new ArrayList<>();
        for (Row row : resultSet)
        {
            if (resultSet.getAvailableWithoutFetching() == 100 && !resultSet.isFullyFetched())
            {
                // trigger an async fetch sooner when there are more to fetch,
                // and it still has around 100 available to consume from the resultSet
                resultSet.fetchMoreResults();
            }
            result.add(RestoreJob.from(row));
        }
        return result;
    }

    /**
     * Find all the recent restore jobs
     * @param days number of days to search back; the value should be non-negative.
     * @return the list of recent restore job
     *
     * Note that in the implementation, one extra day is considered to overcome the timezone differences.
     */
    public List<RestoreJob> findAllRecent(int days)
    {
        Preconditions.checkArgument(days >= 0,
                                    "Input days cannot be negative. We can only look up the created jobs");
        if (days > 10)
        {
            logger.warn("Potentially collecting too many restore jobs. numberOfRecentDays={}", days);
        }

        // Add an extra day to avoid skipping restore jobs unexpectedly. For details, see method #dateInPast(int)
        int actualDays = days + 1;
        List<RestoreJob> result = new ArrayList<>();
        // add the jobs in the chronicle order
        for (int i = actualDays; i >= 0; i--)
        {
            result.addAll(findAllByCreationDate(dateInPast(i)));
        }
        return result;
    }

    // Returns the localDate that is relative to number of days in the past. If the value of days is 0, it is today!
    // Note that the method is implemented based on UTC.
    // It could happen that a time is already in yesterday according to UTC,
    // but in fact the same day in local timezone,
    // or, the other way around, depending on the geographic location (i.e. different timezones).
    // Example 1. 23:01 UTC is 00:01 CET (UTC +1) of the next day.
    // Example 2. 00:01 UTC of the next day is 17:01 PST (UTC -8)
    private LocalDate dateInPast(int days)
    {
        long now = System.currentTimeMillis();
        long delta = TimeUnit.DAYS.toMillis(days);
        return LocalDate.fromMillisSinceEpoch(now - delta);
    }

    private static <T> ByteBuffer serializeValue(T value, String type)
    {
        byte[] bytes;
        try
        {
            bytes = MAPPER.writeValueAsBytes(value);
        }
        catch (JsonProcessingException e)
        {
            throw new DataObjectMappingException("Failed to serialize " + type, e);
        }
        return ByteBuffer.wrap(bytes);
    }
}