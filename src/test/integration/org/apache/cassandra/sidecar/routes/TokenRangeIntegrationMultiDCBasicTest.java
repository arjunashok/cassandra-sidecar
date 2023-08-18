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

package org.apache.cassandra.sidecar.routes;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the token range replica mapping endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class TokenRangeIntegrationMultiDCBasicTest extends BaseTokenRangeIntegrationTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2)
    void retrieveMappingSingleDCReplicatedRf3(VertxTestContext context)
    throws Exception
    {
        int replicationFactor = 3;
        createTestKeyspace(ImmutableMap.of("datacenter1", replicationFactor));
        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
            assertMappingResponseOK(mappingResponse, replicationFactor, Collections.singleton("datacenter1"));
            context.completeNow();
        });
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2)
    void retrieveMappingMultiDcRf3(VertxTestContext context) throws Exception
    {
        int replicationFactor = 3;
        createTestKeyspace(ImmutableMap.of("replication_factor", replicationFactor));
        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
            // the keyspace is replicated to both DCs
            assertMappingResponseOK(mappingResponse,
                                    replicationFactor,
                                    Sets.newHashSet(Arrays.asList("datacenter1", "datacenter2")));
            context.completeNow();
        });
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }
}
