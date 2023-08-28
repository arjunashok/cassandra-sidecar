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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Range;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.IntegrationTestBase;
import org.apache.cassandra.sidecar.adapters.base.Partitioner;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.AbstractCassandraTestContext;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the token range replica mapping endpoint with cassandra container.
 */
public class BaseTokenRangeIntegrationTest extends IntegrationTestBase
{

    protected void validateTokenRanges(TokenRangeReplicasResponse mappingsResponse,
                                       List<Range<BigInteger>> expectedRanges)
    {
        List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicaSet = mappingsResponse.writeReplicas();
        List<TokenRangeReplicasResponse.ReplicaInfo> readReplicaSet = mappingsResponse.readReplicas();
        List<Range<BigInteger>> writeRanges = writeReplicaSet.stream()
                                                             .map(r -> Range.openClosed(new BigInteger(r.start()),
                                                                                        new BigInteger(r.end())))
                                                             .collect(Collectors.toList());

        List<Range<BigInteger>> readRanges = readReplicaSet.stream()
                                                           .map(r -> Range.openClosed(new BigInteger(r.start()),
                                                                                      new BigInteger(r.end())))
                                                           .collect(Collectors.toList());


        assertThat(writeRanges.size()).isEqualTo(writeReplicaSet.size());
        assertThat(writeRanges).containsExactlyElementsOf(expectedRanges);

        //Sorted and Overlap check
        validateOrderAndOverlaps(writeRanges);
        validateOrderAndOverlaps(readRanges);
    }

    private void validateOrderAndOverlaps(List<Range<BigInteger>> ranges)
    {
        for (int r = 0; r < ranges.size() - 1; r++)
        {
            assertThat(ranges.get(r).upperEndpoint()).isLessThan(ranges.get(r + 1).upperEndpoint());
            assertThat(ranges.get(r).intersection(ranges.get(r + 1)).isEmpty()).isTrue();
        }
    }

    protected void validateNodeStates(TokenRangeReplicasResponse mappingResponse,
                                      Set<String> dcReplication,
                                      Function<Integer, String> statusFunction)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int expectedReplicas = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * dcReplication.size();

        AbstractCassandraTestContext cassandraTestContext = sidecarTestContext.cassandraTestContext();
        assertThat(mappingResponse.replicaState().size()).isEqualTo(expectedReplicas);
        for (int i = 1; i <= cassandraTestContext.cluster().size(); i++)
        {
            IInstanceConfig config = cassandraTestContext.cluster().get(i).config();

            if (dcReplication.contains(config.localDatacenter()))
            {
                String ipAndPort = config.broadcastAddress().getAddress().getHostAddress() + ":"
                                   + config.broadcastAddress().getPort();

                String expectedStatus = statusFunction.apply(i);
                assertThat(mappingResponse.replicaState().get(ipAndPort)).isEqualTo(expectedStatus);
            }
        }
    }

    protected UpgradeableCluster getMultiDCCluster(int numNodes,
                                                   int numDcs,
                                                   BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext)
    throws IOException
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier mdcTokenSupplier =
        MultiDcTokenSupplier.evenlyDistributedTokens(numNodes,
                                                     numDcs,
                                                     1);

        int totalNodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
        return cassandraTestContext.configureAndStartCluster(
        builder -> {
            builder.withInstanceInitializer(initializer);
            builder.withTokenSupplier(mdcTokenSupplier);
            builder.withNodeIdTopology(networkTopology(totalNodeCount,
                                                       (nodeId) -> nodeId % 2 != 0 ?
                                                                   dcAndRack("datacenter1", "rack1") :
                                                                   dcAndRack("datacenter2", "rack2")));
        });
    }

    protected List<Range<BigInteger>> generateExpectedRanges()
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int nodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
        return generateExpectedRanges(nodeCount);
    }

    protected List<Range<BigInteger>> generateExpectedRanges(int nodeCount)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = (annotation.numDcs() > 1) ?
                                      MultiDcTokenSupplier.evenlyDistributedTokens(
                                      annotation.nodesPerDc() + annotation.newNodesPerDc(),
                                      annotation.numDcs(),
                                      1) :
                                      TokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc() +
                                                                            annotation.newNodesPerDc(),
                                                                            1);

        List<Range<BigInteger>> expectedRanges = new ArrayList<>();
        BigInteger startToken = Partitioner.Murmur3.minToken;
        BigInteger endToken = Partitioner.Murmur3.maxToken;
        int node = 1;
        BigInteger prevToken = new BigInteger(tokenSupplier.tokens(node++).stream().findFirst().get());
        Range<BigInteger> firstRange = Range.openClosed(startToken, prevToken);
        expectedRanges.add(firstRange);
        while (node <= nodeCount)
        {
            BigInteger currentToken = new BigInteger(tokenSupplier.tokens(node).stream().findFirst().get());
            expectedRanges.add(Range.openClosed(prevToken, currentToken));
            prevToken = currentToken;
            node++;
        }
        expectedRanges.add(Range.openClosed(prevToken, endToken));
        return expectedRanges;
    }

    protected Set<String> instancesFromReplicaSet(List<TokenRangeReplicasResponse.ReplicaInfo> replicas)
    {
        return replicas.stream()
                       .flatMap(r -> r.replicasByDatacenter().values().stream())
                       .flatMap(Collection::stream)
                       .collect(Collectors.toSet());
    }

    protected void validateWriteReplicaMappings(List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicas,
                                              Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMapping)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        assertThat(writeReplicas).hasSize(expectedRangeMapping.get("datacenter1").size());
        for (TokenRangeReplicasResponse.ReplicaInfo r: writeReplicas)
        {
            Range<BigInteger> range = Range.openClosed(BigInteger.valueOf(Long.parseLong(r.start())),
                                                       BigInteger.valueOf(Long.parseLong(r.end())));
            assertThat(expectedRangeMapping).containsKey("datacenter1");
            assertThat(expectedRangeMapping.get("datacenter1")).containsKey(range);
            // Replicaset for the same range match expected
            assertThat(r.replicasByDatacenter().get("datacenter1"))
            .containsExactlyInAnyOrderElementsOf(expectedRangeMapping.get("datacenter1").get(range));

            if (annotation.numDcs() > 1)
            {
                assertThat(expectedRangeMapping).containsKey("datacenter2");
                assertThat(expectedRangeMapping.get("datacenter2")).containsKey(range);
                assertThat(r.replicasByDatacenter().get("datacenter2"))
                .containsExactlyInAnyOrderElementsOf(expectedRangeMapping.get("datacenter2").get(range));
            }
        }
    }

    void retrieveMappingWithKeyspace(VertxTestContext context, String keyspace,
                                     Handler<HttpResponse<Buffer>> verifier) throws Exception
    {
        String testRoute = "/api/v1/keyspaces/" + keyspace + "/token-range-replicas";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .send(context.succeeding(verifier));
        });
    }

    void assertMappingResponseOK(TokenRangeReplicasResponse mappingResponse)
    {
        assertMappingResponseOK(mappingResponse, 1, Collections.singleton("datacenter1"));
    }

    void assertMappingResponseOK(TokenRangeReplicasResponse mappingResponse,
                                 int replicationFactor,
                                 Set<String> dcReplication)
    {
        assertThat(mappingResponse).isNotNull();
        assertThat(mappingResponse.readReplicas()).isNotNull();
        assertThat(mappingResponse.writeReplicas()).isNotNull();
        TokenRangeReplicasResponse.ReplicaInfo readReplica = mappingResponse.readReplicas().get(0);
        assertThat(readReplica.replicasByDatacenter()).isNotNull().hasSize(dcReplication.size());
        TokenRangeReplicasResponse.ReplicaInfo writeReplica = mappingResponse.writeReplicas().get(0);
        assertThat(writeReplica.replicasByDatacenter()).isNotNull().hasSize(dcReplication.size());

        for (String dcName : dcReplication)
        {
            assertThat(readReplica.replicasByDatacenter().keySet()).isNotEmpty().contains(dcName);
            assertThat(readReplica.replicasByDatacenter().get(dcName)).isNotNull().hasSize(replicationFactor);

            assertThat(writeReplica.replicasByDatacenter().keySet()).isNotEmpty().contains(dcName);
            assertThat(writeReplica.replicasByDatacenter().get(dcName)).isNotNull();
            assertThat(writeReplica.replicasByDatacenter().get(dcName).size())
            .isGreaterThanOrEqualTo(replicationFactor);
        }
    }
}
