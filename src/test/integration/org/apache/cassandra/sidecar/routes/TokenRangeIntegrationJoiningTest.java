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

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster expansion scenarios integration tests for token range replica mapping endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class TokenRangeIntegrationJoiningTest extends BaseTokenRangeIntegrationTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithJoiningNode(VertxTestContext context,
                                        ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperSingleJoiningNode::install,
                               BBHelperSingleJoiningNode.TRANSIENT_STATE_START,
                               BBHelperSingleJoiningNode.TRANSIENT_STATE_END);
    }

    @CassandraIntegrationTest(
    nodesPerDc = 3, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithJoiningNodesMultiDC(VertxTestContext context,
                                                ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int numNodes = annotation.nodesPerDc() + annotation.newNodesPerDc();
        UpgradeableCluster cluster = getMultiDCCluster(numNodes,
                                                       annotation.numDcs(),
                                                       BBHelperJoiningNodesMultiDC::install,
                                                       cassandraTestContext);

        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperJoiningNodesMultiDC.TRANSIENT_STATE_START,
                               BBHelperJoiningNodesMultiDC.TRANSIENT_STATE_END,
                               cluster);
    }

    @CassandraIntegrationTest(
    nodesPerDc = 3, newNodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void retrieveMappingsDoubleClusterSizeMultiDC(VertxTestContext context,
                                                  ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int numNodes = annotation.nodesPerDc() + annotation.newNodesPerDc();
        UpgradeableCluster cluster = getMultiDCCluster(numNodes,
                                                       annotation.numDcs(),
                                                       BBHelperDoubleClusterMultiDC::install,
                                                       cassandraTestContext);

        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperDoubleClusterMultiDC.TRANSIENT_STATE_START,
                               BBHelperDoubleClusterMultiDC.TRANSIENT_STATE_END,
                               cluster);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 2, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithMultipleJoiningNodes(VertxTestContext context,
                                                 ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperMultipleJoiningNodes::install,
                               BBHelperMultipleJoiningNodes.TRANSIENT_STATE_START,
                               BBHelperMultipleJoiningNodes.TRANSIENT_STATE_END);
    }


    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithDoubleClusterSize(VertxTestContext context,
                                              ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperDoubleClusterSize::install,
                               BBHelperDoubleClusterSize.TRANSIENT_STATE_START,
                               BBHelperDoubleClusterSize.TRANSIENT_STATE_END);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, gossip = true, network = true)
    void retrieveMappingWithKeyspaceWithAddNode(VertxTestContext context) throws Exception
    {
        createTestKeyspace(ImmutableMap.of("replication_factor", DEFAULT_RF));
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        IUpgradeableInstance instance = cluster.get(1);
        IUpgradeableInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                    instance.config().localDatacenter(),
                                                                    instance.config().localRack(),
                                                                    inst -> inst.with(Feature.NETWORK,
                                                                                      Feature.GOSSIP,
                                                                                      Feature.JMX,
                                                                                      Feature.NATIVE_PROTOCOL));
        cluster.get(4).startup(cluster);
        ClusterUtils.awaitRingState(instance, newInstance, "Normal");

        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertMappingResponseOK(mappingResponse, DEFAULT_RF, Collections.singleton("datacenter1"));
            context.completeNow();
        });
    }

    void runJoiningTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd) throws Exception
    {
        UpgradeableCluster cluster =
        cassandraTestContext
        .configureAndStartCluster(builder ->
                                  builder.withInstanceInitializer(instanceInitializer));

        runJoiningTestScenario(context, cassandraTestContext, transientStateStart, transientStateEnd, cluster);
    }


    void runJoiningTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster) throws Exception
    {
        try
        {
            CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;

            Set<String> dcReplication;
            if (annotation.numDcs() > 1)
            {
                createTestKeyspace(ImmutableMap.of("replication_factor", DEFAULT_RF));
                dcReplication = Sets.newHashSet(Arrays.asList("datacenter1", "datacenter2"));
            }
            else
            {
                createTestKeyspace(ImmutableMap.of("datacenter1", DEFAULT_RF));
                dcReplication = Collections.singleton("datacenter1");
            }

            IUpgradeableInstance seed = cluster.get(1);

            List<IUpgradeableInstance> newInstances = new ArrayList<>();
            for (int i = 0; i < cassandraTestContext.annotation.newNodesPerDc(); i++)
            {
                int dcNodeIdx = 2;
                for (int dc = 1; dc <= annotation.numDcs(); dc++)
                {
                    IUpgradeableInstance dcNode = cluster.get(dcNodeIdx++);
                    IUpgradeableInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                                dcNode.config().localDatacenter(),
                                                                                dcNode.config().localRack(),
                                                                                inst -> {
                                                                                    inst.set("auto_bootstrap", true);
                                                                                    inst.with(Feature.GOSSIP,
                                                                                              Feature.JMX,
                                                                                              Feature.NATIVE_PROTOCOL);
                                                                                });
                    new Thread(() -> newInstance.startup(cluster)).start();
                    newInstances.add(newInstance);
                }
            }

            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);

            for (IUpgradeableInstance newInstance : newInstances)
            {
                ClusterUtils.awaitRingState(seed, newInstance, "Joining");
            }

            retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
                assertMappingResponseOK(mappingResponse,
                                        DEFAULT_RF,
                                        dcReplication);
                int finalNodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
                TokenSupplier tokenSupplier = (annotation.numDcs() > 1) ?
                                              MultiDcTokenSupplier.evenlyDistributedTokens(
                                              annotation.nodesPerDc() + annotation.newNodesPerDc(),
                                              annotation.numDcs(),
                                              1) :
                                              TokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc() +
                                                                                    annotation.newNodesPerDc(),
                                                                                    1);

                List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
                // New split ranges resulting from joining nodes and corresponding tokens
                List<Range<BigInteger>> splitRanges = extractSplitRanges(annotation.newNodesPerDc() *
                                                                         annotation.numDcs(),
                                                                         finalNodeCount,
                                                                         tokenSupplier,
                                                                         expectedRanges);

                List<Integer> newNodes = newInstances.stream().map(i -> i.config().num()).collect(Collectors.toList());
                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber -> newNodes.contains(nodeNumber) ? "Joining" : "Normal");

                validateTokenRanges(mappingResponse, expectedRanges);
                validateReplicaMapping(mappingResponse, newInstances, splitRanges);

                context.completeNow();
            });
        }
        finally
        {
            for (int i = 0;
                 i < (cassandraTestContext.annotation.newNodesPerDc() * cassandraTestContext.annotation.numDcs()); i++)
            {
                transientStateEnd.countDown();
            }
        }
    }

    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        List<IUpgradeableInstance> newInstances,
                                        List<Range<BigInteger>> splitRanges)
    {
        List<String> transientNodeAddresses = newInstances.stream().map(i -> {
            InetSocketAddress address = i.config().broadcastAddress();
            return address.getAddress().getHostAddress() +
                   ":" +
                   address.getPort();
        }).collect(Collectors.toList());

        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());

        Set<String> splitRangeReplicas
        = mappingResponse.writeReplicas().stream()
                         .filter(w -> matchSplitRanges(w, splitRanges))
                         .map(r ->
                              r.replicasByDatacenter().values())
                         .flatMap(Collection::stream)
                         .flatMap(list -> list.stream())
                         .collect(Collectors.toSet());

        assertThat(readReplicaInstances).doesNotContainAnyElementsOf(transientNodeAddresses);
        // Validate that the new nodes are mapped to the split ranges
        assertThat(splitRangeReplicas).containsAll(transientNodeAddresses);
        assertThat(writeReplicaInstances).containsAll(transientNodeAddresses);
    }

    private List<Range<BigInteger>> extractSplitRanges(int newNodes,
                                                       int finalNodeCount,
                                                       TokenSupplier tokenSupplier,
                                                       List<Range<BigInteger>> expectedRanges)
    {

        int newNode = 1;
        List<BigInteger> newNodeTokens = new ArrayList<>();
        while (newNode <= newNodes)
        {
            int nodeIdx = finalNodeCount - newNode;
            newNodeTokens.add(new BigInteger(tokenSupplier.tokens(nodeIdx).stream().findFirst().get()));
            newNode++;
        }

        return expectedRanges.stream()
                             .filter(r -> newNodeTokens.contains(r.upperEndpoint()) ||
                                          newNodeTokens.contains(r.lowerEndpoint()))
                             .collect(Collectors.toList());
    }

    private boolean matchSplitRanges(TokenRangeReplicasResponse.ReplicaInfo range,
                                     List<Range<BigInteger>> expectedSplitRanges)
    {
        return expectedSplitRanges.stream()
                                  .anyMatch(s -> range.start().equals(s.lowerEndpoint().toString()) &&
                                                 range.end().equals(s.upperEndpoint().toString()));
    }
}
