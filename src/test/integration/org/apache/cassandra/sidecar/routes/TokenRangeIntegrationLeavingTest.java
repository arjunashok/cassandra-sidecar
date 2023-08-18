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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster shrink scenarios integration tests for token range replica mapping endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class TokenRangeIntegrationLeavingTest extends BaseTokenRangeIntegrationTest
{
    @CassandraIntegrationTest(nodesPerDc = 4, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithKeyspaceLeavingNode(VertxTestContext context,
                                                ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               1,
                               BBHelperSingleLeavingNode::install,
                               BBHelperSingleLeavingNode.TRANSIENT_STATE_START,
                               BBHelperSingleLeavingNode.TRANSIENT_STATE_END);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithMultipleLeavingNodes(VertxTestContext context,
                                                 ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               2,
                               BBHelperMultipleLeavingNodes::install,
                               BBHelperMultipleLeavingNodes.TRANSIENT_STATE_START,
                               BBHelperMultipleLeavingNodes.TRANSIENT_STATE_END);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, network = true, gossip = true, buildCluster = false)
    void retrieveMappingHalveClusterSize(VertxTestContext context,
                                         ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               3,
                               BBHelperHalveClusterSize::install,
                               BBHelperHalveClusterSize.TRANSIENT_STATE_START,
                               BBHelperHalveClusterSize.TRANSIENT_STATE_END);
    }

    @CassandraIntegrationTest(
    nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithLeavingNodesMultiDC(VertxTestContext context,
                                                ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {

        int leavingNodesPerDC = 1;
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int numNodes = annotation.nodesPerDc() + annotation.newNodesPerDc();
        UpgradeableCluster cluster = getMultiDCCluster(numNodes,
                                                       annotation.numDcs(),
                                                       BBHelperLeavingNodesMultiDC::install,
                                                       cassandraTestContext);

        runLeavingTestScenario(context,
                               leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.TRANSIENT_STATE_START,
                               BBHelperLeavingNodesMultiDC.TRANSIENT_STATE_END,
                               cluster);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void retrieveMappingMultiDCHalveClusterSize(VertxTestContext context,
                                                ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {

        int leavingNodesPerDC = 3;
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int numNodes = annotation.nodesPerDc() + annotation.newNodesPerDc();
        UpgradeableCluster cluster = getMultiDCCluster(numNodes,
                                                       annotation.numDcs(),
                                                       BBHelperHalveClusterMultiDC::install,
                                                       cassandraTestContext);

        runLeavingTestScenario(context,
                               leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.TRANSIENT_STATE_START,
                               BBHelperHalveClusterMultiDC.TRANSIENT_STATE_END,
                               cluster);
    }

    void runLeavingTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                int leavingNodesPerDC,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd) throws Exception
    {

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(
        builder -> builder.withInstanceInitializer(instanceInitializer));

        runLeavingTestScenario(context,
                               leavingNodesPerDC,
                               transientStateStart,
                               transientStateEnd,
                               cluster);
    }

    void runLeavingTestScenario(VertxTestContext context,
                                int leavingNodesPerDC,
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

            List<IUpgradeableInstance> leavingNodes = new ArrayList<>();
            for (int i = 0; i < leavingNodesPerDC * annotation.numDcs(); i++)
            {
                IUpgradeableInstance node = cluster.get(cluster.size() - i);
                new Thread(() -> node.nodetoolResult("decommission").asserts().success()).start();
                leavingNodes.add(node);
            }

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(transientStateStart);

            for (IUpgradeableInstance node : leavingNodes)
            {
                ClusterUtils.awaitRingState(seed, node, "Leaving");
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

                List<Range<BigInteger>> expectedRanges = generateExpectedRanges(tokenSupplier, finalNodeCount);
                int initialNodeCount = annotation.nodesPerDc() * annotation.numDcs();
                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber ->
                                   nodeNumber <= (initialNodeCount - (leavingNodesPerDC * annotation.numDcs())) ?
                                   "Normal" :
                                   "Leaving");
                validateTokenRanges(mappingResponse, expectedRanges);
                validateReplicaMapping(mappingResponse, leavingNodes);

                context.completeNow();
            });
        }
        finally
        {
            for (int i = 0; i < leavingNodesPerDC; i++)
            {
                transientStateEnd.countDown();
            }
        }
    }

    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        List<IUpgradeableInstance> leavingNodes)
    {
        List<String> transientNodeAddresses = leavingNodes.stream().map(i -> {
            InetSocketAddress address = i.config().broadcastAddress();
            return address.getAddress().getHostAddress() +
                   ":" +
                   address.getPort();
        }).collect(Collectors.toList());

        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());
        assertThat(readReplicaInstances).containsAll(transientNodeAddresses);
        assertThat(writeReplicaInstances).containsAll(transientNodeAddresses);
    }
}
