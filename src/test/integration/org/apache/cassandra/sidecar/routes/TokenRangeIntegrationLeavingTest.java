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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
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
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cluster shrink scenarios integration tests for token range replica mapping endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class TokenRangeIntegrationLeavingTest extends BaseTokenRangeIntegrationTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithKeyspaceLeavingNode(VertxTestContext context,
                                                ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        runLeavingTestScenario(context,
                               cassandraTestContext,
                               1,
                               BBHelperSingleLeavingNode::install,
                               BBHelperSingleLeavingNode.TRANSIENT_STATE_START,
                               BBHelperSingleLeavingNode.TRANSIENT_STATE_END,
                               generateExpectedRangeMappingSingleLeavingNode());
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
                               BBHelperMultipleLeavingNodes.TRANSIENT_STATE_END,
                               generateExpectedRangeMappingMultipleLeavingNodes());
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
                               BBHelperHalveClusterSize.TRANSIENT_STATE_END,
                               generateExpectedRangeMappingHalveClusterSize());
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
                               cluster,
                               generateExpectedRangeMappingLeavingNodeMultiDC());
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

        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings
        = generateExpectedRangeHalveClusterSizeMultiDC();
        runLeavingTestScenario(context,
                               leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.TRANSIENT_STATE_START,
                               BBHelperHalveClusterMultiDC.TRANSIENT_STATE_END,
                               cluster,
                               expectedRangeMappings);
    }

    void runLeavingTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                int leavingNodesPerDC,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    throws Exception
    {

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(
        builder -> builder.withInstanceInitializer(instanceInitializer));

        runLeavingTestScenario(context,
                               leavingNodesPerDC,
                               transientStateStart,
                               transientStateEnd,
                               cluster,
                               expectedRangeMappings);
    }

    void runLeavingTestScenario(VertxTestContext context,
                                int leavingNodesPerDC,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    throws Exception
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

                int initialNodeCount = annotation.nodesPerDc() * annotation.numDcs();
                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber ->
                                   nodeNumber <= (initialNodeCount - (leavingNodesPerDC * annotation.numDcs())) ?
                                   "Normal" :
                                   "Leaving");
                validateTokenRanges(mappingResponse, generateExpectedRanges());
                validateReplicaMapping(mappingResponse, leavingNodes, expectedRangeMappings);

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

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the last node leaving the cluster
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
     */
    private HashMap<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingSingleLeavingNode()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));

        mapping.put(expectedRanges.get(2),
                    Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.1"));
        mapping.put(expectedRanges.get(3),
                    Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.2"));
        mapping.put(expectedRanges.get(4),
                    Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.3"));

        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the last 2 nodes leaving the cluster
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)

     */

    private HashMap<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingMultipleLeavingNodes()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1),
                    Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.1"));
        mapping.put(
        expectedRanges.get(2),
        Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.2", "127.0.0.1"));
        mapping.put(
        expectedRanges.get(3),
        Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(
        expectedRanges.get(4),
        Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 6 node cluster
     * with the last 3 nodes leaving the cluster
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)

     */

    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingHalveClusterSize()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(
        expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.1"));
        mapping.put(
        expectedRanges.get(2),
        Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.2"));
        mapping.put(
        expectedRanges.get(3),
        Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.1", "127.0.0.2",
                      "127.0.0.3"));
        mapping.put(
        expectedRanges.get(4),
        Arrays.asList("127.0.0.5", "127.0.0.6", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(
        expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 10 node cluster
     * across 2 DCs with the last 2 nodes leaving the cluster (1 per DC), with RF 3
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * In a multi-DC scenario, a single range will have nodes from both DCs. The replicas are grouped by DC here
     * to allow per-DC validation as returned from the sidecar endpoint.
     *
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     *
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
     */

    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingLeavingNodeMultiDC()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();

        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(3),
                       Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(4),
                       Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(4),
                       Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10", "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(5),
                       Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1", "127.0.0.3"));
        dc2Mapping.put(expectedRanges.get(5),
                       Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10", "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(6),
                       Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1", "127.0.0.3"));
        dc2Mapping.put(expectedRanges.get(6),
                       Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.2", "127.0.0.4"));

        dc1Mapping.put(expectedRanges.get(7),
                       Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(7),
                       Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.2", "127.0.0.4"));

        dc1Mapping.put(expectedRanges.get(8),
                       Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(8),
                       Arrays.asList("127.0.0.10", "127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(9),
                       Arrays.asList("127.0.0.10", "127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        Map<String, Map<Range<BigInteger>, List<String>>> multiDCMapping
        = new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", dc1Mapping);
                put("datacenter2", dc2Mapping);
            }
        };
        return multiDCMapping;
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 12 node cluster
     * across 2 DCs with the last 6 nodes leaving the cluster (3 per DC)
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * In a multi-DC scenario, a single range will have nodes from both DCs. The replicas are grouped by DC here
     * to allow per-DC validation as returned from the sidecar endpoint.
     *
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     *
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
     */

    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeHalveClusterSizeMultiDC()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();

        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(1),
                       Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(2),
                       Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(2),
                       Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8", "127.0.0.2"));

        dc1Mapping.put(
        expectedRanges.get(3),
        Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9", "127.0.0.1", "127.0.0.3"));
        dc2Mapping.put(expectedRanges.get(3),
                       Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8", "127.0.0.2"));

        dc1Mapping.put(
        expectedRanges.get(4),
        Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9", "127.0.0.1", "127.0.0.3"));
        dc2Mapping.put(
        expectedRanges.get(4),
        Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10", "127.0.0.2", "127.0.0.4"));

        dc1Mapping.put(
        expectedRanges.get(5),
        Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.11", "127.0.0.1", "127.0.0.3",
                      "127.0.0.5"));
        dc2Mapping.put(
        expectedRanges.get(5),
        Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10", "127.0.0.2", "127.0.0.4"));

        dc1Mapping.put(
        expectedRanges.get(6),
        Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.11", "127.0.0.1", "127.0.0.3",
                      "127.0.0.5"));
        dc2Mapping.put(
        expectedRanges.get(6),
        Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.12", "127.0.0.2", "127.0.0.4",
                      "127.0.0.6"));

        dc1Mapping.put(
        expectedRanges.get(7),
        Arrays.asList("127.0.0.9", "127.0.0.11", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(
        expectedRanges.get(7),
        Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.12", "127.0.0.2", "127.0.0.4",
                      "127.0.0.6"));

        dc1Mapping.put(
        expectedRanges.get(8),
        Arrays.asList("127.0.0.9", "127.0.0.11", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(
        expectedRanges.get(8),
        Arrays.asList("127.0.0.10", "127.0.0.12", "127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(9),
                       Arrays.asList("127.0.0.11", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(
        expectedRanges.get(9),
        Arrays.asList("127.0.0.10", "127.0.0.12", "127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(10),
                       Arrays.asList("127.0.0.11", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(10),
                       Arrays.asList("127.0.0.12", "127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(11),
                       Arrays.asList("127.0.0.12", "127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        Map<String, Map<Range<BigInteger>, List<String>>> multiDCMapping
        = new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", dc1Mapping);
                put("datacenter2", dc2Mapping);
            }
        };
        return multiDCMapping;
    }

    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        List<IUpgradeableInstance> leavingNodes,
                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
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

        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings);
    }

    /**
     * ByteBuddy Helper for a single leaving node
     */
    @Shared
    public static class BBHelperSingleLeavingNode
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with 1 leaving node
            // We intercept the shutdown of the leaving node (5) to validate token ranges
            if (nodeNumber == 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperSingleLeavingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes
     */
    @Shared
    public static class BBHelperMultipleLeavingNodes
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a 2 leaving nodes
            // We intercept the shutdown of the leaving nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperMultipleLeavingNodes.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for shrinking cluster by half its size
     */
    @Shared
    public static class BBHelperHalveClusterSize
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(3);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(3);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 6 node cluster
            // We intercept the shutdown of the removed nodes (4-6) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterSize.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes multi-DC
     */
    @Shared
    public static class BBHelperLeavingNodesMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 nodes per DC) with a 2 leaving nodes (1 per DC)
            // We intercept the shutdown of the leaving nodes (9, 10) to validate token ranges
            if (nodeNumber > 8)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperLeavingNodesMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for halve cluster size with multi-DC
     */
    @Shared
    public static class BBHelperHalveClusterMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(6);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 12 node cluster (6 per DC)
            // We intercept the shutdown of the removed nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }
}
