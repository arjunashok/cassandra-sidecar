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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
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
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
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
                               BBHelperSingleJoiningNode.TRANSIENT_STATE_END,
                               generateExpectedRangeMappingSingleJoiningNode());
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
                               BBHelperMultipleJoiningNodes.TRANSIENT_STATE_END,
                               generateExpectedRangeMappingMultipleJoiningNodes());
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

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithDoubleClusterSize(VertxTestContext context,
                                              ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperDoubleClusterSize::install,
                               BBHelperDoubleClusterSize.TRANSIENT_STATE_START,
                               BBHelperDoubleClusterSize.TRANSIENT_STATE_END,
                               generateExpectedRangeMappingDoubleClusterSize());
    }

    @CassandraIntegrationTest(
    nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void retrieveMappingsSingleDCReplicatedKeyspace(VertxTestContext context,
                                                    ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int numNodes = annotation.nodesPerDc() + annotation.newNodesPerDc();
        UpgradeableCluster cluster = getMultiDCCluster(numNodes,
                                                       annotation.numDcs(),
                                                       BBHelperMultiDC::install,
                                                       cassandraTestContext);

        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperMultiDC.TRANSIENT_STATE_START,
                               BBHelperMultiDC.TRANSIENT_STATE_END,
                               cluster,
                               generateExpectedRanges(false),
                               generateExpectedRangeMappingOneof2DCs(),
                               false);
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
                               cluster,
                               generateExpectedRanges(),
                               generateExpectedRangeDoubleClusterSizeMultiDC(),
                               true);
    }

    void runJoiningTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    throws Exception
    {
        UpgradeableCluster cluster =
        cassandraTestContext
        .configureAndStartCluster(builder ->
                                  builder.withInstanceInitializer(instanceInitializer));

        runJoiningTestScenario(context,
                               cassandraTestContext,
                               transientStateStart,
                               transientStateEnd,
                               cluster,
                               generateExpectedRanges(),
                               expectedRangeMappings, true);
    }


    void runJoiningTestScenario(VertxTestContext context,
                                ConfigurableCassandraTestContext cassandraTestContext,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster,
                                List<Range<BigInteger>> expectedRanges,
                                Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings,
                                boolean isCrossDCKeyspace)
    throws Exception
    {
        try
        {
            CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;

            Set<String> dcReplication;
            if (annotation.numDcs() > 1 && isCrossDCKeyspace)
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
            // Go over new nodes and add them once for each DC
            for (int i = 0; i < cassandraTestContext.annotation.newNodesPerDc(); i++)
            {
                int dcNodeIdx = 1; // Use node 2's DC
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
                TokenSupplier tokenSupplier = MultiDcTokenSupplier.evenlyDistributedTokens(
                annotation.nodesPerDc() + annotation.newNodesPerDc(), annotation.numDcs(), 1);
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
                validateReplicaMapping(mappingResponse,
                                       newInstances,
                                       isCrossDCKeyspace,
                                       splitRanges,
                                       expectedRangeMappings);

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
                                        boolean isCrossDCKeyspace,
                                        List<Range<BigInteger>> splitRanges,
                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    {

        if (!isCrossDCKeyspace)
        {
            newInstances = newInstances.stream()
                                       .filter(i -> i.config().localDatacenter().equals("datacenter1"))
                                       .collect(Collectors.toList());
        }

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

        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings, isCrossDCKeyspace);
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

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the additional node joining the cluster
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * Ranges that include the joining node will have [RF + no. joining nodes in replica-set] replicas with
     * the replicas being the existing nodes in ring-order.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with E being the joining node)
     * Expected Range 2 - B, C, D, E
     */
    private HashMap<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingOneof2DCs()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(false);
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5")); // 2, 4, 6
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7")); // 2, 4, 6
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7")); // 4, 6, 8
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9")); // 4, 6, 8
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9")); // 6, 8, 10
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"    // 6, 8, 10
        , "127.0.0.11"));
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"    // 8, 10, 12
        , "127.0.0.11"));
        mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3"    // 8, 10, 12, 2
        , "127.0.0.11"));
        mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3"    // 10, 12 ,2, 4
        , "127.0.0.11"));
        mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"    // 10, 12 ,2, 4
        , "127.0.0.11"));
        mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"    // 10, 12 ,2, 4
        , "127.0.0.11"));
        mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5")); // 12 ,2, 4

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the additional node joining the cluster
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * Ranges that include the joining node will have [RF + no. joining nodes in replica-set] replicas with
     * the replicas being the existing nodes in ring-order.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with E being the joining node)
     * Expected Range 2 - B, C, D, E
     */
    private HashMap<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingSingleJoiningNode()
    {
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));

        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5"));
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1"
        , "127.0.0.6"));
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2"
        , "127.0.0.6"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"
        , "127.0.0.6"));
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 3 node cluster
     * with the 2 more nodes joining the cluster
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * We generate the expected ranges by using
     * 1) the initial token allocations to nodes (prior to adding nodes) shown under "Initial Ranges"
     * (in the comment block below),
     * 2)the "pending node ranges" and
     * 3) the final token allocations per node.
     *
     * Step 1: Prepare ranges starting from partitioner min-token, ending at partitioner max-token using (3) above
     * Step 2: Create the cascading list of replica-sets based on the RF (3) for each range using the initial node list
     * Step 3: Add replicas to ranges based on (1) and (2) above
     */

    private HashMap<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingMultipleJoiningNodes()
    {
        /*
         * All ranges previously had replicas 1, 2, 3, since this was a 3 node cluster with RF = 3
         *
         * Initial Ranges:
         * [-9223372036854775808, -5534023222112865485]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [-5534023222112865485, -1844674407370955163]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [-1844674407370955163, 1844674407370955159]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [1844674407370955159, 9223372036854775807]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         *
         * Pending ranges:
         * [-5534023222112865485, -1844674407370955163]=[127.0.0.4, 127.0.0.5]
         * [-1844674407370955163, 1844674407370955159]=[127.0.0.4, 127.0.0.5]
         * [1844674407370955159, 5534023222112865481]=[127.0.0.4]
         * [1844674407370955159, 9223372036854775803]=[127.0.0.5]
         *
         * Token assignment for new nodes:
         * 127.0.0.4 - [5534023222112865481]
         * 127.0.0.5 - [9223372036854775803]
         *
         * Based on the pending ranges, we add the expected replicas to the ranges they intersect below
         */
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.1", "127.0.0.4",
                                                         "127.0.0.5"));
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.1", "127.0.0.2", "127.0.0.4",
                                                         "127.0.0.5"));
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4",
                                                         "127.0.0.5"));
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
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
     * doubling in size
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * We generate the expected ranges by using
     * 1) the initial token allocations to nodes (prior to adding nodes) shown under "Initial Ranges"
     * (in the comment block below),
     * 2)the "pending node ranges" and
     * 3) the final token allocations per node.
     *
     * Step 1: Prepare ranges starting from partitioner min-token, ending at partitioner max-token using (3) above
     * Step 2: Create the cascading list of replica-sets based on the RF (3) for each range using the initial node list
     * Step 3: Add replicas to ranges based on (1) and (2) above.
     *
     */

    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingDoubleClusterSize()
    {

        /*
         *
         * Initial Ranges:
         * [-9223372036854775808, -5534023222112865485]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [-5534023222112865485, -1844674407370955163]:["127.0.0.3","127.0.0.2","127.0.0.4"]
         * [-1844674407370955163, 1844674407370955159]:["127.0.0.3","127.0.0.5","127.0.0.4"]
         * [1844674407370955159, 5534023222112865481]:["127.0.0.5","127.0.0.4","127.0.0.1"]
         * [5534023222112865481, 9223372036854775803]:["127.0.0.5","127.0.0.2","127.0.0.1"]
         * [9223372036854775803, 9223372036854775807]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         *
         * 127.0.0.1 at token -7378697629483820647
         * 127.0.0.2 at token -5534023222112865487
         * 127.0.0.3 at token -3689348814741910327
         * 127.0.0.4 at token -1844674407370955167
         * 127.0.0.5 at token -7
         * 127.0.0.6 at token 1844674407370955153
         * 127.0.0.7 at token 3689348814741910313
         * 127.0.0.8 at token 5534023222112865473
         * 127.0.0.9 at token 7378697629483820633
         * 127.0.0.10 at token 9223372036854775793
         *
         * Pending Ranges:
         * [-3689348814741910327, -1844674407370955167]=[127.0.0.6, 127.0.0.7, 127.0.0.8, 127.0.0.9,
         *                                              127.0.0.10]
         * [-1844674407370955167, -7]=[127.0.0.6, 127.0.0.7, 127.0.0.8, 127.0.0.9, 127.0.0.10]
         * [-7, 1844674407370955153]=[127.0.0.6] - 6 - 10
         * [-7, 3689348814741910313]=[127.0.0.7] - 7 - 10 (1 - 3)
         * [-7, 5534023222112865473]=[127.0.0.8] - 8 - 10 (3 - 5)
         * [-7, 7378697629483820633]=[127.0.0.9] - 9, 10 (5 - 7)
         * [-7, 9223372036854775793]=[127.0.0.10] - 10 (7 - 9)
         *
         */

        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        mapping.put(expectedRanges.get(0),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        mapping.put(expectedRanges.get(1),
                    Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        mapping.put(expectedRanges.get(2),
                    Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5"));
        // Nodes 6 - 10 are added to the existing replica-set from the pending ranges containing this exact range
        mapping.put(expectedRanges.get(3),
                    Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.6",
                                  "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10"));
        // Nodes 6 - 10 are added to the existing replica-set from the pending ranges containing this exact range
        mapping.put(expectedRanges.get(4),
                    Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.6",
                                  "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10"));
        // Nodes 6 - 10 are added to the existing replica-set from the pending ranges containing this sub-range
        mapping.put(expectedRanges.get(5),
                    Arrays.asList("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9",
                                  "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        // Nodes 7 - 10 are added to the existing replica-set from the pending ranges containing this sub-range
        mapping.put(expectedRanges.get(6),
                    Arrays.asList("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10",
                                  "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        // Nodes 8 - 10 are added to the existing replica-set from the pending ranges containing this sub-range
        mapping.put(expectedRanges.get(7),
                    Arrays.asList("127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3",
                                  "127.0.0.8", "127.0.0.9"));
        // Nodes 9, 10 are added to the existing replica-set from the pending ranges containing this sub-range
        mapping.put(expectedRanges.get(8),
                    Arrays.asList("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2",
                                  "127.0.0.3"));
        // Node 10 is added to the existing replica-set from the pending ranges containing this sub-range
        mapping.put(expectedRanges.get(9),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.10"));
        // Un-wrapped wrap-around range with the nodes in the initial range
        mapping.put(expectedRanges.get(10),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
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
     * We generate the expected ranges by using
     * 1) the initial token allocations to nodes (prior to adding nodes) shown under "Initial Ranges"
     * (in the comment block below),
     * 2)the "pending node ranges" and
     * 3) the final token allocations per node.
     *
     * Step 1: Prepare ranges starting from partitioner min-token, ending at partitioner max-token using (3) above
     * Step 2: Create the cascading list of replica-sets based on the RF (3) for each range using the initial node list
     * Step 3: Add replicas to ranges based on (1) and (2) above
     */

    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeDoubleClusterSizeMultiDC()
    {
        /*
         * Initial Ranges:
         * [-9223372036854775808", "-6148914691236517207"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-6148914691236517207", "-6148914691236517206"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-6148914691236517206", "-3074457345618258607"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-3074457345618258607", "-3074457345618258606"]:["127.0.0.5","127.0.0.3","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-3074457345618258606", "-7"]:["127.0.0.5","127.0.0.3","127.0.0.1", "127.0.0.6",
         *                                                  "127.0.0.2","127.0.0.4"]
         * [-7", "-6"]:["127.0.0.3","127.0.0.5","127.0.0.1", "127.0.0.6","127.0.0.2",
         *                                                  "127.0.0.4"]
         * [-6", "9223372036854775807"]:["127.0.0.3","127.0.0.5","127.0.0.1", "127.0.0.6",
         *                                                  "127.0.0.2","127.0.0.4:]
         *
         *  Node tokens:
         * 127.0.0.1 at token -6148914691236517207
         * 127.0.0.2 at token -6148914691236517206
         * 127.0.0.3 at token -3074457345618258607
         * 127.0.0.4 at token -3074457345618258606
         * 127.0.0.5 at token -7
         * 127.0.0.6 at token -6
         * 127.0.0.7 at token 3074457345618258593
         * 127.0.0.8 at token 3074457345618258594
         * 127.0.0.9 at token 6148914691236517193
         * 127.0.0.10 at token 6148914691236517194
         * 127.0.0.11 at token 9223372036854775793
         * 127.0.0.12 at token 9223372036854775794
         *
         *
         * Pending Ranges:
        * [-6, 6148914691236517194]=[127.0.0.10]
        * [-7, -6]=[127.0.0.12, 127.0.0.7, 127.0.0.8, 127.0.0.9, 127.0.0.10, 127.0.0.11]
        * [-6, 3074457345618258594]=[127.0.0.8]
        * [-6, 6148914691236517193]=[127.0.0.9]
        * [-3074457345618258607, -3074457345618258606]=[127.0.0.12, 127.0.0.7, 127.0.0.8, 127.0.0.9,
        *                                               127.0.0.10, 127.0.0.11]
        * [-3074457345618258606, -7]=[127.0.0.12, 127.0.0.7, 127.0.0.8, 127.0.0.9, 127.0.0.10,
        *                                               127.0.0.11]
        * [-6148914691236517207, -6148914691236517206]=[127.0.0.7, 127.0.0.9, 127.0.0.11], DONE
        * [-6148914691236517206, -3074457345618258607]=[127.0.0.12, 127.0.0.7, 127.0.0.8, 127.0.0.9,
        *                                               127.0.0.10, 127.0.0.11]
        * [-6, 3074457345618258593]=[127.0.0.7]
        * [-6, 9223372036854775793]=[127.0.0.11]
        * [-6, 9223372036854775794]=[127.0.0.12]
        *
        */

        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();

        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.1", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.1", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));
        // From pending ranges - nodes 7-12 from exact range
        dc1Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.3", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));
        // From pending ranges, adds nodes 7-12 from exact range
        dc1Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.1",
                                                            "127.0.0.3", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.6", "127.0.0.2",
                                                            "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));
        // From pending ranges, adds nodes 7-12 from exact range
        dc1Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));
        // From pending ranges - adds nodes 7, 8, 9, 10 and 12
        dc1Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));
        // From pending ranges - node 10, 11 added to subrange
        // Nodes 2,4,6 were initially part of the range
        dc1Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.9", "127.0.0.11", "127.0.0.1",
                                                            "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.12",
                                                            "127.0.0.6", "127.0.0.2", "127.0.0.4"));
        // From pending ranges - nodes 9, 10, 11 added to subrange
        // Nodes 2-6 were initially part of the range
        dc1Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.9", "127.0.0.11", "127.0.0.1", "127.0.0.3",
                                                            "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.10", "127.0.0.12", "127.0.0.2", "127.0.0.6",
                                                            "127.0.0.4"));
        // From pending ranges - node 12 added to subrange
        // Nodes 4, 5, 6 were initially part of the range
        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.11", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.10", "127.0.0.12", "127.0.0.2", "127.0.0.6",
                                                            "127.0.0.4"));
        // Nodes 5, 6 were initially part of the range
        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.11", "127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.12", "127.0.0.2", "127.0.0.4", "127.0.0.6"));
        // Node 6 was initially part of the range
        dc1Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.12", "127.0.0.2", "127.0.0.4", "127.0.0.6"));

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

    /**
     * ByteBuddy helper for a single joining node
     */
    @Shared
    public static class BBHelperSingleJoiningNode
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with 1 joining node
            // We intercept the bootstrap of the leaving node (4) to validate token ranges
            if (nodeNumber == 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperSingleJoiningNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    @Shared
    public static class BBHelperMultipleJoiningNodes
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with a 2 joining nodes
            // We intercept the joining of nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultipleJoiningNodes.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for doubling cluster size
     */
    @Shared
    public static class BBHelperDoubleClusterSize
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(5);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(5);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster doubling in size
            // We intercept the bootstrap of the new nodes (6-10) to validate token ranges
            if (nodeNumber > 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperDoubleClusterSize.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    @Shared
    public static class BBHelperDoubleClusterMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(6);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves doubling the size of a 6 node cluster (3 per DC)
            // We intercept the bootstrap of nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperDoubleClusterMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    @Shared
    public static class BBHelperMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves adding 2 nodes to a 10 node cluster (5 per DC)
            // We intercept the bootstrap of nodes (11,12) to validate token ranges
            if (nodeNumber > 10)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }
}
