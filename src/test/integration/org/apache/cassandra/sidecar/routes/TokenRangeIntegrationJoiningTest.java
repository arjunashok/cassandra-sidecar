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
 * Cluster expansion scenarios integration tests for token range replica mapping endpoint with the in-jvm
 * dtest framework.
 */
@ExtendWith(VertxExtension.class)
public class TokenRangeIntegrationJoiningTest extends BaseTokenRangeIntegrationTest
{

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
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDC::install, cassandraTestContext);

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
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperDoubleClusterMultiDC::install, cassandraTestContext);

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

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(instanceInitializer);
            builder.withTokenSupplier(tokenSupplier);
        });

        runJoiningTestScenario(context,
                               cassandraTestContext,
                               transientStateStart,
                               transientStateEnd,
                               cluster,
                               generateExpectedRanges(),
                               expectedRangeMappings,
                               true);
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
                TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                        annotation.newNodesPerDc(),
                                                                                        annotation.numDcs(),
                                                                                        1);
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

        /*
         * Initial Ranges:
         * [-9223372036854775808, -5869418568907584607]=[127.0.0.1:52914, 127.0.0.3:52916, 127.0.0.5:52918]
         * [-5869418568907584607, -5869418568907584606]=[127.0.0.3:52916, 127.0.0.5:52918, 127.0.0.7:52920]
         * [-5869418568907584606, -2515465100960393407]=[127.0.0.3:52918, 127.0.0.5:52920, 127.0.0.7:52922]
         * [-2515465100960393407, -2515465100960393406]=[127.0.0.5:52918, 127.0.0.7:52920, 127.0.0.9:52922]
         * [-2515465100960393406, 838488366986797793]=[127.0.0.5:52918, 127.0.0.7:52920, 127.0.0.9:52922]
         * [838488366986797793, 838488366986797794]=[127.0.0.7:52920, 127.0.0.9:52922, 127.0.0.1:52914]
         * [838488366986797794, 4192441834933988993]=[127.0.0.7:52920, 127.0.0.9:52922, 127.0.0.1:52914]
         * [4192441834933988993, 4192441834933988994]=[127.0.0.9:52922, 127.0.0.1:52914, 127.0.0.3:52916]
         * [4192441834933988994, 7546395302881180193]=[127.0.0.9:52922, 127.0.0.1:52914, 127.0.0.3:52916]
         * [7546395302881180193, 7546395302881180194]=[127.0.0.1:52914, 127.0.0.3:52916, 127.0.0.5:52918]
         * [7546395302881180194, 9223372036854775807]=[127.0.0.1:52914, 127.0.0.3:52916, 127.0.0.5:52918]
         *
         * Pending Ranges:
         * [-5869418568907584607, -5869418568907584606]=[127.0.0.11:58400]
         * [-5869418568907584606, -4192441834933989006]=[127.0.0.11:58400]
         * [4192441834933988993, 4192441834933988994]=[127.0.0.11:58400]
         * [7546395302881180194, -5869418568907584607]=[127.0.0.11:58400] (wrap-around)
         * [7546395302881180193, 7546395302881180194]=[127.0.0.11:58400]
         * [4192441834933988994, 7546395302881180193]=[127.0.0.11:58400]
         *
         */
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(false);
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        // [-9223372036854775808, -5869418568907584607]
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.11"));
        // [-5869418568907584607, -5869418568907584606]
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7", "127.0.0.11"));
        // [-5869418568907584606, -4192441834933989006]
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7", "127.0.0.11"));
        // [-4192441834933989006, -2515465100960393407]
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7"));
        // [-2515465100960393407, -2515465100960393406]
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9"));
        // [-2515465100960393406, 838488366986797793]
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9"));
        // [838488366986797793, 838488366986797794]
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"));
        // [838488366986797794, 4192441834933988993]
        mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"));
        // [4192441834933988993, 4192441834933988994]
        mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3", "127.0.0.11"));
        // [4192441834933988994, 7546395302881180193]
        mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3", "127.0.0.11"));
        // [7546395302881180193, 7546395302881180194]
        mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.11"));
        // [7546395302881180194, 9223372036854775807]
        mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.11"));

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
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6"));
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.6"));

        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5"));
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.6"));
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6"));

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
         * [-9223372036854775808, -4611686018427387905]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [-4611686018427387905, -3]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [-3, 4611686018427387899]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [4611686018427387899, 9223372036854775807]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         *
         * Pending ranges:
         * [-3, 4611686018427387899]=[127.0.0.4:62469]
         * [-4611686018427387905, -3]=[127.0.0.5:62472]
         * [-4611686018427387905, -2305843009213693954]=[127.0.0.4:62469]
         * [4611686018427387899, -4611686018427387905]=[127.0.0.4:62469, 127.0.0.5:62472] (wrap-around)
         * [-3, 2305843009213693948]=[127.0.0.5:62472]
         *
         * Token assignment for new nodes:
         * 127.0.0.4 - [-2305843009213693954]
         * 127.0.0.5 - [2305843009213693948]
         *
         * Based on the pending ranges, we add the expected replicas to the ranges they intersect below
         */
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        // [-9223372036854775808, -4611686018427387905]
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4",
                                                         "127.0.0.5"));
        // [-4611686018427387905, -2305843009213693954]
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.1", "127.0.0.4",
                                                         "127.0.0.5"));
        // [-2305843009213693954, -3]
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.1", "127.0.0.2", "127.0.0.5"));
        // [-3, 2305843009213693948]
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4",
                                                         "127.0.0.5"));
        // [2305843009213693948, 4611686018427387899]
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.4", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        // [4611686018427387899, 9223372036854775807]
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4",
                                                         "127.0.0.5"));
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
         * [-9223372036854775808, -6148914691236517205]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         * [-6148914691236517205, -3074457345618258603]:["127.0.0.3","127.0.0.2","127.0.0.4"]
         * [-3074457345618258603, -1]:["127.0.0.3","127.0.0.5","127.0.0.4"]
         * [-1, 3074457345618258601]:["127.0.0.5","127.0.0.4","127.0.0.1"]
         * [3074457345618258601, 6148914691236517203]:["127.0.0.5","127.0.0.2","127.0.0.1"]
         * [6148914691236517203, 9223372036854775807]:["127.0.0.3","127.0.0.2","127.0.0.1"]
         *
         * New node tokens
         * 127.0.0.6 at token -4611686018427387904
         * 127.0.0.7 at token -1537228672809129302
         * 127.0.0.8 at token 1537228672809129300
         * 127.0.0.9 at token 4611686018427387902
         * 127.0.0.10 at token 7686143364045646504
         *
         * Pending Ranges:
         * [-3074457345618258603, -1]=[127.0.0.9:64060, 127.0.0.8:64055]
         * [6148914691236517203, -6148914691236517205]=[127.0.0.6:64047, 127.0.0.7:64050] (wrap-around)
         * [-6148914691236517205, -4611686018427387904]=[127.0.0.6:64047]
         * [6148914691236517203, 7686143364045646504]=[127.0.0.10:64068]
         * [-3074457345618258603, -1537228672809129302]=[127.0.0.7:64050]
         * [3074457345618258601, 6148914691236517203]=[127.0.0.6:64047, 127.0.0.10:64068]
         * [-1, 1537228672809129300]=[127.0.0.8:64055]
         * [-6148914691236517205, -3074457345618258603]=[127.0.0.7:64050, 127.0.0.8:64055]
         * [-1, 3074457345618258601]=[127.0.0.9:64060, 127.0.0.10:64068]
         * [3074457345618258601, 4611686018427387902]=[127.0.0.9:64060]
         *
         */

        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        // [-9223372036854775808, -6148914691236517205]
        mapping.put(expectedRanges.get(0),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6", "127.0.0.7"));
        // [-6148914691236517205, -4611686018427387904]
        mapping.put(expectedRanges.get(1),
                    Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.6", "127.0.0.7", "127.0.0.8"));
        // [-4611686018427387904, -3074457345618258603]
        mapping.put(expectedRanges.get(2),
                    Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.7", "127.0.0.8"));
        // [-3074457345618258603, -1537228672809129302]
        mapping.put(expectedRanges.get(3),
                    Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.7", "127.0.0.8", "127.0.0.9"));
        // [-1537228672809129302, -1]
        mapping.put(expectedRanges.get(4),
                    Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.8", "127.0.0.9"));
        // [-1, 1537228672809129300]
        mapping.put(expectedRanges.get(5),
                    Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.8", "127.0.0.9", "127.0.0.10"));
        // [1537228672809129300, 3074457345618258601]
        mapping.put(expectedRanges.get(6),
                    Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1", "127.0.0.9", "127.0.0.10"));
        // [3074457345618258601, 4611686018427387902]
        mapping.put(expectedRanges.get(7),
                    Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.6", "127.0.0.9", "127.0.0.10"));
        // [4611686018427387902, 6148914691236517203]
        mapping.put(expectedRanges.get(8),
                    Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2", "127.0.0.6", "127.0.0.10"));
        // [6148914691236517203, 7686143364045646504]
        mapping.put(expectedRanges.get(9),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6", "127.0.0.7", "127.0.0.10"));
        // Un-wrapped wrap-around range with the nodes in the initial range
        // [7686143364045646504, 9223372036854775807]
        mapping.put(expectedRanges.get(10),
                    Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.6", "127.0.0.7"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 6 node cluster
     * across 2 DCs with the 6 nodes joining the cluster (3 per DC)
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
         * [-9223372036854775808", "-4611686018427387907"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-4611686018427387907", "-4611686018427387906"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-4611686018427387906", "-7"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-7", "-6"]:["127.0.0.5","127.0.0.3","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-6", "4611686018427387893"]:["127.0.0.5","127.0.0.3","127.0.0.1", "127.0.0.6",
         *                                                  "127.0.0.2","127.0.0.4"]
         * [4611686018427387893", "4611686018427387894"]:["127.0.0.3","127.0.0.5","127.0.0.1", "127.0.0.6","127.0.0.2",
         *                                                  "127.0.0.4"]
         * ["4611686018427387894"", "9223372036854775807"]:["127.0.0.3","127.0.0.5","127.0.0.1", "127.0.0.6",
         *                                                  "127.0.0.2","127.0.0.4:]
         *
         *  New Node tokens:
         * 127.0.0.7 at token -2305843009213693956
         * 127.0.0.8 at token -2305843009213693955
         * 127.0.0.9 at token 2305843009213693944
         * 127.0.0.10 at token 2305843009213693945
         * 127.0.0.11 at token 6917529027641081844
         * 127.0.0.12 at token 6917529027641081845
         *
         *
         * Pending Ranges:
         * [-6, 2305843009213693944]=[127.0.0.9:62801]
         * [-6, 2305843009213693945]=[127.0.0.10:62802]
         * [-6, 4611686018427387893]=[127.0.0.12:62804, 127.0.0.7:62799, 127.0.0.8:62800, 127.0.0.11:62803]
         * [4611686018427387894, -4611686018427387907]=[127.0.0.7:62799, 127.0.0.8:62800, 127.0.0.9:62801,
         * 127.0.0.10:62802] (wrap-around)
         * [-4611686018427387906, -2305843009213693956]=[127.0.0.7:62799]
         * [-4611686018427387907, -4611686018427387906]=[127.0.0.7:62799, 127.0.0.8:62800, 127.0.0.9:62801,
         * 127.0.0.10:62802, 127.0.0.11:62803]
         * [-4611686018427387906, -7]=[127.0.0.12:62804, 127.0.0.9:62801, 127.0.0.10:62802, 127.0.0.11:62803]
         * [-4611686018427387906, -2305843009213693955]=[127.0.0.8:62800]
         * [4611686018427387894, 6917529027641081844]=[127.0.0.11:62803]
         * [4611686018427387894, 6917529027641081845]=[127.0.0.12:62804]
         * [4611686018427387893, 4611686018427387894]=[127.0.0.12:62804, 127.0.0.7:62799, 127.0.0.8:62800,
         * 127.0.0.9:62801, 127.0.0.11:62803]
         *
         */

        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();

        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10"));

        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.1", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10"));


        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.1", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));


        dc1Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.3", "127.0.0.9",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                            "127.0.0.8", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.3", "127.0.0.9",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                            "127.0.0.12"));
        dc1Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.10",
                                                            "127.0.0.12"));



        dc1Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.7", "127.0.0.11", "127.0.0.1", "127.0.0.3",
                                                            "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.8", "127.0.0.12", "127.0.0.2", "127.0.0.6",
                                                            "127.0.0.4"));

        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                             "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                             "127.0.0.8", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                             "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                             "127.0.0.8", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                             "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                             "127.0.0.8"));

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
