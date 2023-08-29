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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
import org.apache.cassandra.sidecar.adapters.base.Partitioner;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Node movement scenarios integration tests for token range replica mapping endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class TokenRangeIntegrationMovingTest extends BaseTokenRangeIntegrationTest
{
    public static final int MOVING_NODE_IDX = 5;
    public static final int MULTIDC_MOVING_NODE_IDX = 10;

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWithKeyspaceMovingNode(VertxTestContext context,
                                               ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {

        UpgradeableCluster cluster =
        cassandraTestContext.configureAndStartCluster(builder ->
                                                      builder.withInstanceInitializer(BBHelperMovingNode::install));

        long moveTarget = getMoveTargetToken(cluster);
        runMovingTestScenario(context,
                              BBHelperMovingNode.TRANSIENT_STATE_START,
                              BBHelperMovingNode.TRANSIENT_STATE_END,
                              cluster,
                              generateExpectedRangeMappingMovingNode(moveTarget),
                              moveTarget);
    }

    private long getMoveTargetToken(UpgradeableCluster cluster)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        IUpgradeableInstance seed = cluster.get(1);
        // The target token to move the node to is calculated by adding an offset to the seed node token which
        // is half of the range between 2 tokens.
        // For multi-DC case (specifically 2 DCs), since neighbouring tokens can be consecutive, we use tokens 1
        // and 3 to calculate the offset
        int nextIndex = (annotation.numDcs() > 1) ? 3 : 2;
        long t2 = Long.parseLong(seed.config().getString("initial_token"));
        long t3 = Long.parseLong(cluster.get(nextIndex).config().getString("initial_token"));
        return (t2 + ((t3 - t2) / 2));
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void retrieveMappingWhileMovingNodeMultiDC(VertxTestContext context,
                                               ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        UpgradeableCluster cluster = getMultiDCCluster(annotation.nodesPerDc(),
                                                       annotation.numDcs(),
                                                       BBHelperMovingNodeMultiDC::install,
                                                       cassandraTestContext);

        long moveTarget = getMoveTargetToken(cluster);
        runMovingTestScenario(context,
                              BBHelperMovingNodeMultiDC.TRANSIENT_STATE_START,
                              BBHelperMovingNodeMultiDC.TRANSIENT_STATE_END,
                              cluster,
                              generateExpectedRangeMappingMovingNodeMultiDC(moveTarget),
                              moveTarget);
    }

    // TODO: Multiple replica-safe node movements in same DC, different DCs

    void runMovingTestScenario(VertxTestContext context,
                               CountDownLatch transientStateStart,
                               CountDownLatch transientStateEnd,
                               UpgradeableCluster cluster,
                               Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings,
                               long moveTargetToken) throws Exception
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
            final int movingNodeIndex = (annotation.numDcs() > 1) ? MULTIDC_MOVING_NODE_IDX : MOVING_NODE_IDX;

            IUpgradeableInstance movingNode = cluster.get(movingNodeIndex);
            new Thread(() -> movingNode.nodetoolResult("move", "--", Long.toString(moveTargetToken))
                                       .asserts()
                                       .success()).start();

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);
            ClusterUtils.awaitRingState(seed, movingNode, "Moving");

            retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
                assertMappingResponseOK(mappingResponse,
                                        DEFAULT_RF,
                                        dcReplication);

                validateNodeStates(mappingResponse,
                                   dcReplication,
                                   nodeNumber -> nodeNumber == movingNodeIndex ? "Moving" : "Normal");
                List<Range<BigInteger>> expectedRanges = getMovingNodesExpectedRanges(annotation.nodesPerDc(),
                                                                                      annotation.numDcs(),
                                                                                      moveTargetToken);
                validateTokenRanges(mappingResponse, expectedRanges);
                validateReplicaMapping(mappingResponse, movingNode, moveTargetToken, expectedRangeMappings);

                context.completeNow();
            });
        }
        finally
        {
            transientStateEnd.countDown();
        }
    }


    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
                                        IUpgradeableInstance movingNode,
                                        long moveTo,
                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
    {
        InetSocketAddress address = movingNode.config().broadcastAddress();
        String expectedAddress = address.getAddress().getHostAddress() +
                                 ":" +
                                 address.getPort();

        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());

        Optional<TokenRangeReplicasResponse.ReplicaInfo> moveResultRange // Get ranges ending in move token
        = mappingResponse.writeReplicas()
                         .stream()
                         .filter(r -> r.end().equals(String.valueOf(moveTo)))
                         .findAny();
        assertThat(moveResultRange.isPresent());
        List<String> replicasInRange = moveResultRange.get().replicasByDatacenter().values()
                                                      .stream()
                                                      .flatMap(Collection::stream)
                                                      .collect(Collectors.toList());
        assertThat(replicasInRange).contains(expectedAddress);
        assertThat(readReplicaInstances).contains(expectedAddress);
        assertThat(writeReplicaInstances).contains(expectedAddress);

        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings);
    }

    private List<Range<BigInteger>> getMovingNodesExpectedRanges(int initialNodeCount, int numDcs, long moveTo)
    {
        boolean moveHandled = false;

        TokenSupplier tokenSupplier = (numDcs > 1) ?
                                      MultiDcTokenSupplier.evenlyDistributedTokens(initialNodeCount, numDcs, 1) :
                                      TokenSupplier.evenlyDistributedTokens(initialNodeCount, 1);


        List<Range<BigInteger>> expectedRanges = new ArrayList<>();
        BigInteger startToken = Partitioner.Murmur3.minToken;
        BigInteger endToken = Partitioner.Murmur3.maxToken;
        int node = 1;
        BigInteger prevToken = new BigInteger(tokenSupplier.tokens(node++).stream().findFirst().get());
        Range<BigInteger> firstRange = Range.openClosed(startToken, prevToken);
        expectedRanges.add(firstRange);
        while (node <= (initialNodeCount * numDcs))
        {

            BigInteger currentToken = new BigInteger(tokenSupplier.tokens(node).stream().findFirst().get());
            if (!moveHandled && currentToken.compareTo(BigInteger.valueOf(moveTo)) > 0)
            {
                expectedRanges.add(Range.openClosed(prevToken, BigInteger.valueOf(moveTo)));
                expectedRanges.add(Range.openClosed(BigInteger.valueOf(moveTo), currentToken));
                moveHandled = true;
            }
            else
            {
                expectedRanges.add(Range.openClosed(prevToken, currentToken));
            }

            prevToken = currentToken;
            node++;
        }
        expectedRanges.add(Range.openClosed(prevToken, endToken));

        return expectedRanges;
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the last node being moved by assigning it a different token
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * In this test case, the moved node is inserted between nodes 1 and 2, resulting in splitting the ranges.
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingMovingNode(long moveTarget)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        List<Range<BigInteger>> expectedRanges = getMovingNodesExpectedRanges(annotation.nodesPerDc(),
                                                                              annotation.numDcs(),
                                                                              moveTarget);
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        // Initial range from Partitioner's MIN_TOKEN. This will include one of the replicas of the moved node since
        // it is adjacent to the range where it is being introduced.
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3",
                                                         "127.0.0.5"));
        // Range including the token of the moved node. Node 5 is added here (and the preceding 3 ranges)
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4",
                                                         "127.0.0.5"));
        // Split range resulting from the new token. This range is exclusive of the new token and node 5, and
        // has the same replicas as the previous range (as a result of the split)
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        // Node 1 is introduced here as it will take ownership of a portion of node 5's previous tokens as a result
        // of the move.
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5",
                                                         "127.0.0.1"));
        // Following 2 ranges remain unchanged as the replica-set remain the same post-move
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2"));
        // Third (wrap-around) replica of the new location of node 5 is added to the existing replica-set
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3",
                                                         "127.0.0.5"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 10 node cluster
     * across 2 DCs with the last node being moved by assigning it a different token
     *
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     *
     * In this test case, the moved node is inserted between nodes 1 and 2, resulting in splitting the ranges.
     */
    private Map<String, Map<Range<BigInteger>, List<String>>>
    generateExpectedRangeMappingMovingNodeMultiDC(long moveTarget)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        List<Range<BigInteger>> expectedRanges = getMovingNodesExpectedRanges(annotation.nodesPerDc(),
                                                                              annotation.numDcs(),
                                                                              moveTarget);
        /*
         * The following expected ranges are generated based on the following token assignments and pending ranges.
         *
         * Token Allocations:
         * MIN TOKEN: -9223372036854775808
         * /127.0.0.1:[-5534023222112865487]
         * /127.0.0.2:[-5534023222112865486]
         * /127.0.0.3:[-1844674407370955167]
         * /127.0.0.10:[-3689348814741910327] (New Location)
         * /127.0.0.4:[-1844674407370955166]
         * /127.0.0.5:[1844674407370955153]
         * /127.0.0.6:[1844674407370955154]
         * /127.0.0.7:[5534023222112865473]
         * /127.0.0.8:[5534023222112865474]
         * /127.0.0.9:[9223372036854775793]
         * /127.0.0.10:[9223372036854775793] (Old Location)
         * MAX TOKEN: 9223372036854775807
         *
         * Pending Ranges:
         * [-5534023222112865487, -5534023222112865486]=[127.0.0.10]
         * [-5534023222112865486, -3689348814741910327]=[127.0.0.10]
         * [-1844674407370955166, 1844674407370955153]=[127.0.0.2]
         * [1844674407370955153, 1844674407370955154]=[127.0.0.2]
         * [9223372036854775794, -5534023222112865487]=[127.0.0.10]
         */

        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();
        // Replica 2
        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6",
                                                            "127.0.0.10"));

        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6",
                                                            "127.0.0.10"));
        // Split range resulting from the new token. Part 1 including the new token.
        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10"));
        // Split range resulting from the new token. Part 2 excluding new token (but starting from it)
        dc1Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7"));
        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10",
                                                            "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10",
                                                            "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3"));
        dc2Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.2"));

        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3"));
        dc2Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.10", "127.0.0.2", "127.0.0.4"));

        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.10", "127.0.0.2", "127.0.0.4"));
        // Replica 3
        dc1Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6",
                                                             "127.0.0.10"));

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
     * ByteBuddy Helper for a multiDC moving node
     */
    @Shared
    public static class BBHelperMovingNodeMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == MULTIDC_MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMovingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return res;
        }
    }

    /**
     * ByteBuddy Helper for a single moving node
     */
    @Shared
    public static class BBHelperMovingNode
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMovingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return res;
        }
    }
}
