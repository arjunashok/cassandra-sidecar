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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.distributed.api.TokenSupplier;

/**
 * Static factory holder that provides a multi-DC token supplier
 */
public class MultiDcTokenSupplier
{

    /**
     * Tokens are allocation used in tests to simulate token allocation nodes for an approx even distribution
     * in a multiDC environment with nodes from different DCs being interleaved.
     * @param numNodes no. nodes from a single DC
     * @param numDcs no. of datacenters
     * @param numTokensPerNode no. tokens allocated to each node (this is always 1 if there are no vnodes)
     * @return The token supplier that vends the tokens
     */
    static TokenSupplier evenlyDistributedTokens(int numNodes, int numDcs, int numTokensPerNode)
    {

        long totalTokens = (long) numNodes * numDcs * numTokensPerNode;
        // Similar to Cassandra TokenSupplier, the increment is doubled to account for all tokens from MIN - MAX.
        // For multi-DC, since neighboring nodes from different DCs have consecutive tokens, the increment is
        // broadened by a factor of numDcs.
        BigInteger increment = BigInteger.valueOf(((Long.MAX_VALUE / totalTokens) * 2 * numDcs));
        List<String>[] tokens = new List[numNodes * numDcs];

        for (int i = 0; i < (numNodes * numDcs); ++i)
        {
            tokens[i] = new ArrayList(numTokensPerNode);
        }

        BigInteger value = BigInteger.valueOf(Long.MIN_VALUE + 1);

        for (int i = 0; i < numTokensPerNode; ++i)
        {
            int nodeId = 1;
            while (nodeId <= (numNodes * numDcs))
            {
                value = value.add(increment);
                // Nodes in different DCs are separated by a single token
                for (int dc = 0; dc < numDcs; dc++)
                {
                    tokens[nodeId - 1].add(value.add(BigInteger.valueOf(dc)).toString());
                    nodeId++;
                }
            }
        }

        return (nodeIdx) -> tokens[nodeIdx - 1];
    }
}
