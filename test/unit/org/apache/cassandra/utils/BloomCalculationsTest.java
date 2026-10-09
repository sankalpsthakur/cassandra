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
package org.apache.cassandra.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BloomCalculationsTest
{
    /**
     * A false positive chance of at least probs[2][1] (0.393) is met with 2 buckets per element, so
     * computeBloomSpec returns that spec without searching the table. It used to swap the arguments of
     * BloomSpecification and return K=2 hash functions over 1 bucket per element, whose false positive
     * rate of about 0.75 is higher than the one asked for.
     */
    @Test
    public void testComputeBloomSpecForHighFalsePositiveChance()
    {
        int maxBuckets = BloomCalculations.probs.length - 1;
        BloomCalculations.BloomSpecification twoBuckets = BloomCalculations.computeBloomSpec(2);

        for (double maxFalsePosProb : new double[]{ BloomCalculations.probs[2][1], 0.4, 0.5, 0.74, 0.9 })
        {
            BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(maxBuckets, maxFalsePosProb);
            String message = spec + " for maxFalsePosProb " + maxFalsePosProb;
            assertEquals(message, 2, spec.bucketsPerElement);
            assertEquals(message, twoBuckets.K, spec.K);
            assertTrue(message, BloomCalculations.probs[spec.bucketsPerElement][spec.K] <= maxFalsePosProb);
        }
    }
}
