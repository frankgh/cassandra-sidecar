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

package org.apache.cassandra.sidecar.adapters.base;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;


/**
 * Representation of a token range (exclusive start and inclusive end - (start, end]) and the
 * corresponding mapping to replica-set hosts. Static factory ensures that ranges are always unwrapped.
 * Note: Range comparisons are used for ordering of ranges. eg. A.compareTo(B) <= 0 implies that
 * range A occurs before range B, not their sizes.
 */
public class TokenRangeReplicas implements Comparable<TokenRangeReplicas>
{
    private final BigInteger start;
    private final BigInteger end;

    private final Partitioner partitioner;

    private final Set<String> replicaSet;

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicas.class);

    private TokenRangeReplicas(BigInteger start, BigInteger end, Partitioner partitioner, Set<String> replicaSet)
    {
        this.start = start;
        this.end = end;
        this.partitioner = partitioner;
        this.replicaSet = replicaSet;
    }

    public static List<TokenRangeReplicas> generateTokenRangeReplicas(BigInteger start,
                                                                      BigInteger end,
                                                                      Partitioner partitioner,
                                                                      Set<String> replicaSet)
    {
        if (start.compareTo(end) > 0)
        {
            return unwrapRange(start, end, partitioner, replicaSet);
        }

        return Collections.singletonList(new TokenRangeReplicas(start, end, partitioner, replicaSet));
    }


    public BigInteger start()
    {
        return start;
    }

    public BigInteger end()
    {
        return end;
    }

    public Set<String> replicaSet()
    {
        return replicaSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@NotNull TokenRangeReplicas other)
    {
        validateRangesForComparison(other);
        int compareStart = this.start.compareTo(other.start);
        return (compareStart != 0) ? compareStart : this.end.compareTo(other.end);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof TokenRangeReplicas))
        {
            return false;
        }
        TokenRangeReplicas that = (TokenRangeReplicas) o;
        return (this.start.equals(that.start) && this.end.equals(that.end) && this.partitioner == that.partitioner);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hashCode(start, end, partitioner);
    }

    private boolean isWrapAround()
    {
        // Empty/Self range are also treated as wrap-around
        return start.compareTo(end) >= 0;
    }

    private void validateRangesForComparison(@NotNull TokenRangeReplicas other)
    {
        if (this.partitioner != other.partitioner)
            throw new IllegalStateException("Token ranges being compared do not have the same partitioner");
    }

    protected boolean contains(TokenRangeReplicas other)
    {
        validateRangesForComparison(other);
        return (other.start.compareTo(this.start) >= 0 && other.end.compareTo(this.end) <= 0);
    }

    /**
     * For subset ranges, this is used to determine if a range is larger than the other by comparing start-end lengths
     * If both ranges end at the min, we compare starting points to determine the result.
     * When the left range is the only one ending at min, it is always the larger one since all subsequent ranges
     * in the sorted range list have to be smaller.
     * <p>
     * This method assumes that the ranges are normalized and unwrapped, i.e.
     * 'this' comes before 'other' AND there's no wrapping around the min token
     *
     * @param other the next range in the range list to compare
     * @return true if "this" range is larger than the other
     */
    protected boolean isLarger(TokenRangeReplicas other)
    {
        validateRangesForComparison(other);
        return this.end.subtract(this.start).compareTo(other.end.subtract(other.start)) > 0;
    }

    /**
     * Determines intersection if the next range starts before the current range ends. This method assumes that
     * the provided ranges are sorted and unwrapped.
     * When the current range goes all the way to the end, we determine intersection if the next range starts
     * after the current since all subsequent ranges have to be subsets.
     *
     * @param other the range we are currently processing to check if "this" intersects it
     * @return true if "this" range intersects the other
     */
    protected boolean intersects(TokenRangeReplicas other)
    {
        if (this.compareTo(other) > 0)
            throw new IllegalStateException(
            String.format("Token ranges - (this:%s other:%s) are not ordered", this, other));

        return this.end.compareTo(other.start) > 0 && this.start.compareTo(other.end) < 0; // Start exclusive (DONE)
    }

    /**
     * For a range to be a last range, it has to span until the max token of the partitioner. Note that there could be
     * other sub-ranges occurring after this range in the sequence, but they are candidates to be merged since
     * they would overlap.
     *
     * @return true if this range is the last range
     */
    protected boolean isLastRange()
    {
        return this.end.compareTo(partitioner.maxToken) == 0;
    }

    /**
     * Unwraps the token range if it wraps-around to end either on or after the least token by overriding such
     * ranges to end at the partitioner max-token value in the former case and splitting into 2 ranges in the latter
     * case.
     *
     * @return list of split ranges
     */
    private static List<TokenRangeReplicas> unwrapRange(BigInteger start,
                                                        BigInteger end,
                                                        Partitioner partitioner,
                                                        Set<String> replicaSet)
    {
        if (start.compareTo(partitioner.maxToken) == 0)
        {
            return Collections.singletonList(
            new TokenRangeReplicas(partitioner.minToken, end, partitioner, replicaSet));
        }

        // Wrap-around range goes beyond at the "min-token" and is therefore split into two.
        List<TokenRangeReplicas> unwrapped = new ArrayList<>(2);
        unwrapped.add(new TokenRangeReplicas(start, partitioner.maxToken, partitioner, replicaSet));
        unwrapped.add(new TokenRangeReplicas(partitioner.minToken, end, partitioner, replicaSet));
        return unwrapped;
    }


    /**
     * Given a list of token ranges with replica-sets, normalizes them by unwrapping around the beginning/min
     * of the range and removing overlaps to return a sorted list of non-overlapping ranges.
     * <p>
     * For an overlapping range that is included in both natural and pending ranges, say R_natural and R_pending
     * (where R_natural == R_pending), the replicas of both R_natural and R_pending should receive writes.
     * Therefore, the write-replicas of such range is the union of both replica sets.
     * This method implements the consolidation process.
     *
     * @param ranges
     * @return sorted list of non-overlapping ranges and replica-sets
     */
    public static List<TokenRangeReplicas> normalize(List<TokenRangeReplicas> ranges)
    {

        if (ranges.stream().noneMatch(r -> r.partitioner.minToken.compareTo(r.start()) == 0))
        {
            LOGGER.warn("{} based minToken does not exist in the token ranges", Partitioner.class.getName());
        }

        return deoverlap(ranges);
    }

    /**
     * Given a list of unwrapped (around the starting/min value) token ranges and their replica-sets, return list of
     * ranges with no overlaps. Any impacted range absorbs the replica-sets from the overlapping range.
     * This is to ensure that we have most coverage while using the replica-sets as write-replicas.
     * Overlaps are removed by splitting the original range around the overlap boundaries, resulting in sub-ranges
     * with replicas from all the overlapping replicas.
     *
     *
     * <pre>
     * Illustration:
     * Input with C overlapping with A and B
     *   |----------A-----------||----------B-------------|
     *                  |--------C----------|
     *
     * Split result: C is split first which further splits A and B to create
     *  |-----------A----------||----------B-------------|
     *                 |---C---|----C'----|
     *
     * Subsets C & C' are merged into supersets A and B by splitting them. Replica-sets for A,C and B,C are merged
     * for the resulting ranges.
     *  |-----A------|----AC---||---BC-----|-----B------|
     *
     *  </pre>
     */
    private static List<TokenRangeReplicas> deoverlap(List<TokenRangeReplicas> ranges)
    {
        if (ranges.isEmpty())
            return ranges;

        Collections.sort(ranges);
        List<TokenRangeReplicas> output = new ArrayList<>();

        Iterator<TokenRangeReplicas> iter = ranges.iterator();
        TokenRangeReplicas current = iter.next();
        Partitioner partitioner = current.partitioner;

        while (iter.hasNext())
        {
            TokenRangeReplicas next = iter.next();
            if (!current.intersects(next))
            {
                // No overlaps found add current range
                output.add(current);
                current = next;
                continue;
            }

            if (current.equals(next))
            {
                current = new TokenRangeReplicas(current.start, current.end, current.partitioner,
                                                 mergeReplicas(current, next));
                LOGGER.debug("Merged identical token ranges. TokenRangeReplicas={}", current);
            }
            // Intersection handling of subset case i.e. when either range is a subset of the other
            // Since ranges are "unwrapped" and pre-sorted, we treat intersections from a range that ends at the
            // "max" (and following ranges) as a subset case.
            else if (current.isLastRange() || current.contains(next) || next.contains(current))
            {
                // Merge subset ranges
                current = processSubsetCases(partitioner, output, iter, current, next);
            }
            else
            {
                // Split overlapping ranges
                current = processOverlappingRanges(partitioner, output, current, next);
            }
        }
        if (current != null)
            output.add(current);
        return output;
    }

    private static TokenRangeReplicas processOverlappingRanges(Partitioner partitioner,
                                                               List<TokenRangeReplicas> output,
                                                               TokenRangeReplicas current,
                                                               TokenRangeReplicas next)
    {
        output.addAll(splitOverlappingRanges(current, next));
        current = new TokenRangeReplicas(current.end, next.end, partitioner, next.replicaSet);
        return current;
    }

    private static TokenRangeReplicas processSubsetCases(Partitioner partitioner,
                                                         List<TokenRangeReplicas> output,
                                                         Iterator<TokenRangeReplicas> iter,
                                                         TokenRangeReplicas current,
                                                         TokenRangeReplicas next)
    {
        LOGGER.debug("Processing subset token ranges. current={}, next={}", current, next);
        TokenRangeReplicas outer = current.isLarger(next) ? current : next;
        TokenRangeReplicas inner = outer.equals(current) ? next : current;

        // Pre-calculate the overlap segment which is common for the sub-cases below
        processOverlappingPartOfRange(partitioner, output, current, next, inner);

        // 1. Subset shares the start of the range
        if (outer.start.compareTo(inner.start) == 0)
        {
            // Override last segment as "current" as there could be other overlaps with subsequent segments
            current = new TokenRangeReplicas(inner.end, outer.end, partitioner, outer.replicaSet);
        }
        // 2. Subset shares the end of the range
        else if (outer.end.compareTo(inner.end) == 0)
        {
            current = processSubsetWithSharedEnd(partitioner, output, iter, outer, inner);
        }
        // 3. Subset is in-between the range
        else
        {
            current = processContainedSubset(partitioner, output, outer, inner);
        }
        return current;
    }

    private static TokenRangeReplicas processContainedSubset(Partitioner partitioner,
                                                             List<TokenRangeReplicas> output,
                                                             TokenRangeReplicas outer,
                                                             TokenRangeReplicas inner)
    {
        TokenRangeReplicas current;
        TokenRangeReplicas previous = new TokenRangeReplicas(outer.start, inner.start, partitioner, outer.replicaSet);
        output.add(previous);
        // Override last segment as "current" as there could be other overlaps with subsequent segments
        current = new TokenRangeReplicas(inner.end, outer.end, partitioner, outer.replicaSet);
        return current;
    }

    private static TokenRangeReplicas processSubsetWithSharedEnd(Partitioner partitioner,
                                                                 List<TokenRangeReplicas> output,
                                                                 Iterator<TokenRangeReplicas> iter,
                                                                 TokenRangeReplicas outer,
                                                                 TokenRangeReplicas inner)
    {
        TokenRangeReplicas current;
        TokenRangeReplicas previous = new TokenRangeReplicas(outer.start, inner.start, partitioner,
                                                             outer.replicaSet);
        output.add(previous);
        // Move pointer forward since we are done processing both current & next
        // Return if we have processed the last range
        current = (iter.hasNext()) ? iter.next() : null;
        return current;
    }

    private static void processOverlappingPartOfRange(Partitioner partitioner,
                                                      List<TokenRangeReplicas> output,
                                                      TokenRangeReplicas current,
                                                      TokenRangeReplicas next,
                                                      TokenRangeReplicas inner)
    {
        TokenRangeReplicas overlap = new TokenRangeReplicas(inner.start, inner.end, partitioner,
                                                            mergeReplicas(current, next));
        output.add(overlap);
    }

    private static Set<String> mergeReplicas(TokenRangeReplicas current, TokenRangeReplicas next)
    {
        Set<String> merged = new HashSet<>(current.replicaSet);
        merged.addAll(next.replicaSet);
        return merged;
    }

    private static List<TokenRangeReplicas> splitOverlappingRanges(TokenRangeReplicas current,
                                                                   TokenRangeReplicas next)
    {
        Partitioner partitioner = current.partitioner;
        TokenRangeReplicas part = new TokenRangeReplicas(current.start,
                                                         next.start,
                                                         partitioner,
                                                         current.replicaSet);
        // Split current at starting point of next; add to result; add new overlap to set
        Set<String> mergedReplicaSet = Stream.concat(current.replicaSet.stream(), next.replicaSet.stream())
                                             .collect(Collectors.toSet());
        TokenRangeReplicas overlap = new TokenRangeReplicas(next.start,
                                                            current.end,
                                                            partitioner,
                                                            mergedReplicaSet);
        LOGGER.debug("Overlapping token ranges split into part={}, overlap={}", part, overlap);
        return Arrays.asList(part, overlap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("Range(%s, %s]: %s:%s", start.toString(), end.toString(), replicaSet, partitioner);
    }
}
