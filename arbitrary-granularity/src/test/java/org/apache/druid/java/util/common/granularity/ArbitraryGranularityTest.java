/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.common.granularity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ArbitraryGranularityTest
{
  private final List<Interval> INTERVALS = Lists.newArrayList(
          Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
          Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
          Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
          Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
          Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
  );
  private ArbitraryGranularity granularity;

  @Before
  public void setUp()
  {
    granularity = new ArbitraryGranularity(INTERVALS);
  }

  @Test
  public void testGetIntervals() {
    NavigableSet<Interval> set = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    set.addAll(INTERVALS);
    assertEquals(set, granularity.getIntervals());
  }

  /**
   * testTime pre-dates any intervals
   */
  @Test
  public void testIncrementLongTooEarly()
  {
    //2011-01-01T00Z
    long testTime = 1293840000000L;
    assertEquals(DateTimes.MAX.getMillis(), granularity.increment(testTime));
  }

  /**
   * testTime post-dates any intervals
   */
  @Test
  public void testIncrementLongTooLate()
  {
    //2013-01-01T00Z
    long testTime = 1356998400000L;
    assertEquals(DateTimes.MAX.getMillis(), granularity.increment(testTime));
  }

  /**
   * testTime falls within an interval
   */
  @Test
  public void testIncrementLongMatchedInterval()
  {
    //2012-01-01T05Z
    long testTime = 1325394000000L;
    assertEquals(DateTimes.of("2012-01-03T00Z").getMillis(), granularity.increment(testTime));
  }

  /**
   * testTime is within the start of the first interval and end of the last interval, but it doesn't match an interval.
   */
  @Test
  public void testIncrementLongMissedInterval()
  {
    //2012-01-12T00Z
    long testTime = 1326326400000L;
    assertEquals(DateTimes.MAX.getMillis(), granularity.increment(testTime));
  }

  /**
   * testTime pre-dates any intervals
   */
  @Test
  public void testBucketStartLongTooEarly()
  {
    //2011-01-01T00Z
    long testTime = 1293840000000L;
    assertEquals(DateTimes.MAX.getMillis(), granularity.bucketStart(testTime));
  }

  /**
   * testTime post-dates any intervals
   */
  @Test
  public void testBucketStartLongTooLate()
  {
    //2013-01-01T00Z
    long testTime = 1356998400000L;
    assertEquals(DateTimes.MAX.getMillis(), granularity.bucketStart(testTime));
  }

  /**
   * testTime falls within an interval
   */
  @Test
  public void testBucketStartLongMatchedInterval()
  {
    //2012-01-01T05Z
    long testTime = 1325394000000L;
    assertEquals(DateTimes.of("2012-01-01T00Z").getMillis(), granularity.bucketStart(testTime));
  }

  /**
   * testTime is within the start of the first interval and end of the last interval, but it doesn't match an interval.
   */
  @Test
  public void testBucketStartLongMissedInterval()
  {
    //2012-01-12T00Z
    long testTime = 1326326400000L;
    assertEquals(DateTimes.MAX.getMillis(), granularity.bucketStart(testTime));
  }

  @Test
  public void testTooEarly()
  {
    Iterable<Interval> result = granularity.getIterable(Intervals.of("2010-01-01/2011-01-01"));

    Assert.assertFalse(result.iterator().hasNext());
  }

  @Test
  public void testTooLate()
  {
    Iterable<Interval> result = granularity.getIterable(Intervals.of("2014-01-01/2015-01-01"));

    Assert.assertFalse(result.iterator().hasNext());
  }

  @Test
  public void testStartMatch()
  {
    Iterable<Interval> result =
            granularity.getIterable(Intervals.of("2012-01-08T00Z/2012-01-11T00Z"));

    Iterator<Interval> iterator = result.iterator();
    assertEquals(
            "2012-01-08T00:00:00.000Z/2012-01-11T00:00:00.000Z",
            iterator.next().toString()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testMiddle()
  {
    Iterable<Interval> result =
            granularity.getIterable(Intervals.of("2012-01-05T00Z/2012-01-31T00Z"));

    Iterator<Interval> iterator = result.iterator();
    assertEquals(
            "2012-01-07T00:00:00.000Z/2012-01-08T00:00:00.000Z",
            iterator.next().toString()
    );
    assertEquals(
            "2012-01-08T00:00:00.000Z/2012-01-11T00:00:00.000Z",
            iterator.next().toString()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testStartInclusiveEndExclusive()
  {
    Iterable<Interval> result =
            granularity.getIterable(Intervals.of("2012-01-03T00Z/2012-01-03T12:00:00Z"));

    Iterator<Interval> iterator = result.iterator();
    assertEquals(
            "2012-01-03T00:00:00.000Z/2012-01-04T00:00:00.000Z",
            iterator.next().toString()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testEndsInInterval()
  {
    Iterable<Interval> result =
            granularity.getIterable(Intervals.of("2012-01-15T00Z/2012-02-15T00Z"));

    Iterator<Interval> iterator = result.iterator();
    assertEquals(
            "2012-02-01T00:00:00.000Z/2012-03-01T00:00:00.000Z",
            iterator.next().toString()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testNoIntervals()
  {
    //noinspection ResultOfObjectAllocationIgnored
    assertThrows(
            IllegalArgumentException.class,
            () -> new ArbitraryGranularity(Collections.emptyList(), null));
  }

  @Test
  public void testOverlap()
  {
    Interval i0 = Intervals.of("2020-01-01/2020-01-02");
    Interval i1 = Intervals.of("2020-01-01/2020-02-01");
    assertThrows("Overlapping granularity intervals: " +
                    "2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z, " +
                    "2020-01-01T00:00:00.000Z/2020-02-01T00:00:00.000Z",
            IAE.class,
            () -> new ArbitraryGranularity(ImmutableList.of(i0, i1), null));
  }

  @Test
  public void testTimezone()
  {
    granularity = new ArbitraryGranularity(
            ImmutableList.of(Intervals.of("2012-01-08/2012-01-11")),
            DateTimes.inferTzFromString("US/Central")
    );

    Iterable<Interval> result =
            granularity.getIterable(Intervals.of("2012-01-07T00Z/2012-01-09T00Z"));

    Iterator<Interval> iterator = result.iterator();
    assertEquals(
            "2012-01-07T18:00:00.000-06:00/2012-01-10T18:00:00.000-06:00",
            iterator.next().toString()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testBucketStartTooEarly()
  {
    assertEquals(DateTimes.MAX, granularity.bucketStart(DateTimes.of("2010-01-01T0Z")));
  }

  @Test
  public void testBucketStartTooLate()
  {
    assertEquals(DateTimes.MAX, granularity.bucketStart(DateTimes.of("2020-01-01T0Z")));
  }

  @Test
  public void testBucketStartBeginningInclusive()
  {
    assertEquals(
            DateTimes.of("2012-01-01T0Z"),
            granularity.bucketStart(DateTimes.of("2012-01-01T0Z"))
    );
  }

  @Test
  public void testBucketStartEndExclusive()
  {
    assertEquals(
            DateTimes.MAX,
            granularity.bucketStart(DateTimes.of("2012-03-01T0Z"))
    );
  }

  @Test
  public void testBucketStartHole()
  {
    assertEquals(
            DateTimes.MAX,
            granularity.bucketStart(DateTimes.of("2012-01-11T0Z"))
    );
  }

  @Test
  public void testBucketStartTruncateToIntervalStart()
  {
    assertEquals(
            DateTimes.of("2012-01-03T0Z"),
            granularity.bucketStart(DateTimes.of("2012-01-03T07:00Z"))
    );
    assertEquals(
            DateTimes.of("2012-02-01T0Z"),
            granularity.bucketStart(DateTimes.of("2012-02-13T17:00Z"))
    );
  }

  @Test
  public void testIterableTooFar()
  {
    Iterable<Interval> iterable = granularity.getIterable(Intervals.of("2010-01-01/2012-01-05"));
    Iterator<Interval> iterator = iterable.iterator();
    assertEquals(Intervals.of("2012-01-01T00Z/2012-01-03T00Z"), iterator.next());
    assertEquals(Intervals.of("2012-01-03T00Z/2012-01-04T00Z"), iterator.next());
    Assert.assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testIterableStartInMiddle()
  {
    Iterable<Interval> iterable = granularity.getIterable(Intervals.of("2012-01-03T12:00/2020-01-05"));
    Iterator<Interval> iterator = iterable.iterator();
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-01-03T00Z/2012-01-04T00Z"), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-01-07T00Z/2012-01-08T00Z"), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-01-08T00Z/2012-01-11T00Z"), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-02-01T00Z/2012-03-01T00Z"), iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIterableTooEarly()
  {
    Iterable<Interval> iterable = granularity.getIterable(Intervals.of("2010-01-01/2010-01-05"));
    Assert.assertFalse(iterable.iterator().hasNext());
  }

  @Test
  public void testIterableTooLate()
  {
    Iterable<Interval> iterable = granularity.getIterable(Intervals.of("2020-01-01/2020-01-05"));
    Assert.assertFalse(iterable.iterator().hasNext());
  }

  @Test
  public void testIterableAll()
  {
    Iterable<Interval> iterable = granularity.getIterable(Intervals.of("2010-01-01/2020-01-05"));
    Iterator<Interval> iterator = iterable.iterator();
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-01-01T00Z/2012-01-03T00Z"), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-01-03T00Z/2012-01-04T00Z"), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-01-07T00Z/2012-01-08T00Z"), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-01-08T00Z/2012-01-11T00Z"), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    assertEquals(Intervals.of("2012-02-01T00Z/2012-03-01T00Z"), iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIncrementIsIntervalEndNotNextIntervalStart()
  {
    assertEquals(
            DateTimes.of("2012-01-04T0Z"),
            granularity.increment(DateTimes.of("2012-01-03T0Z"))
    );
  }
}
