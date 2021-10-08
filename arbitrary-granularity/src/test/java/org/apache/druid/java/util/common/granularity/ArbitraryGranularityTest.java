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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ArbitraryGranularityTest
{
  private ArbitraryGranularity granularity;

  @Before
  public void setUp()
  {
    granularity = new ArbitraryGranularity(Lists.newArrayList(
        Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    ));
  }

  /**
   * testTime pre-dates any intervals
   */
  @Test
  public void testIncrementLongTooEarly()
  {
    //2011-01-01T00Z
    long testTime = 1293840000000L;
    Assert.assertEquals(testTime, granularity.increment(testTime));
  }

  /**
   * testTime post-dates any intervals
   */
  @Test
  public void testIncrementLongTooLate()
  {
    //2013-01-01T00Z
    long testTime = 1356998400000L;
    Assert.assertEquals(DateTimes.MAX.getMillis(), granularity.increment(testTime));
  }

  /**
   * testTime falls within an interval
   */
  @Test
  public void testIncrementLongMatchedInterval()
  {
    //2012-01-01T05Z
    long testTime = 1325394000000L;
    Assert.assertEquals(DateTimes.of("2012-01-03T00Z").getMillis(), granularity.increment(testTime));
  }

  /**
   * testTime is within the start of the first interval and end of the last interval, but it doesn't match an interval.
   */
  @Test
  public void testIncrementLongMissedInterval()
  {
    //2012-01-12T00Z
    long testTime = 1326326400000L;
    Assert.assertEquals(testTime, granularity.increment(testTime));
  }

  /**
   * testTime pre-dates any intervals
   */
  @Test
  public void testBucketStartLongTooEarly()
  {
    //2011-01-01T00Z
    long testTime = 1293840000000L;
    Assert.assertEquals(DateTimes.MAX.getMillis(), granularity.bucketStart(testTime));
  }

  /**
   * testTime post-dates any intervals
   */
  @Test
  public void testBucketStartLongTooLate()
  {
    //2013-01-01T00Z
    long testTime = 1356998400000L;
    Assert.assertEquals(DateTimes.MAX.getMillis(), granularity.bucketStart(testTime));
  }

  /**
   * testTime falls within an interval
   */
  @Test
  public void testBucketStartLongMatchedInterval()
  {
    //2012-01-01T05Z
    long testTime = 1325394000000L;
    Assert.assertEquals(DateTimes.of("2012-01-01T00Z").getMillis(), granularity.bucketStart(testTime));
  }

  /**
   * testTime is within the start of the first interval and end of the last interval, but it doesn't match an interval.
   */
  @Test
  public void testBucketStartLongMissedInterval()
  {
    //2012-01-12T00Z
    long testTime = 1326326400000L;
    Assert.assertEquals(DateTimes.MAX.getMillis(), granularity.bucketStart(testTime));
  }

}
