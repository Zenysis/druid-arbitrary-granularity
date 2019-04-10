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

// Copyright 2017 Zenysis Inc. All Rights Reserved.
// Author: stephen@zenysis.com (Stephen Ball)

package org.apache.druid.java.util.common.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class ArbitraryGranularity extends Granularity
{
  private final TreeSet<Interval> intervals;
  private final DateTimeZone timezone;
  private static final DateTime MAX_DATETIME = DateTimes.MAX;

  @JsonCreator
  public ArbitraryGranularity(
      @JsonProperty("intervals") List<Interval> inputIntervals,
      @JsonProperty("timezone") DateTimeZone timezone
  )
  {
    this.intervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    this.timezone = timezone == null ? DateTimeZone.UTC : timezone;

    if (inputIntervals == null) {
      inputIntervals = new ArrayList<>();
    }

    Preconditions.checkArgument(inputIntervals.size() > 0,
                                "at least one interval should be specified");

    // Insert all intervals
    for (final Interval inputInterval : inputIntervals) {
      Interval adjustedInterval = inputInterval;
      if (timezone != null) {
        adjustedInterval = new Interval(inputInterval.getStartMillis(),
                                        inputInterval.getEndMillis(), timezone);
      }
      intervals.add(adjustedInterval);
    }

    // Ensure intervals are non-overlapping (but they may abut each other)
    final PeekingIterator<Interval> intervalIterator =
        Iterators.peekingIterator(intervals.iterator());
    while (intervalIterator.hasNext()) {
      final Interval currentInterval = intervalIterator.next();

      if (intervalIterator.hasNext()) {
        final Interval nextInterval = intervalIterator.peek();
        if (currentInterval.overlaps(nextInterval)) {
          throw new IAE(
            "Overlapping granularity intervals: %s, %s",
            currentInterval,
            nextInterval
          );
        }
      }
    }
  }

  public ArbitraryGranularity(List<Interval> inputIntervals)
  {
    this(inputIntervals, null);
  }

  @JsonProperty("intervals")
  public TreeSet<Interval> getIntervals()
  {
    return this.intervals;
  }

  // Used only for Segments. Not for Queries
  @Override
  public DateTimeFormatter getFormatter(Formatter type)
  {
    throw new UnsupportedOperationException(
      "This method should not be invoked for this granularity type"
    );
  }

  // Used only for Segments. Not for Queries
  @Override
  public DateTime toDate(String filePath, Formatter formatter)
  {
    throw new UnsupportedOperationException(
      "This method should not be invoked for this granularity type"
    );
  }

  @Override
  public DateTime increment(DateTime time)
  {
    // Test if the input cannot be bucketed
    if (time.getMillis() > intervals.last().getEndMillis()) {
      return MAX_DATETIME;
    }

    // First interval with start time <= timestamp
    final Interval interval = intervals.floor(new Interval(time, MAX_DATETIME));
    return interval != null && interval.contains(time)
            ? interval.getEnd()
            : time;
  }

  @Override
  public DateTime bucketStart(DateTime time)
  {
    // Test if the input cannot be bucketed
    if (time.getMillis() > intervals.last().getEndMillis()) {
      return MAX_DATETIME;
    }

    // Just return the input. The iterable override will
    // define the buckets that should be used.
    return time;
  }

  @Override
  public DateTime toDateTime(long offset)
  {
    return new DateTime(offset, timezone);
  }

  @Override
  public Iterable<Interval> getIterable(Interval input)
  {
    long start = input.getStartMillis();
    long end = input.getEndMillis();

    // Return an empty iterable if the requested time interval does not
    // overlap any of the arbitrary intervals specified
    if (end < intervals.first().getStartMillis() ||
        start > intervals.last().getEndMillis()) {
      return ImmutableList.of();
    }

    return new Iterable<Interval>()
    {
      @Override
      public Iterator<Interval> iterator()
      {
        // Skip over the intervals that are known to be invalid
        // because they end before the requested start timestamp
        final PeekingIterator<Interval> intervalIterator =
            Iterators.peekingIterator(intervals.iterator());
        while (intervalIterator.hasNext() &&
               intervalIterator.peek().getEndMillis() <= start) {
          intervalIterator.next();
        }

        return new Iterator<Interval>()
        {
          @Override
          public boolean hasNext()
          {
            return intervalIterator.hasNext() &&
                   intervalIterator.peek().getStartMillis() < end;
          }

          @Override
          public Interval next()
          {
            return intervalIterator.next();
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public byte[] getCacheKey()
  {
    return StringUtils.toUtf8(getPrettyIntervals() + ":" + timezone.toString());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArbitraryGranularity that = (ArbitraryGranularity) o;

    if (!intervals.equals(that.intervals)) {
      return false;
    }
    return timezone.equals(that.timezone);
  }

  @Override
  public int hashCode()
  {
    int result = intervals.hashCode();
    result = 31 * result + timezone.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ArbitraryGranularity{" +
           "intervals=" + getPrettyIntervals() +
           ", timezone=" + timezone +
           '}';
  }

  private String getPrettyIntervals()
  {
    StringBuilder bob = new StringBuilder('[');
    boolean hit = false;
    for (Interval interval : intervals) {
      if (hit) {
        bob.append(',');
      }
      bob.append(interval.toString());
      hit = true;
    }
    bob.append(']');
    return bob.toString();
  }
}
