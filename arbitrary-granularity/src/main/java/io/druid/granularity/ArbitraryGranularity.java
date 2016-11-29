// Copyright 2016 Zenysis Inc. All Rights Reserved.
// Author: stephen@zenysis.com (Stephen Ball)

package io.druid.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;

// TODO(stephen): Use these when our druid version is updated
// import io.druid.common.utils.JodaUtils;
// import io.druid.java.util.common.guava.Comparators;
// import io.druid.java.util.common.StringUtils;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

// TODO(stephen): Remove these imports when our druid is updated
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import org.joda.time.DateTimeComparator;


public class ArbitraryGranularity extends BaseQueryGranularity
{
  private final TreeSet<Interval> intervals;
  private final String timezone;

  @JsonCreator
  public ArbitraryGranularity(
    @JsonProperty("intervals") List<Interval> inputIntervals,
    @JsonProperty("timezone") String timezone
  )
  {
    this.intervals = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());
    this.timezone = timezone;
    final DateTimeZone timeZone = DateTimeZone.forID(this.timezone);

    if (inputIntervals == null) {
      inputIntervals = Lists.newArrayList();
    }

    Preconditions.checkArgument(inputIntervals.size() > 0,
                                "at least one interval should be specified");

    // Insert all intervals
    for (final Interval inputInterval : inputIntervals) {
      Interval adjustedInterval = inputInterval;
      if (this.timezone != null) {
        adjustedInterval = new Interval(inputInterval.getStartMillis(),
                                        inputInterval.getEndMillis(), timeZone);
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
          throw new IllegalArgumentException(
              String.format(
                  "Overlapping granularity intervals: %s, %s",
                  currentInterval,
                  nextInterval
              )
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

  @Override
  public long next(long t)
  {
    // Test if the input cannot be bucketed
    if (t > intervals.last().getEndMillis()) {
      return JodaUtils.MAX_INSTANT;
    }

    // First interval with start time <= timestamp
    final Interval interval = intervals.floor(
      new Interval(t, JodaUtils.MAX_INSTANT, DateTimeZone.UTC)
    );
    return interval != null && interval.contains(t)
            ? interval.getEndMillis()
            : t;
  }

  @Override
  public long truncate(final long t)
  {
    // Test if the input cannot be bucketed
    if (t > intervals.last().getEndMillis()) {
      return JodaUtils.MAX_INSTANT;
    }

    // Just return the input. The iterable override below will
    // define the buckets that should be used.
    return t;
  }

  @Override
  public Iterable<Long> iterable(final long start, final long end)
  {
    // Return an empty iterable if the requested time interval does not
    // overlap any of the arbitrary intervals specified
    if (end < intervals.first().getStartMillis() ||
        start > intervals.last().getEndMillis()) {
      // Special case for GroupBy queries. GroupBy calls iterator().next()
      // without first checking hasNext(). For these queries, we need to return
      // at least one value so that a null pointer exception is not raised.
      // Since we don't have a way of inspecting which query type is being
      // performed, try to match the signature of GroupBy's call to iterable
      // where the end timestamp is only one millisecond after the start
      // timestamp.
      // TODO(stephen): Remove this when the issue is resolved.
      if (end - start == 1) {
        return ImmutableList.of(JodaUtils.MIN_INSTANT);
      }
      return ImmutableList.of();
    }

    return new Iterable<Long>()
    {
      @Override
      public Iterator<Long> iterator()
      {
        // Skip over the intervals that are known to be invalid
        // because they end before the requested start timestamp
        final PeekingIterator<Interval> intervalIterator =
            Iterators.peekingIterator(intervals.iterator());
        while (intervalIterator.hasNext() &&
               intervalIterator.peek().getEndMillis() <= start) {
          intervalIterator.next();
        }

        return new Iterator<Long>()
        {
          @Override
          public boolean hasNext()
          {
            return intervalIterator.hasNext() &&
                   intervalIterator.peek().getStartMillis() < end;
          }

          @Override
          public Long next()
          {
            return intervalIterator.next().getStartMillis();
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
  public byte[] cacheKey()
  {
    return StringUtils.toUtf8(getPrettyIntervals());
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
    return !(timezone != null
             ? !timezone.equals(that.timezone)
             : that.timezone != null);
  }

  @Override
  public int hashCode()
  {
    int result = intervals.hashCode();
    result = 31 * result + (timezone != null ? timezone.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ArbitraryGranularity{" +
           "intervals=" + getPrettyIntervals() +
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

  // The classes below backfill in the classes that are available on 0.9.3+ but not
  // on druid 0.9.1.1 that we run on prod.
  // TODO(stephen): Remove when druid is updated
  private static class Comparators
  {
    private static final Comparator<Interval> INTERVAL_BY_START_THEN_END = new Comparator<Interval>()
    {
      private final DateTimeComparator dateTimeComp = DateTimeComparator.getInstance();

      @Override
      public int compare(Interval lhs, Interval rhs)
      {
        int retVal = dateTimeComp.compare(lhs.getStart(), rhs.getStart());
        if (retVal == 0) {
          retVal = dateTimeComp.compare(lhs.getEnd(), rhs.getEnd());
        }
        return retVal;
      }
    };

    public static Comparator<Interval> intervalsByStartThenEnd()
    {
      return INTERVAL_BY_START_THEN_END;
    }
  }

  private static class JodaUtils
  {
    public static final long MAX_INSTANT = Long.MAX_VALUE / 2;
    public static final long MIN_INSTANT = Long.MIN_VALUE / 2;
  }

  private static class StringUtils
  {
    public static final String UTF8_STRING = Charsets.UTF_8.toString();

    public static byte[] toUtf8(final String string)
    {
      try {
        return string.getBytes(UTF8_STRING);
      }
      catch (UnsupportedEncodingException e) {
        // Should never happen
        throw Throwables.propagate(e);
      }
    }
  }
}
