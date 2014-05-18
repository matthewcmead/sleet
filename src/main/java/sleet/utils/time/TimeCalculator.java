package sleet.utils.time;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates the calculation of the time-based portion of sleet ids.
 * 
 * @author mmead
 * 
 */
public class TimeCalculator {
  private final long epoch;
  private final long granularity;
  private final int bitsInTimeValue;
  private final long maxTimeValue;

  /**
   * Instantiate a TimeCalculator.
   * 
   * @param epoch
   *          the point in time of the epoch, expressed in milliseconds since
   *          Java's epoch UTC
   * @param granularity
   *          the number of time units each increment of the output value
   *          represents
   * @param granularityUnit
   *          the time unit in which granularity is expressed
   * @param bitsInTimeValue
   *          the number of bits in the output time value
   */
  public TimeCalculator(long epoch, long granularity, TimeUnit granularityUnit, int bitsInTimeValue) throws TimeCalculationException {
    if (bitsInTimeValue > 63) {
      throw new TimeCalculationException("This implementation supports time values represented by at most 63 bits.");
    }
    this.epoch = epoch;
    this.granularity = TimeUnit.MILLISECONDS.convert(granularity, granularityUnit);
    this.bitsInTimeValue = bitsInTimeValue;
    this.maxTimeValue = (1L << this.bitsInTimeValue) - 1L;
  }

  /**
   * Calculate a sleet time value for now (using System.currentTimeMillis()).
   * 
   * @return time value expressed as a long with the least significant bits
   *         populated
   * @throws TimeCalculationException
   *           if the number of output bits does not provide sufficient
   *           resolution for the value calculated
   * @throws TimeCalculationException
   *           if the number of output bits does not fit within a long
   * @throws TimeCalculationException
   *           if epoch is later than System.currentTimeMillis()
   */
  public long timeValue() throws TimeCalculationException {
    return timeValue(System.currentTimeMillis());
  }

  /**
   * Calculate a sleet time value.
   * 
   * @param millisSinceJavaEpochUTC
   *          the time for which to calculate a time value, expressed as
   *          milliseconds since Java epoch in UTC
   * @return time value expressed as a long with the least significant bits
   *         populated
   * @throws TimeCalculationException
   *           if the number of output bits does not provide sufficient
   *           resolution for the value calculated
   * @throws TimeCalculationException
   *           if the number of output bits does not fit within a long
   * @throws TimeCalculationException
   *           if epoch is later than millisSinceJavaEpochUTC
   */
  public long timeValue(long millisSinceJavaEpochUTC) throws TimeCalculationException {
    long delta = millisSinceJavaEpochUTC - this.epoch;
    if (delta < 0) {
      throw new TimeCalculationException("Epoch of " + this.epoch + " is later in time than " + millisSinceJavaEpochUTC + ".");
    }

    long timevalue = (long) Math.floor(delta / this.granularity);
    if (timevalue > this.maxTimeValue) {
      throw new TimeCalculationException("Time value calculated overflowed the number of time bits specified (" + this.bitsInTimeValue + ").");
    }
    if (timevalue < 0) {
      throw new TimeCalculationException("Time value calculated overflowed the maximum of 63 bits allowed by this implementation.");
    }
    return timevalue;
  }

  public long millisSinceJavaEpochUTC(long timeValue) throws TimeCalculationException {
    if (timeValue > this.maxTimeValue) {
      throw new TimeCalculationException("Time value provided overflowed the number of time bits specified (" + this.bitsInTimeValue + ").");
    }
    if (timeValue < 0) {
      throw new TimeCalculationException("Time value provided overflowed the maximum of 63 bits allowed by this implementation.");
    }

    return this.epoch + (timeValue * this.granularity);
  }

  public static void main(String[] args) throws Exception {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(Calendar.YEAR, 2014);
    cal.set(Calendar.MONTH, Calendar.MARCH);
    cal.set(Calendar.DAY_OF_MONTH, 14);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    TimeCalculator tc = new TimeCalculator(cal.getTimeInMillis(), 1L, TimeUnit.MILLISECONDS, 40);
    long lastval = -1;
    long countinms = 0;
    for (int i = 0; i < 250000; i++) {
      long val = tc.timeValue();
      if (lastval == -1 || val == lastval) {
        countinms++;
      } else {
        if (lastval > -1) {
          System.out.println("Calculated " + countinms + " timevalues in the last " + tc.granularity + "ms.");
        }
        countinms = 0;
      }
      lastval = val;
    }
    System.out.println("Calculated " + countinms + " timevalues in the last " + tc.granularity + "ms.");
  }

  public long getEpoch() {
    return epoch;
  }

  public long getGranularity() {
    return granularity;
  }

  public int getBitsInTimeValue() {
    return bitsInTimeValue;
  }

  public long getMaxTimeValue() {
    return maxTimeValue;
  }

}
