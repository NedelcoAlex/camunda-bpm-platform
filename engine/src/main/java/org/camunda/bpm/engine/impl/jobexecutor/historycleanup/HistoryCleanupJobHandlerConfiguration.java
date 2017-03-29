/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.engine.impl.jobexecutor.historycleanup;

import org.camunda.bpm.engine.impl.jobexecutor.JobHandlerConfiguration;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.joda.time.LocalTime;

import java.util.Calendar;
import java.util.Date;

/**
 * @author Svetlana Dorokhova
 *
 */
public class HistoryCleanupJobHandlerConfiguration implements JobHandlerConfiguration {

//  private final static LocalTime BATCH_WINDOW_START_TIME = new LocalTime(21);
  private final static LocalTime BATCH_WINDOW_END_TIME = new LocalTime(0,0);  //midnight
  private final static int BUNCH_SIZE = 500;

  private LocalTime batchWindowStartTime;

  private LocalTime batchWindowEndTime = BATCH_WINDOW_END_TIME;

  private Integer bunchSize = BUNCH_SIZE;

  public LocalTime getBatchWindowStartTime() {
    return batchWindowStartTime;
  }

  public void setBatchWindowStartTime(LocalTime batchWindowStartTime) {
    this.batchWindowStartTime = batchWindowStartTime;
  }

  public LocalTime getBatchWindowEndTime() {
    return batchWindowEndTime;
  }

  public void setBatchWindowEndTime(LocalTime batchWindowEndTime) {
    this.batchWindowEndTime = batchWindowEndTime;
  }

  public Integer getBunchSize() {
    return bunchSize;
  }

  public void setBunchSize(Integer bunchSize) {
    this.bunchSize = bunchSize;
  }

  @Override
  public String toCanonicalString() {
    return batchWindowStartTime == null ? "" : batchWindowStartTime
        + "$" + batchWindowEndTime
        + "$" + bunchSize;
  }

  public static HistoryCleanupJobHandlerConfiguration newInstance(String canonicalString) {
    //TODO svt parse canonicalString
    return new HistoryCleanupJobHandlerConfiguration();
  }

  public Date getNextRunStartTime() {
    Date now = ClockUtil.getCurrentTime();
    Date todayPossibleRun = updateTime(now, batchWindowStartTime);
    if (todayPossibleRun.after(now)) {
      return todayPossibleRun;
    } else {
      //tomorrow
      return addDays(todayPossibleRun, 1);
    }
  }

  private Date updateTime(Date now, LocalTime newTime) {
    Calendar c = Calendar.getInstance();
    c.setTime(now);
    c.set(Calendar.HOUR_OF_DAY, newTime.getHourOfDay());
    c.set(Calendar.MINUTE, newTime.getMinuteOfHour());
    c.set(Calendar.SECOND, newTime.getSecondOfMinute());
    c.set(Calendar.MILLISECOND, newTime.getMillisOfSecond());
    return c.getTime();
  }

  private Date addDays(Date date, int amount) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.DATE, amount);
    return c.getTime();
  }

  public boolean isTimeLeft() {
    Date now = ClockUtil.getCurrentTime();
    Date todayEndTime = updateTime(now, batchWindowEndTime);
    //TODO svt maybe some delta is needed? we can remember duration of last run. disclaimer in docs is needed

    if (batchWindowEndTime.isAfter(batchWindowStartTime)) {
      return todayEndTime.after(now);
    } else {
      //batchWindowEndTime is supposed to be in next date
      Date tomorrowEndTime = addDays(todayEndTime, 1);
      return tomorrowEndTime.after(now);
    }
  }

  public boolean isBatchWindowConfigured() {
    return batchWindowStartTime != null;
  }
}
