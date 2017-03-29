/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.engine.impl.cmd;

import java.io.Serializable;
import org.camunda.bpm.engine.authorization.Permissions;
import org.camunda.bpm.engine.authorization.Resources;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.jobexecutor.historycleanup.HistoryCleanupJobDeclaration;
import org.camunda.bpm.engine.impl.jobexecutor.historycleanup.HistoryCleanupJobHandler;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.util.ClockUtil;

/**
 * @author Svetlana Dorokhova
 */
public class HistoryCleanupCmd implements Command<String>, Serializable {

  public static final HistoryCleanupJobDeclaration HISTORY_CLEANUP_JOB_DECLARATION = new HistoryCleanupJobDeclaration();

  public HistoryCleanupCmd() {
  }

  @Override
  public String execute(CommandContext commandContext) {
    //TODO svt check authorizations
    commandContext.getAuthorizationManager().checkAuthorization(Permissions.DELETE_HISTORY, Resources.PROCESS_DEFINITION);

    //find or create job instance
    commandContext.getPropertyManager().acquireExclusiveLockForHistoryCleanupJob();
    JobEntity historyCleanupJob = commandContext.getJobManager().findJobByHandlerType(HistoryCleanupJobHandler.TYPE);
    if (historyCleanupJob == null) {
      historyCleanupJob = HISTORY_CLEANUP_JOB_DECLARATION.createJobInstance(null);
      historyCleanupJob.setDuedate(ClockUtil.getCurrentTime());
      Context.getCommandContext().getJobManager().insertAndHintJobExecutor(historyCleanupJob);
    } else {
      //reschedule now
      commandContext.getJobManager().reschedule(historyCleanupJob, ClockUtil.getCurrentTime());
    }

    return historyCleanupJob.getId();
  }
}
