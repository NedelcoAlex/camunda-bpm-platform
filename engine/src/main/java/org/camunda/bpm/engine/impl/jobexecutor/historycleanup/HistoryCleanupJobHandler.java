package org.camunda.bpm.engine.impl.jobexecutor.historycleanup;

import java.util.List;
import org.camunda.bpm.engine.impl.ProcessEngineLogger;
import org.camunda.bpm.engine.impl.cmd.CommandLogger;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.jobexecutor.JobHandler;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.util.ClockUtil;

/**
 * @author Svetlana Dorokhova
 */
public class HistoryCleanupJobHandler implements JobHandler<HistoryCleanupJobHandlerConfiguration> {

  private final static CommandLogger LOG = ProcessEngineLogger.CMD_LOGGER;

  public static final String TYPE = "history-cleanup";

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public void execute(HistoryCleanupJobHandlerConfiguration configuration, ExecutionEntity execution, CommandContext commandContext, String tenantId) {
    //find JobEntity
    JobEntity jobEntity = commandContext.getJobManager().findJobByHandlerType(getType());
    try {
      if (!configuration.isBatchWindowConfigured() || configuration.isTimeLeft()) {
        //delete bunch of data
        List<String> processInstanceIds = getProcessInstanceIds(configuration, commandContext);
        if (!processInstanceIds.isEmpty()) {
          commandContext.getHistoricProcessInstanceManager().deleteHistoricProcessInstanceByIds(processInstanceIds);
          //TODO svt statistics / metrics
          //some data can still be left
          if (processInstanceIds.size() == configuration.getBunchSize()) {
            //reschedule now
            commandContext.getJobManager().reschedule(jobEntity, ClockUtil.getCurrentTime());
          }
        }
      }
    } finally {
      //if there are no retries left
      if (configuration.isBatchWindowConfigured() && jobEntity.getRetries() == 0) {
        //reschedule next "standard" call
        commandContext.getJobManager().reschedule(jobEntity, configuration.getNextRunStartTime());
      }
    }
  }

  private List<String> getProcessInstanceIds(HistoryCleanupJobHandlerConfiguration configuration, CommandContext commandContext) {
    return commandContext.getHistoricProcessInstanceManager().findHistoricProcessInstanceIdsForCleanup(configuration.getBunchSize());
  }

  @Override
  public HistoryCleanupJobHandlerConfiguration newConfiguration(String canonicalString) {
    return HistoryCleanupJobHandlerConfiguration.newInstance(canonicalString);
  }

  @Override
  public void onDelete(HistoryCleanupJobHandlerConfiguration configuration, JobEntity jobEntity) {
    //TODO svt
  }
}
