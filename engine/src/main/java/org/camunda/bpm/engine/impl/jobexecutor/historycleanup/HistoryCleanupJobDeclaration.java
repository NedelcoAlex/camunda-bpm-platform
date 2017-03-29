package org.camunda.bpm.engine.impl.jobexecutor.historycleanup;

import org.camunda.bpm.engine.impl.jobexecutor.JobDeclaration;
import org.camunda.bpm.engine.impl.persistence.entity.EverLivingJobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.util.ClockUtil;

/**
 * @author Svetlana Dorokhova
 */
public class HistoryCleanupJobDeclaration extends JobDeclaration<Object, EverLivingJobEntity> {

  public HistoryCleanupJobDeclaration() {
    super(HistoryCleanupJobHandler.TYPE);
  }

  @Override
  protected ExecutionEntity resolveExecution(Object context) {
    return null;
  }

  @Override
  protected EverLivingJobEntity newJobInstance(Object context) {
    EverLivingJobEntity message = new EverLivingJobEntity();
    HistoryCleanupJobHandlerConfiguration configuration = resolveJobHandlerConfiguration(context);
    message.setJobHandlerConfiguration(configuration);
    if (!configuration.isBatchWindowConfigured()) {
      message.setDuedate(ClockUtil.getCurrentTime());
    } else {
      message.setDuedate(configuration.getNextRunStartTime());
    }
    return message;
  }

  @Override
  protected HistoryCleanupJobHandlerConfiguration resolveJobHandlerConfiguration(Object context) {
    return new HistoryCleanupJobHandlerConfiguration();
  }
}
