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
package org.camunda.bpm.container.impl.deployment;

import static org.camunda.bpm.container.impl.deployment.Attachments.BPM_PLATFORM_XML;

import java.util.List;

import org.camunda.bpm.application.impl.metadata.spi.ProcessesXml;
import org.camunda.bpm.container.impl.metadata.spi.BpmPlatformXml;
import org.camunda.bpm.container.impl.metadata.spi.ProcessEngineXml;
import org.camunda.bpm.container.impl.spi.DeploymentOperation;

/**
 * <p> Retrieves the List of ProcessEngines from an attached {@link ProcessesXml}.</p>
 * 
 * @see AbstractParseBpmPlatformXmlStep 
 *  
 */
public class PlatformXmlStartProcessEnginesStep extends AbstractStartProcessEnginesStep {

  protected List<ProcessEngineXml> getProcessEnginesXmls(DeploymentOperation operationContext) {
    
    final BpmPlatformXml bpmPlatformXml = operationContext.getAttachment(BPM_PLATFORM_XML);
    
    return bpmPlatformXml.getProcessEngines();
  }

}
