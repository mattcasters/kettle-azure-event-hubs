package com.neo4j.kettle.azure.steps.write;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

public class AzureWriterData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;

  public ConnectionStringBuilder connectionStringBuilder;
  public ExecutorService executorService;
  public EventHubClient eventHubClient;
  public long batchSize;
  public int fieldIndex;
  public LinkedList<EventData> list;
}
