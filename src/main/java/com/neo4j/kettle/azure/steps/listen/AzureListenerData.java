package com.neo4j.kettle.azure.steps.listen;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

public class AzureListenerData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;

  public ConnectionStringBuilder connectionStringBuilder;
  public ExecutorService executorService;
  public EventHubClient eventHubClient;
  public int batchSize;
  public int prefetchSize;
  public LinkedList<EventData> list;
  public String outputField;
  public String partitionIdField;
  public String offsetField;
  public String sequenceNumberField;
  public String hostField;
  public String enquedTimeField;

  public TransMeta sttTransMeta;
  public Trans sttTrans;
  public SingleThreadedTransExecutor sttExecutor;
  public boolean stt = false;
  public RowProducer sttRowProducer;
}
