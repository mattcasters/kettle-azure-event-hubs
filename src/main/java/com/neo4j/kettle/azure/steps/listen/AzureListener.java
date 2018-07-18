package com.neo4j.kettle.azure.steps.listen;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.IEventProcessorFactory;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import com.neo4j.kettle.azure.ErrorNotificationHandler;
import com.neo4j.kettle.azure.EventProcessor;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AzureListener extends BaseStep implements StepInterface {

  private AzureListenerData data;

  public AzureListener( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                        TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    AzureListenerMeta meta = (AzureListenerMeta) smi;
    AzureListenerData data = (AzureListenerData)sdi;

    data.batchSize = Const.toInt(environmentSubstitute( meta.getBatchSize()), 100);
    data.prefetchSize = Const.toInt(environmentSubstitute( meta.getPrefetchSize()), -1);
    data.list = new LinkedList<>();

    return super.init( smi, sdi );
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {

    AzureListenerData data = (AzureListenerData)sdi;

    data.executorService.shutdown();

    super.dispose( smi, sdi );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    final AzureListenerMeta meta = (AzureListenerMeta) smi;
    final AzureListenerData data = (AzureListenerData) sdi;


    // This thing is executed only once, rows are processed in the event processor later
    //

    // Get the output fields starting from nothing
    //
    data.outputRowMeta = new RowMeta();
    meta.getFields( data.outputRowMeta, getStepname(), null, getStepMeta(), this, repository, metaStore );

    data.outputField = environmentSubstitute( meta.getOutputField() );
    data.partitionIdField = environmentSubstitute( meta.getPartitionIdField());
    data.offsetField = environmentSubstitute( meta.getOffsetField());
    data.sequenceNumberField = environmentSubstitute( meta.getSequenceNumberField());
    data.hostField = environmentSubstitute( meta.getHostField());
    data.enquedTimeField = environmentSubstitute( meta.getEnquedTimeField());

    log.logDetailed("Creating connection string");

    String namespace = environmentSubstitute( meta.getNamespace() );
    String eventHubName = environmentSubstitute(meta.getEventHubName());
    String eventHubConnectionString = environmentSubstitute(meta.getEventHubConnectionString());
    String sasKeyName = environmentSubstitute(meta.getSasKeyName());
    String sasKey = environmentSubstitute(meta.getSasKey());
    String consumerGroupName = environmentSubstitute( meta.getConsumerGroupName() );
    String storageContainerName = environmentSubstitute(meta.getStorageContainerName());
    String storageConnectionString = environmentSubstitute(meta.getStorageConnectionString());

    data.connectionStringBuilder = new ConnectionStringBuilder()
        .setNamespaceName(namespace)
        .setEventHubName(eventHubName)
        .setSasKeyName(sasKeyName)
        .setSasKey(sasKey);

    log.logDetailed("Opening new executor service");
    data.executorService = Executors.newSingleThreadExecutor();
    log.logDetailed("Creating event hub client");
    try {
      data.eventHubClient = EventHubClient.createSync( data.connectionStringBuilder.toString(), data.executorService );
    } catch ( Exception e ) {
      throw new KettleStepException( "Unable to create event hub client", e);
    }

    EventProcessorHost host = new EventProcessorHost(
      EventProcessorHost.createHostName("KettleHost"), // unique Hostname string
      eventHubName, // Event hub name
      consumerGroupName,  // consumer group name
      eventHubConnectionString, // event hub connection string
      storageConnectionString, // storage connection string
      storageContainerName // storage container name
    );

    log.logDetailed("Registering host named " + host.getHostName());

    EventProcessorOptions options = new EventProcessorOptions();
    options.setExceptionNotification(new ErrorNotificationHandler());

    if (!StringUtils.isNotEmpty(meta.getBatchSize())) {
      options.setMaxBatchSize( Const.toInt( environmentSubstitute( meta.getBatchSize() ), 100 ) );
    }

    if (!StringUtils.isNotEmpty(meta.getPrefetchSize())) {
      options.setPrefetchCount( Const.toInt( environmentSubstitute( meta.getPrefetchSize() ), 100 ) );
    }

    data.executorService = Executors.newSingleThreadExecutor();
    try {
      data.eventHubClient = EventHubClient.createSync( data.connectionStringBuilder.toString(), data.executorService );
    } catch ( Exception e ) {
      throw new KettleStepException( "Unable to create event hub client", e );
    }

    /*
    try {
      host.registerEventProcessor(AzureListenerEventProcessor.class, options)
    */

    try{
      host.registerEventProcessorFactory( partitionContext -> new AzureListenerEventProcessor( AzureListener.this, data, data.batchSize ) )
         .whenComplete((unused, e) -> {
          // whenComplete passes the result of the previous stage through unchanged,
          // which makes it useful for logging a result without side effects.
          //
          if (e != null)
          {
            logError("Failure while registering: " + e.toString());
            if (e.getCause() != null)
            {
              logError("Inner exception: " + e.getCause().toString());
            }
            setErrors( 1 );
            stopAll();
            setOutputDone();
          }
        })
        .thenAccept((unused) -> {
          // This stage will only execute if registerEventProcessor succeeded.
          // If it completed exceptionally, this stage will be skipped.
          //
          // block until we need to stop...
          //
          while (!AzureListener.this.isStopped() && !AzureListener.this.outputIsDone()) {
            try {
              Thread.sleep( 0, 100 );
            } catch ( InterruptedException e ) {
              // Ignore
            }
          }

        })
        .thenCompose((unused) -> {
          // This stage will only execute if registerEventProcessor succeeded.
          //
          // Processing of events continues until unregisterEventProcessor is called. Unregistering shuts down the
          // receivers on all currently owned leases, shuts down the instances of the event processor class, and
          // releases the leases for other instances of EventProcessorHost to claim.
          //
          return host.unregisterEventProcessor();
        })
        .exceptionally((e) -> {
          logError("Failure while unregistering: " + e.toString());
          if (e.getCause() != null) {
            logError("Inner exception: " + e.getCause().toString());
          }
          return null;
        })
        .get(); // Wait for everything to finish before exiting main!
    } catch ( Exception e ) {
      throw new KettleException( "Error in event processor", e );
    }

    setOutputDone();
    return false;
  }
}
