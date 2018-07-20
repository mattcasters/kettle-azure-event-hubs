package com.neo4j.kettle.azure.steps.listen;


import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
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
    meta.getRegularRowMeta( data.outputRowMeta, this );

    data.outputField = environmentSubstitute( meta.getOutputField() );
    data.partitionIdField = environmentSubstitute( meta.getPartitionIdField());
    data.offsetField = environmentSubstitute( meta.getOffsetField());
    data.sequenceNumberField = environmentSubstitute( meta.getSequenceNumberField());
    data.hostField = environmentSubstitute( meta.getHostField());
    data.enquedTimeField = environmentSubstitute( meta.getEnquedTimeField());

    String namespace = environmentSubstitute( meta.getNamespace() );
    String eventHubName = environmentSubstitute(meta.getEventHubName());
    String eventHubConnectionString = environmentSubstitute(meta.getEventHubConnectionString());
    String sasKeyName = environmentSubstitute(meta.getSasKeyName());
    String sasKey = environmentSubstitute(meta.getSasKey());
    String consumerGroupName = environmentSubstitute( meta.getConsumerGroupName() );
    String storageContainerName = environmentSubstitute(meta.getStorageContainerName());
    String storageConnectionString = environmentSubstitute(meta.getStorageConnectionString());

    String batchTransformationFile = environmentSubstitute( meta.getBatchTransformation() );
    String batchInputStep = environmentSubstitute( meta.getBatchInputStep() );
    String batchOutputStep = environmentSubstitute( meta.getBatchOutputStep() );

    // Create a single threaded transformation
    //
    if (StringUtils.isNotEmpty( batchTransformationFile ) && StringUtils.isNotEmpty( batchInputStep )) {
      logBasic( "Passing rows to a batching transformation running single threaded : " +batchTransformationFile);
      data.stt = true;
      data.sttMaxWaitTime = Const.toLong( environmentSubstitute( meta.getBatchMaxWaitTime() ), -1L);
      data.sttTransMeta = meta.loadBatchTransMeta( meta, repository, metaStore, this );
      data.sttTransMeta.setTransformationType( TransMeta.TransformationType.SingleThreaded );
      data.sttTrans = new Trans( data.sttTransMeta, this );
      data.sttTrans.setParent( getTrans() );

      // Leave a trace for Spoon...
      //
      getTrans().addActiveSubTransformation( getStepname(), data.sttTrans );

      data.sttTrans.prepareExecution( getTrans().getArguments() );

      data.sttRowProducer = data.sttTrans.addRowProducer( batchInputStep, 0 );

      if (StringUtils.isNotEmpty( batchOutputStep )) {
        StepInterface outputStep = data.sttTrans.findRunThread( batchOutputStep );
        if (outputStep==null) {
          throw new KettleStepException( "Unable to find output step '"+batchOutputStep+"'in batch transformation" );
        }
        outputStep.addRowListener( new RowAdapter() {
          @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
            AzureListener.this.putRow(rowMeta, row);
          }
        } );
      }

      data.sttTrans.startThreads();

      data.sttExecutor = new SingleThreadedTransExecutor( data.sttTrans );

      boolean ok = data.sttExecutor.init();
      if (!ok) {
        logError("Initializing batch transformation failed");
        stopAll();
        setErrors( 1 );
        return false;
      }
    } else {
      data.stt = false;
    }

    log.logDetailed("Creating connection string");
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
    options.setExceptionNotification(new AzureListenerErrorNotificationHandler( AzureListener.this ));

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

    // Create our event processor which is going to actually send rows to the batch transformation (or not)
    // and get rows from an optional output step.
    //
    final AzureListenerEventProcessor eventProcessor = new AzureListenerEventProcessor( AzureListener.this, data, data.batchSize );

    // In case we have a while since an iteration was done sending rows to the batch transformation, keep an eye out for the
    // maximum wait time.  If we go over that time, and we have records in the input of the batch, call oneIteration.
    // We need to make sure to halt the rest though.
    //
    if (data.stt && data.sttMaxWaitTime>0) {
      // Add a timer to check every max wait time to see whether or not we have to do an iteration...
      //
      logBasic( "Checking for stalled rows every 100ms to see if we exceed the maximum wait time: " +data.sttMaxWaitTime );
      try {
        Timer timer = new Timer();
        TimerTask timerTask = new TimerTask() {
          @Override public void run() {
            // Do nothing if we haven't started yet.
            //
            if ( eventProcessor.getLastIterationTime() > 0 ) {
              if ( eventProcessor.getPassedRowsCount() > 0 ) {
                long now = System.currentTimeMillis();

                long diff = now - eventProcessor.getLastIterationTime();
                if ( diff > data.sttMaxWaitTime ) {
                  logDetailed( "Stalled rows detected with wait time of "+((double)diff/1000) );

                  // Call one iteration but halt anything else first.
                  //
                  try {
                    eventProcessor.startWait();
                    eventProcessor.doOneIteration();
                  } catch(Exception e) {
                    throw new RuntimeException( "Error in batch iteration when max wait time was exceeded", e);
                  } finally {
                    eventProcessor.endWait();
                  }
                  logDetailed( "Done processing after max wait time.");

                }
              }
            }
          }
        };
        // Check ten times per second
        //
        timer.schedule( timerTask, 100, 100);
      } catch(RuntimeException e) {
        throw new KettleStepException( "Error in batch iteration when max wait time was exceeded", e);
      }
    }

    try{
      host.registerEventProcessorFactory( partitionContext -> eventProcessor )
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
