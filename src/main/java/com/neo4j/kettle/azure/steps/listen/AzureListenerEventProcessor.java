package com.neo4j.kettle.azure.steps.listen;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.row.RowDataUtil;

import java.sql.Timestamp;
import java.time.Instant;

public class AzureListenerEventProcessor implements IEventProcessor {

  private final AzureListener azureStep;
  private final AzureListenerData azureData;

  private int checkpointBatchingSize = 500;
  private int checkpointBatchingCount = 0;

  public AzureListenerEventProcessor( AzureListener step, AzureListenerData data, int checkpointBatchingSize ) {
    this.azureStep = step;
    this.azureData = data;
    this.checkpointBatchingSize = checkpointBatchingSize;
  }

  // OnOpen is called when a new event processor instance is created by the host. In a real implementation, this
  // is the place to do initialization so that events can be processed when they arrive, such as opening a database
  // connection.
  //
  @Override
  public void onOpen( PartitionContext context ) throws Exception {
    if (azureStep.isDebug()) {
      azureStep.logDebug( "Partition " + context.getPartitionId() + " is opening" );
    }
  }

  // OnClose is called when an event processor instance is being shut down. The reason argument indicates whether the shut down
  // is because another host has stolen the lease for this partition or due to error or host shutdown. In a real implementation,
  // this is the place to do cleanup for resources that were opened in onOpen.
  //
  @Override
  public void onClose( PartitionContext context, CloseReason reason ) throws Exception {
    if (azureStep.isDebug()) {
      azureStep.logDebug( "Partition " + context.getPartitionId() + " is closing for reason " + reason.toString() );
    }
  }

  // onError is called when an error occurs in EventProcessorHost code that is tied to this partition, such as a receiver failure.
  // It is NOT called for exceptions thrown out of onOpen/onClose/onEvents. EventProcessorHost is responsible for recovering from
  // the error, if possible, or shutting the event processor down if not, in which case there will be a call to onClose. The
  // notification provided to onError is primarily informational.
  //
  @Override
  public void onError( PartitionContext context, Throwable error ) {
    azureStep.logError("Error on partition id "+context.getPartitionId()+" : "+error.toString(), error);
  }

  // onEvents is called when events are received on this partition of the Event Hub. The maximum number of events in a batch
  // can be controlled via EventProcessorOptions. Also, if the "invoke processor after receive timeout" option is set to true,
  // this method will be called with null when a receive timeout occurs.
  //
  @Override
  public void onEvents( PartitionContext context, Iterable<EventData> events ) throws Exception {
    int eventCount = 0;
    for ( EventData data : events ) {
      // It is important to have a try-catch around the processing of each event. Throwing out of onEvents deprives
      // you of the chance to process any remaining events in the batch.
      //

      // TODO: collect rows in a batch and send them to a sub-transformation
      //
      try {

        Object[] row = RowDataUtil.allocateRowData(azureData.outputRowMeta.size());
        int index = 0;

        // Message : String
        //
        if ( StringUtils.isNotEmpty(azureData.outputField)) {
          row[ index++ ] = new String( data.getBytes(), "UTF-8" );
        }

        // Partition ID : String
        //
        if ( StringUtils.isNotEmpty(azureData.partitionIdField)) {
          row[ index++ ] = context.getPartitionId();
        }

        // Offset : String
        //
        if ( StringUtils.isNotEmpty(azureData.offsetField)) {
          row[ index++ ] = data.getSystemProperties().getOffset();
        }

        // Sequence number: Integer
        //
        if ( StringUtils.isNotEmpty(azureData.sequenceNumberField)) {
          row[ index++ ] = Long.valueOf( data.getSystemProperties().getSequenceNumber() );
        }

        // Host: String
        //
        if ( StringUtils.isNotEmpty(azureData.hostField)) {
          row[ index++ ] = context.getOwner();
        }

        // Enqued Time: Timestamp
        //
        if ( StringUtils.isNotEmpty(azureData.enquedTimeField)) {
          Instant enqueuedTime = data.getSystemProperties().getEnqueuedTime();
          row[ index++ ] = Timestamp.from( enqueuedTime );
        }

        azureStep.putRow( azureData.outputRowMeta,  row);

        if (azureStep.isDebug()) {
          azureStep.logDebug("Event read and passed for PartitionId (" + context.getPartitionId() + "," + data.getSystemProperties().getOffset() + "," +
            data.getSystemProperties().getSequenceNumber() + "): " + new String( data.getBytes(), "UTF8" ) );
        }

        eventCount++;

        // Checkpointing persists the current position in the event stream for this partition and means that the next
        // time any host opens an event processor on this event hub+consumer group+partition combination, it will start
        // receiving at the event after this one. Checkpointing is usually not a fast operation, so there is a tradeoff
        // between checkpointing frequently (to minimize the number of events that will be reprocessed after a crash, or
        // if the partition lease is stolen) and checkpointing infrequently (to reduce the impact on event processing
        // performance). Checkpointing every five events is an arbitrary choice for this sample.
        //
        this.checkpointBatchingCount++;
        if ( ( checkpointBatchingCount % checkpointBatchingSize ) == 0 ) {
          if (azureStep.isDebug()) {
            azureStep.logDebug( "Partition " + context.getPartitionId() + " checkpointing at " +
              data.getSystemProperties().getOffset() + "," + data.getSystemProperties().getSequenceNumber() );
          }

          // Checkpoints are created asynchronously. It is important to wait for the result of checkpointing
          // before exiting onEvents or before creating the next checkpoint, to detect errors and to ensure proper ordering.
          //
          context.checkpoint( data ).get();
        }
      } catch ( Exception e ) {
        azureStep.logError( "Processing failed for an event: " + e.toString(), e );
        azureStep.setErrors( 1 );
        azureStep.stopAll();
      }
    }
    if (azureStep.isDebug()) {
      azureStep.logDebug( "Partition " + context.getPartitionId() + " batch size was " + eventCount + " for host " + context.getOwner() );
    }
  }


  public int getCheckpointBatchingSize() {
    return checkpointBatchingSize;
  }

  public void setCheckpointBatchingSize( int checkpointBatchingSize ) {
    this.checkpointBatchingSize = checkpointBatchingSize;
  }

  public int getCheckpointBatchingCount() {
    return checkpointBatchingCount;
  }

  public void setCheckpointBatchingCount( int checkpointBatchingCount ) {
    this.checkpointBatchingCount = checkpointBatchingCount;
  }

  public AzureListener getAzureStep() {
    return azureStep;
  }

  public AzureListenerData getAzureData() {
    return azureData;
  }
}