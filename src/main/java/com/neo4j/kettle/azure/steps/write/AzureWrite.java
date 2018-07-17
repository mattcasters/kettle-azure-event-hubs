package com.neo4j.kettle.azure.steps.write;


import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.concurrent.Executors;

public class AzureWrite extends BaseStep implements StepInterface {

  private AzureWriterData data;

  public AzureWrite( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                     TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    AzureWriterMeta meta = (AzureWriterMeta) smi;
    AzureWriterData data = (AzureWriterData)sdi;

    data.batchSize = Const.toLong(environmentSubstitute( meta.getBatchSize()), 1);
    data.list = new LinkedList<>();

    return super.init( smi, sdi );
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {

    AzureWriterData data = (AzureWriterData)sdi;

    data.executorService.shutdown();

    super.dispose( smi, sdi );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    AzureWriterMeta meta = (AzureWriterMeta) smi;
    AzureWriterData data = (AzureWriterData)sdi;

    // Input row
    //
    Object[] row = getRow();
    if (row==null) {

      // See if we have data left in the message buffer
      //
      if (data.batchSize>1) {
        if (data.list.size()>0) {
          try {
            data.eventHubClient.sendSync( data.list );
          } catch ( EventHubException e ) {
            throw new KettleStepException( "Unable to send messages", e );
          }
          data.list = null;
        }
      }

      setOutputDone();
      return false;
    }

    if (first) {
      first=false;

      // get the output fields...
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, getStepMeta(), this, repository, metaStore );

      data.fieldIndex = getInputRowMeta().indexOfValue( meta.getMessageField() );
      if ( data.fieldIndex<0 ) {
        throw new KettleStepException( "Unable to find field '"+meta.getMessageField()+"' in the step input" );
      }

      log.logBasic("Creating connection string");

      String namespace = environmentSubstitute( meta.getNamespace() );
      String eventHubName = environmentSubstitute(meta.getEventHubName());
      String sasKeyName = environmentSubstitute(meta.getSasKeyName());
      String sasKey = environmentSubstitute(meta.getSasKey());

      data.connectionStringBuilder = new ConnectionStringBuilder()
        .setNamespaceName(namespace)
        .setEventHubName(eventHubName)
        .setSasKeyName(sasKeyName)
        .setSasKey(sasKey);

      log.logBasic("Opening new executor service");
      data.executorService = Executors.newSingleThreadExecutor();
      log.logBasic("Creating event hub client");
      try {
        data.eventHubClient = EventHubClient.createSync( data.connectionStringBuilder.toString(), data.executorService );
      } catch ( Exception e ) {
        throw new KettleStepException( "Unable to create event hub client", e);
      }
    }

    String message = getInputRowMeta().getString(row, data.fieldIndex);
    byte[] payloadBytes = message.getBytes( Charset.forName( "UTF-8" ) );
    EventData sendEvent = EventData.create( payloadBytes );
    try {

      if (data.batchSize<=1) {
        data.eventHubClient.sendSync( sendEvent );
      } else {
        data.list.add(sendEvent);

        if (data.list.size()>=data.batchSize) {
          data.eventHubClient.sendSync( data.list );
          data.list.clear();
        }
      }
    } catch ( EventHubException e ) {
      throw new KettleStepException( "Unable to send message to event hubs", e );
    }

    // Pass the rows to the next steps
    //
    putRow( data.outputRowMeta, row);
    return true;
  }
}
