package com.neo4j.kettle.azure.steps.listen;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@Step(
  id = "AzureListener",
  name = "Microsoft Azure Event Hubs Listener",
  description = "Listen to a Microsoft Azure Event Hub and read from it",
  image = "azure.svg",
  categoryDescription = "Streaming"
)
public class AzureListenerMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String NAMESPACE = "namespace";
  public static final String EVENT_HUB_NAME = "event_hub_name";
  public static final String SAS_KEY_NAME = "sas_key_name";
  public static final String SAS_KEY = "sas_key";
  public static final String BATCH_SIZE = "batch_size";
  public static final String PREFETCH_SIZE = "prefetch_size";
  public static final String OUTPUT_FIELD = "output_field";
  public static final String PARTITION_ID_FIELD = "partition_id_field";
  public static final String OFFSET_FIELD = "offset_field";
  public static final String SEQUENCE_NUMBER_FIELD = "sequence_number_field";
  public static final String HOST_FIELD = "host_field";
  public static final String ENQUED_TIME_FIELD = "enqued_time_field";

  public static final String CONSUMER_GROUP_NAME = "consumer_group_name";
  public static final String EVENT_HUB_CONNECTION_STRING = "event_hub_connection_string";
  public static final String STORAGE_CONNECTION_STRING = "storage_connection_string";
  public static final String STORAGE_CONTAINER_NAME = "storage_container_name";


  private String namespace;
  private String eventHubName;
  private String sasKeyName;
  private String sasKey;
  private String consumerGroupName;
  private String eventHubConnectionString;
  private String storageConnectionString;
  private String storageContainerName;

  private String prefetchSize;
  private String batchSize;

  private String outputField;
  private String partitionIdField;
  private String offsetField;
  private String sequenceNumberField;
  private String hostField;
  private String enquedTimeField;

  public AzureListenerMeta() {
    super();
  }

  @Override public void setDefault() {
    consumerGroupName = "$Default";
    outputField = "message";
    partitionIdField = "partitionId";
    offsetField= "offset";
    sequenceNumberField = "sequenceNumber";
    hostField = "host";
    enquedTimeField = "enquedTime";
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new AzureListener( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new AzureListenerData();
  }

  @Override public String getDialogClassName() {
    return AzureListenerDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) throws KettleStepException {

    // Output message field name
    //
    String outputFieldName = space.environmentSubstitute( outputField );
    if (StringUtils.isNotEmpty(outputFieldName)) {
      ValueMetaInterface outputValueMeta = new ValueMetaString(outputFieldName);
      rowMeta.addValueMeta( outputValueMeta );
    }

    // The partition ID field name
    //
    String partitionIdFieldName = space.environmentSubstitute( partitionIdField );
    if (StringUtils.isNotEmpty(partitionIdFieldName)) {
      ValueMetaInterface outputValueMeta = new ValueMetaString(partitionIdFieldName);
      rowMeta.addValueMeta( outputValueMeta );
    }

    // The offset field name
    //
    String offsetFieldName = space.environmentSubstitute( offsetField );
    if (StringUtils.isNotEmpty(offsetFieldName)) {
      ValueMetaInterface outputValueMeta = new ValueMetaString(offsetFieldName);
      rowMeta.addValueMeta( outputValueMeta );
    }

    // The sequence number field name
    //
    String sequenceNumberFieldName = space.environmentSubstitute( sequenceNumberField );
    if (StringUtils.isNotEmpty(sequenceNumberFieldName)) {
      ValueMetaInterface outputValueMeta = new ValueMetaInteger(sequenceNumberFieldName);
      rowMeta.addValueMeta( outputValueMeta );
    }

    // The host field name
    //
    String hostFieldName = space.environmentSubstitute( hostField );
    if (StringUtils.isNotEmpty(hostFieldName)) {
      ValueMetaInterface outputValueMeta = new ValueMetaString(hostFieldName);
      rowMeta.addValueMeta( outputValueMeta );
    }

    // The enqued time field name
    //
    String enquedTimeFieldName = space.environmentSubstitute( enquedTimeField );
    if (StringUtils.isNotEmpty(enquedTimeFieldName)) {
      ValueMetaInterface outputValueMeta = new ValueMetaTimestamp(enquedTimeFieldName);
      rowMeta.addValueMeta( outputValueMeta );
    }
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( NAMESPACE, namespace ) );
    xml.append( XMLHandler.addTagValue( EVENT_HUB_NAME, eventHubName ) );
    xml.append( XMLHandler.addTagValue( SAS_KEY_NAME, sasKeyName ) );
    xml.append( XMLHandler.addTagValue( SAS_KEY, Encr.encryptPasswordIfNotUsingVariables(sasKey) ) );
    xml.append( XMLHandler.addTagValue( BATCH_SIZE, batchSize ) );
    xml.append( XMLHandler.addTagValue( PREFETCH_SIZE, prefetchSize) );
    xml.append( XMLHandler.addTagValue( OUTPUT_FIELD, outputField ) );
    xml.append( XMLHandler.addTagValue( PARTITION_ID_FIELD, partitionIdField) );
    xml.append( XMLHandler.addTagValue( OFFSET_FIELD, offsetField ) );
    xml.append( XMLHandler.addTagValue( SEQUENCE_NUMBER_FIELD, sequenceNumberField) );
    xml.append( XMLHandler.addTagValue( HOST_FIELD, hostField ) );
    xml.append( XMLHandler.addTagValue( ENQUED_TIME_FIELD, enquedTimeField) );
    xml.append( XMLHandler.addTagValue( CONSUMER_GROUP_NAME, consumerGroupName ) );
    xml.append( XMLHandler.addTagValue( EVENT_HUB_CONNECTION_STRING, eventHubConnectionString ) );
    xml.append( XMLHandler.addTagValue( STORAGE_CONNECTION_STRING, storageConnectionString ) );
    xml.append( XMLHandler.addTagValue( STORAGE_CONTAINER_NAME, storageContainerName) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    namespace = XMLHandler.getTagValue( stepnode, NAMESPACE );
    eventHubName = XMLHandler.getTagValue( stepnode, EVENT_HUB_NAME );
    sasKeyName = XMLHandler.getTagValue( stepnode, SAS_KEY_NAME );
    sasKey = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode, SAS_KEY ) );
    batchSize = XMLHandler.getTagValue( stepnode, BATCH_SIZE );
    prefetchSize = XMLHandler.getTagValue( stepnode, PREFETCH_SIZE);
    outputField = XMLHandler.getTagValue( stepnode, OUTPUT_FIELD );
    partitionIdField = XMLHandler.getTagValue( stepnode, PARTITION_ID_FIELD);
    offsetField = XMLHandler.getTagValue( stepnode, OFFSET_FIELD );
    sequenceNumberField = XMLHandler.getTagValue( stepnode, SEQUENCE_NUMBER_FIELD);
    hostField = XMLHandler.getTagValue( stepnode, HOST_FIELD );
    enquedTimeField = XMLHandler.getTagValue( stepnode, ENQUED_TIME_FIELD);
    consumerGroupName = XMLHandler.getTagValue( stepnode, CONSUMER_GROUP_NAME);
    eventHubConnectionString = XMLHandler.getTagValue( stepnode, EVENT_HUB_CONNECTION_STRING);
    storageConnectionString = XMLHandler.getTagValue( stepnode, STORAGE_CONNECTION_STRING);
    storageContainerName = XMLHandler.getTagValue( stepnode, STORAGE_CONTAINER_NAME );
    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, NAMESPACE, namespace );
    rep.saveStepAttribute( id_transformation, id_step, EVENT_HUB_NAME, eventHubName );
    rep.saveStepAttribute( id_transformation, id_step, SAS_KEY_NAME, sasKeyName );
    rep.saveStepAttribute( id_transformation, id_step, SAS_KEY, Encr.encryptPasswordIfNotUsingVariables(sasKey) );
    rep.saveStepAttribute( id_transformation, id_step, BATCH_SIZE, batchSize );
    rep.saveStepAttribute( id_transformation, id_step, PREFETCH_SIZE, prefetchSize);
    rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FIELD, outputField );
    rep.saveStepAttribute( id_transformation, id_step, PARTITION_ID_FIELD, partitionIdField);
    rep.saveStepAttribute( id_transformation, id_step, OFFSET_FIELD, offsetField);
    rep.saveStepAttribute( id_transformation, id_step, SEQUENCE_NUMBER_FIELD, sequenceNumberField);
    rep.saveStepAttribute( id_transformation, id_step, HOST_FIELD, hostField);
    rep.saveStepAttribute( id_transformation, id_step, ENQUED_TIME_FIELD, enquedTimeField);
    rep.saveStepAttribute( id_transformation, id_step, CONSUMER_GROUP_NAME, consumerGroupName);
    rep.saveStepAttribute( id_transformation, id_step, EVENT_HUB_CONNECTION_STRING, eventHubConnectionString);
    rep.saveStepAttribute( id_transformation, id_step, STORAGE_CONNECTION_STRING, storageConnectionString);
    rep.saveStepAttribute( id_transformation, id_step, STORAGE_CONTAINER_NAME, storageContainerName );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    namespace = rep.getStepAttributeString( id_step, NAMESPACE );
    eventHubName = rep.getStepAttributeString( id_step, EVENT_HUB_NAME );
    sasKeyName = rep.getStepAttributeString( id_step, SAS_KEY_NAME );
    sasKey = Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( id_step, SAS_KEY ) );
    batchSize = rep.getStepAttributeString( id_step, BATCH_SIZE );
    prefetchSize = rep.getStepAttributeString( id_step, PREFETCH_SIZE);
    outputField = rep.getStepAttributeString( id_step, OUTPUT_FIELD );
    partitionIdField = rep.getStepAttributeString( id_step, PARTITION_ID_FIELD);
    offsetField = rep.getStepAttributeString( id_step, OFFSET_FIELD );
    sequenceNumberField = rep.getStepAttributeString( id_step, SEQUENCE_NUMBER_FIELD);
    hostField = rep.getStepAttributeString( id_step, HOST_FIELD );
    enquedTimeField = rep.getStepAttributeString( id_step, ENQUED_TIME_FIELD);
    consumerGroupName = rep.getStepAttributeString( id_step, CONSUMER_GROUP_NAME);
    eventHubConnectionString = rep.getStepAttributeString( id_step, EVENT_HUB_CONNECTION_STRING);
    storageConnectionString = rep.getStepAttributeString( id_step, STORAGE_CONNECTION_STRING);
    storageContainerName = rep.getStepAttributeString( id_step, STORAGE_CONTAINER_NAME );
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace( String namespace ) {
    this.namespace = namespace;
  }

  public String getEventHubName() {
    return eventHubName;
  }

  public void setEventHubName( String eventHubName ) {
    this.eventHubName = eventHubName;
  }

  public String getSasKeyName() {
    return sasKeyName;
  }

  public void setSasKeyName( String sasKeyName ) {
    this.sasKeyName = sasKeyName;
  }

  public String getSasKey() {
    return sasKey;
  }

  public void setSasKey( String sasKey ) {
    this.sasKey = sasKey;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField( String outputField ) {
    this.outputField = outputField;
  }

  public String getConsumerGroupName() {
    return consumerGroupName;
  }

  public void setConsumerGroupName( String consumerGroupName ) {
    this.consumerGroupName = consumerGroupName;
  }

  public String getEventHubConnectionString() {
    return eventHubConnectionString;
  }

  public void setEventHubConnectionString( String eventHubConnectionString ) {
    this.eventHubConnectionString = eventHubConnectionString;
  }

  public String getStorageConnectionString() {
    return storageConnectionString;
  }

  public void setStorageConnectionString( String storageConnectionString ) {
    this.storageConnectionString = storageConnectionString;
  }

  public String getStorageContainerName() {
    return storageContainerName;
  }

  public void setStorageContainerName( String storageContainerName ) {
    this.storageContainerName = storageContainerName;
  }

  public String getPrefetchSize() {
    return prefetchSize;
  }

  public void setPrefetchSize( String prefetchSize ) {
    this.prefetchSize = prefetchSize;
  }

  public String getPartitionIdField() {
    return partitionIdField;
  }

  public void setPartitionIdField( String partitionIdField ) {
    this.partitionIdField = partitionIdField;
  }

  public String getOffsetField() {
    return offsetField;
  }

  public void setOffsetField( String offsetField ) {
    this.offsetField = offsetField;
  }

  public String getSequenceNumberField() {
    return sequenceNumberField;
  }

  public void setSequenceNumberField( String sequenceNumberField ) {
    this.sequenceNumberField = sequenceNumberField;
  }

  public String getHostField() {
    return hostField;
  }

  public void setHostField( String hostField ) {
    this.hostField = hostField;
  }

  public String getEnquedTimeField() {
    return enquedTimeField;
  }

  public void setEnquedTimeField( String enquedTimeField ) {
    this.enquedTimeField = enquedTimeField;
  }
}
