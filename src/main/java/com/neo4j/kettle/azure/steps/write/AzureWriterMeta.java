package com.neo4j.kettle.azure.steps.write;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
  id = "AzureWriter",
  name = "Microsoft Azure Event Hubs Writer",
  description = "Write data to a Microsoft Azure Event Hub",
  image = "azure_writer.svg",
  categoryDescription = "Streaming"
)
public class AzureWriterMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String NAMESPACE = "namespace";
  public static final String EVENT_HUB_NAME = "event_hub_name";
  public static final String SAS_KEY_NAME = "sas_key_name";
  public static final String SAS_KEY = "sas_key";
  public static final String BATCH_SIZE = "batch_size";
  public static final String MESSAGE_FIELD = "message_field";


  private String namespace;
  private String eventHubName;
  private String sasKeyName;
  private String sasKey;

  private String batchSize;

  private String messageField;

  public AzureWriterMeta() {
    super();
  }

  @Override public void setDefault() {

  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new AzureWrite( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new AzureWriterData();
  }

  @Override public String getDialogClassName() {
    return AzureWriterDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // No output fields for now
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( NAMESPACE, namespace ) );
    xml.append( XMLHandler.addTagValue( EVENT_HUB_NAME, eventHubName ) );
    xml.append( XMLHandler.addTagValue( SAS_KEY_NAME, sasKeyName ) );
    xml.append( XMLHandler.addTagValue( SAS_KEY, Encr.encryptPasswordIfNotUsingVariables(sasKey) ) );
    xml.append( XMLHandler.addTagValue( BATCH_SIZE, batchSize ) );
    xml.append( XMLHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    namespace = XMLHandler.getTagValue( stepnode, NAMESPACE );
    eventHubName = XMLHandler.getTagValue( stepnode, EVENT_HUB_NAME );
    sasKeyName = XMLHandler.getTagValue( stepnode, SAS_KEY_NAME );
    sasKey = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode, SAS_KEY ) );
    batchSize = XMLHandler.getTagValue( stepnode, BATCH_SIZE );
    messageField = XMLHandler.getTagValue( stepnode, MESSAGE_FIELD );

    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, NAMESPACE, namespace );
    rep.saveStepAttribute( id_transformation, id_step, EVENT_HUB_NAME, eventHubName );
    rep.saveStepAttribute( id_transformation, id_step, SAS_KEY_NAME, sasKeyName );
    rep.saveStepAttribute( id_transformation, id_step, SAS_KEY, Encr.encryptPasswordIfNotUsingVariables(sasKey) );
    rep.saveStepAttribute( id_transformation, id_step, BATCH_SIZE, batchSize );
    rep.saveStepAttribute( id_transformation, id_step, MESSAGE_FIELD, messageField);
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    namespace = rep.getStepAttributeString( id_step, NAMESPACE );
    eventHubName = rep.getStepAttributeString( id_step, EVENT_HUB_NAME );
    sasKeyName = rep.getStepAttributeString( id_step, SAS_KEY_NAME );
    sasKey = Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( id_step, SAS_KEY ) );
    batchSize = rep.getStepAttributeString( id_step, BATCH_SIZE );
    messageField = rep.getStepAttributeString( id_step, MESSAGE_FIELD );
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

  public String getMessageField() {
    return messageField;
  }

  public void setMessageField( String messageField ) {
    this.messageField = messageField;
  }
}
