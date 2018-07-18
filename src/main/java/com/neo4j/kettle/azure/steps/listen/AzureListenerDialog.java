package com.neo4j.kettle.azure.steps.listen;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class AzureListenerDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = AzureListenerMeta.class; // for i18n purposes, needed by Translator2!!
  
  private Text wStepname;
  private TextVar wNamespace;
  private TextVar wEventHub;
  private TextVar wSasKeyName;
  private TextVar wSasKey;
  private TextVar wBatchSize;
  private TextVar wPrefetchSize;
  private TextVar wOutputField;
  private TextVar wPartitionIdField;
  private TextVar wOffsetField;
  private TextVar wSequenceNumberField;
  private TextVar wHostField;
  private TextVar wEnquedTimeField;

  private TextVar wConsumerGroup;
  private TextVar wEventHubConnectionString;
  private TextVar wStorageConnectionString;
  private TextVar wStorageContainerName;

  
  private AzureListenerMeta input;

  public AzureListenerDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta)inputMetadata, transMeta, stepname );
    input = (AzureListenerMeta) inputMetadata;

    // Hack the metastore...
    //
    metaStore = Spoon.getInstance().getMetaStore();
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( "AzureListener" );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    //
    Label wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( "Step name" );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER);
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    // Name space
    //
    Label wlNamespace = new Label( shell, SWT.RIGHT );
    wlNamespace.setText( "Event Hubs namespace" );
    props.setLook( wlNamespace );
    FormData fdlNamespace = new FormData();
    fdlNamespace.left = new FormAttachment( 0, 0 );
    fdlNamespace.right = new FormAttachment( middle, -margin );
    fdlNamespace.top = new FormAttachment( lastControl, 2*margin );
    wlNamespace.setLayoutData( fdlNamespace );
    wNamespace = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNamespace );
    wNamespace.addModifyListener( lsMod );
    FormData fdNamespace = new FormData();
    fdNamespace.left = new FormAttachment( middle, 0 );
    fdNamespace.right = new FormAttachment( 100, 0 );
    fdNamespace.top = new FormAttachment( wlNamespace, 0, SWT.CENTER );
    wNamespace.setLayoutData( fdNamespace );
    lastControl = wNamespace;

    Label wlEventHub = new Label( shell, SWT.RIGHT );
    wlEventHub.setText( "Event Hubs Instance name" );
    props.setLook( wlEventHub );
    FormData fdlEventHub = new FormData();
    fdlEventHub.left = new FormAttachment( 0, 0 );
    fdlEventHub.right = new FormAttachment( middle, -margin );
    fdlEventHub.top = new FormAttachment( lastControl, 2*margin );
    wlEventHub.setLayoutData( fdlEventHub );
    wEventHub = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEventHub );
    wEventHub.addModifyListener( lsMod );
    FormData fdEventHub = new FormData();
    fdEventHub.left = new FormAttachment( middle, 0 );
    fdEventHub.right = new FormAttachment( 100, 0 );
    fdEventHub.top = new FormAttachment( wlEventHub, 0, SWT.CENTER );
    wEventHub.setLayoutData( fdEventHub );
    lastControl = wEventHub;

    Label wlEventHubConnectionString = new Label( shell, SWT.RIGHT );
    wlEventHubConnectionString.setText( "Event Hub Connection String" );
    props.setLook( wlEventHubConnectionString );
    FormData fdlEventHubConnectionString = new FormData();
    fdlEventHubConnectionString.left = new FormAttachment( 0, 0 );
    fdlEventHubConnectionString.right = new FormAttachment( middle, -margin );
    fdlEventHubConnectionString.top = new FormAttachment( lastControl, 2*margin );
    wlEventHubConnectionString.setLayoutData( fdlEventHubConnectionString );
    wEventHubConnectionString = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wEventHubConnectionString.setEchoChar( '*' );
    props.setLook( wEventHubConnectionString );
    wEventHubConnectionString.addModifyListener( lsMod );
    FormData fdEventHubConnectionString = new FormData();
    fdEventHubConnectionString.left = new FormAttachment( middle, 0 );
    fdEventHubConnectionString.right = new FormAttachment( 100, 0 );
    fdEventHubConnectionString.top = new FormAttachment( wlEventHubConnectionString, 0, SWT.CENTER );
    wEventHubConnectionString.setLayoutData( fdEventHubConnectionString );
    lastControl = wEventHubConnectionString;

    Label wlSasKeyName = new Label( shell, SWT.RIGHT );
    wlSasKeyName.setText( "SAS Policy key name" );
    props.setLook( wlSasKeyName );
    FormData fdlSasKeyName = new FormData();
    fdlSasKeyName.left = new FormAttachment( 0, 0 );
    fdlSasKeyName.right = new FormAttachment( middle, -margin );
    fdlSasKeyName.top = new FormAttachment( lastControl, 2*margin );
    wlSasKeyName.setLayoutData( fdlSasKeyName );
    wSasKeyName = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSasKeyName );
    wSasKeyName.addModifyListener( lsMod );
    FormData fdSasKeyName = new FormData();
    fdSasKeyName.left = new FormAttachment( middle, 0 );
    fdSasKeyName.right = new FormAttachment( 100, 0 );
    fdSasKeyName.top = new FormAttachment( wlSasKeyName, 0, SWT.CENTER );
    wSasKeyName.setLayoutData( fdSasKeyName );
    lastControl = wSasKeyName;

    Label wlSasKey = new Label( shell, SWT.RIGHT );
    wlSasKey.setText( "SAS Key connection string" );
    props.setLook( wlSasKey );
    FormData fdlSasKey = new FormData();
    fdlSasKey.left = new FormAttachment( 0, 0 );
    fdlSasKey.right = new FormAttachment( middle, -margin );
    fdlSasKey.top = new FormAttachment( lastControl, 2*margin );
    wlSasKey.setLayoutData( fdlSasKey );
    wSasKey = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSasKey.setEchoChar( '*' );
    props.setLook( wSasKey );
    wSasKey.addModifyListener( lsMod );
    FormData fdSasKey = new FormData();
    fdSasKey.left = new FormAttachment( middle, 0 );
    fdSasKey.right = new FormAttachment( 100, 0 );
    fdSasKey.top = new FormAttachment( wlSasKey, 0, SWT.CENTER );
    wSasKey.setLayoutData( fdSasKey );
    lastControl = wSasKey;

    Label wlConsumerGroup = new Label( shell, SWT.RIGHT );
    wlConsumerGroup.setText( "Consumer Group Name" );
    props.setLook( wlConsumerGroup );
    FormData fdlConsumerGroup = new FormData();
    fdlConsumerGroup.left = new FormAttachment( 0, 0 );
    fdlConsumerGroup.right = new FormAttachment( middle, -margin );
    fdlConsumerGroup.top = new FormAttachment( lastControl, 2*margin );
    wlConsumerGroup.setLayoutData( fdlConsumerGroup );
    wConsumerGroup = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConsumerGroup );
    wConsumerGroup.addModifyListener( lsMod );
    FormData fdConsumerGroup = new FormData();
    fdConsumerGroup.left = new FormAttachment( middle, 0 );
    fdConsumerGroup.right = new FormAttachment( 100, 0 );
    fdConsumerGroup.top = new FormAttachment( wlConsumerGroup, 0, SWT.CENTER );
    wConsumerGroup.setLayoutData( fdConsumerGroup );
    lastControl = wConsumerGroup;

    Label wlStorageContainerName = new Label( shell, SWT.RIGHT );
    wlStorageContainerName.setText( "Storage Container name" );
    props.setLook( wlStorageContainerName );
    FormData fdlStorageContainerName = new FormData();
    fdlStorageContainerName.left = new FormAttachment( 0, 0 );
    fdlStorageContainerName.right = new FormAttachment( middle, -margin );
    fdlStorageContainerName.top = new FormAttachment( lastControl, 2*margin );
    wlStorageContainerName.setLayoutData( fdlStorageContainerName );
    wStorageContainerName = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStorageContainerName );
    wStorageContainerName.addModifyListener( lsMod );
    FormData fdStorageContainerName = new FormData();
    fdStorageContainerName.left = new FormAttachment( middle, 0 );
    fdStorageContainerName.right = new FormAttachment( 100, 0 );
    fdStorageContainerName.top = new FormAttachment( wlStorageContainerName, 0, SWT.CENTER );
    wStorageContainerName.setLayoutData( fdStorageContainerName );
    lastControl = wStorageContainerName;

    Label wlStorageConnectionString = new Label( shell, SWT.RIGHT );
    wlStorageConnectionString.setText( "Storage Connection String" );
    props.setLook( wlStorageConnectionString );
    FormData fdlStorageConnectionString = new FormData();
    fdlStorageConnectionString.left = new FormAttachment( 0, 0 );
    fdlStorageConnectionString.right = new FormAttachment( middle, -margin );
    fdlStorageConnectionString.top = new FormAttachment( lastControl, 2*margin );
    wlStorageConnectionString.setLayoutData( fdlStorageConnectionString );
    wStorageConnectionString = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStorageConnectionString.setEchoChar( '*' );
    props.setLook( wStorageConnectionString );
    wStorageConnectionString.addModifyListener( lsMod );
    FormData fdStorageConnectionString = new FormData();
    fdStorageConnectionString.left = new FormAttachment( middle, 0 );
    fdStorageConnectionString.right = new FormAttachment( 100, 0 );
    fdStorageConnectionString.top = new FormAttachment( wlStorageConnectionString, 0, SWT.CENTER );
    wStorageConnectionString.setLayoutData( fdStorageConnectionString );
    lastControl = wStorageConnectionString;


    Label wlBatchSize = new Label( shell, SWT.RIGHT );
    wlBatchSize.setText( "Batch size" );
    props.setLook( wlBatchSize );
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.right = new FormAttachment( middle, -margin );
    fdlBatchSize.top = new FormAttachment( lastControl, 2*margin );
    wlBatchSize.setLayoutData( fdlBatchSize );
    wBatchSize = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( lsMod );
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( middle, 0 );
    fdBatchSize.right = new FormAttachment( 100, 0 );
    fdBatchSize.top = new FormAttachment( wlBatchSize, 0, SWT.CENTER );
    wBatchSize.setLayoutData( fdBatchSize );
    lastControl = wBatchSize;

    Label wlPrefetchSize = new Label( shell, SWT.RIGHT );
    wlPrefetchSize.setText( "Prefetch size" );
    props.setLook( wlPrefetchSize );
    FormData fdlPrefetchSize = new FormData();
    fdlPrefetchSize.left = new FormAttachment( 0, 0 );
    fdlPrefetchSize.right = new FormAttachment( middle, -margin );
    fdlPrefetchSize.top = new FormAttachment( lastControl, 2*margin );
    wlPrefetchSize.setLayoutData( fdlPrefetchSize );
    wPrefetchSize = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPrefetchSize );
    wPrefetchSize.addModifyListener( lsMod );
    FormData fdPrefetchSize = new FormData();
    fdPrefetchSize.left = new FormAttachment( middle, 0 );
    fdPrefetchSize.right = new FormAttachment( 100, 0 );
    fdPrefetchSize.top = new FormAttachment( wlPrefetchSize, 0, SWT.CENTER );
    wPrefetchSize.setLayoutData( fdPrefetchSize );
    lastControl = wPrefetchSize;

    Label wlOutputField = new Label( shell, SWT.RIGHT );
    wlOutputField.setText( "Message (data) output field name" );
    props.setLook( wlOutputField );
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment( 0, 0 );
    fdlOutputField.right = new FormAttachment( middle, -margin );
    fdlOutputField.top = new FormAttachment( lastControl, 2*margin );
    wlOutputField.setLayoutData( fdlOutputField );
    wOutputField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputField );
    wOutputField.addModifyListener( lsMod );
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment( middle, 0 );
    fdOutputField.right = new FormAttachment( 100, 0 );
    fdOutputField.top = new FormAttachment( wlOutputField, 0, SWT.CENTER );
    wOutputField.setLayoutData( fdOutputField );
    lastControl = wOutputField;

    Label wlPartitionIdField = new Label( shell, SWT.RIGHT );
    wlPartitionIdField.setText( "Partition ID field name" );
    props.setLook( wlPartitionIdField );
    FormData fdlPartitionIdField = new FormData();
    fdlPartitionIdField.left = new FormAttachment( 0, 0 );
    fdlPartitionIdField.right = new FormAttachment( middle, -margin );
    fdlPartitionIdField.top = new FormAttachment( lastControl, 2*margin );
    wlPartitionIdField.setLayoutData( fdlPartitionIdField );
    wPartitionIdField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPartitionIdField );
    wPartitionIdField.addModifyListener( lsMod );
    FormData fdPartitionIdField = new FormData();
    fdPartitionIdField.left = new FormAttachment( middle, 0 );
    fdPartitionIdField.right = new FormAttachment( 100, 0 );
    fdPartitionIdField.top = new FormAttachment( wlPartitionIdField, 0, SWT.CENTER );
    wPartitionIdField.setLayoutData( fdPartitionIdField );
    lastControl = wPartitionIdField;

    Label wlOffsetField = new Label( shell, SWT.RIGHT );
    wlOffsetField.setText( "Offset field name" );
    props.setLook( wlOffsetField );
    FormData fdlOffsetField = new FormData();
    fdlOffsetField.left = new FormAttachment( 0, 0 );
    fdlOffsetField.right = new FormAttachment( middle, -margin );
    fdlOffsetField.top = new FormAttachment( lastControl, 2*margin );
    wlOffsetField.setLayoutData( fdlOffsetField );
    wOffsetField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOffsetField );
    wOffsetField.addModifyListener( lsMod );
    FormData fdOffsetField = new FormData();
    fdOffsetField.left = new FormAttachment( middle, 0 );
    fdOffsetField.right = new FormAttachment( 100, 0 );
    fdOffsetField.top = new FormAttachment( wlOffsetField, 0, SWT.CENTER );
    wOffsetField.setLayoutData( fdOffsetField );
    lastControl = wOffsetField;

    Label wlSequenceNumberField = new Label( shell, SWT.RIGHT );
    wlSequenceNumberField.setText( "Sequence number field name" );
    props.setLook( wlSequenceNumberField );
    FormData fdlSequenceNumberField = new FormData();
    fdlSequenceNumberField.left = new FormAttachment( 0, 0 );
    fdlSequenceNumberField.right = new FormAttachment( middle, -margin );
    fdlSequenceNumberField.top = new FormAttachment( lastControl, 2*margin );
    wlSequenceNumberField.setLayoutData( fdlSequenceNumberField );
    wSequenceNumberField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSequenceNumberField );
    wSequenceNumberField.addModifyListener( lsMod );
    FormData fdSequenceNumberField = new FormData();
    fdSequenceNumberField.left = new FormAttachment( middle, 0 );
    fdSequenceNumberField.right = new FormAttachment( 100, 0 );
    fdSequenceNumberField.top = new FormAttachment( wlSequenceNumberField, 0, SWT.CENTER );
    wSequenceNumberField.setLayoutData( fdSequenceNumberField );
    lastControl = wSequenceNumberField;
    
    Label wlHostField = new Label( shell, SWT.RIGHT );
    wlHostField.setText( "Host (owner) field name" );
    props.setLook( wlHostField );
    FormData fdlHostField = new FormData();
    fdlHostField.left = new FormAttachment( 0, 0 );
    fdlHostField.right = new FormAttachment( middle, -margin );
    fdlHostField.top = new FormAttachment( lastControl, 2*margin );
    wlHostField.setLayoutData( fdlHostField );
    wHostField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostField );
    wHostField.addModifyListener( lsMod );
    FormData fdHostField = new FormData();
    fdHostField.left = new FormAttachment( middle, 0 );
    fdHostField.right = new FormAttachment( 100, 0 );
    fdHostField.top = new FormAttachment( wlHostField, 0, SWT.CENTER );
    wHostField.setLayoutData( fdHostField );
    lastControl = wHostField;
    
    Label wlEnquedTimeField = new Label( shell, SWT.RIGHT );
    wlEnquedTimeField.setText( "Enqued time field name" );
    props.setLook( wlEnquedTimeField );
    FormData fdlEnquedTimeField = new FormData();
    fdlEnquedTimeField.left = new FormAttachment( 0, 0 );
    fdlEnquedTimeField.right = new FormAttachment( middle, -margin );
    fdlEnquedTimeField.top = new FormAttachment( lastControl, 2*margin );
    wlEnquedTimeField.setLayoutData( fdlEnquedTimeField );
    wEnquedTimeField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnquedTimeField );
    wEnquedTimeField.addModifyListener( lsMod );
    FormData fdEnquedTimeField = new FormData();
    fdEnquedTimeField.left = new FormAttachment( middle, 0 );
    fdEnquedTimeField.right = new FormAttachment( 100, 0 );
    fdEnquedTimeField.top = new FormAttachment( wlEnquedTimeField, 0, SWT.CENTER );
    wEnquedTimeField.setLayoutData( fdEnquedTimeField );
    lastControl = wEnquedTimeField;

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK, wCancel }, margin, lastControl );

    // Add listeners
    lsCancel = e -> cancel();
    lsOK = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wNamespace.addSelectionListener( lsDef );
    wEventHub.addSelectionListener( lsDef );
    wSasKeyName.addSelectionListener( lsDef );
    wSasKey.addSelectionListener( lsDef );
    wBatchSize.addSelectionListener( lsDef );
    wPrefetchSize.addSelectionListener( lsDef );
    wConsumerGroup.addSelectionListener( lsDef );
    wEventHubConnectionString.addSelectionListener( lsDef );
    wStorageConnectionString.addSelectionListener( lsDef );
    wStorageContainerName.addSelectionListener( lsDef );

    wOutputField.addSelectionListener( lsDef );
    wPartitionIdField.addSelectionListener( lsDef );
    wOffsetField.addSelectionListener( lsDef );
    wSequenceNumberField.addSelectionListener( lsDef );
    wHostField.addSelectionListener( lsDef );
    wEnquedTimeField.addSelectionListener( lsDef );


    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;

  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  public void getData() {
    wStepname.setText( Const.NVL( stepname, "" ) );
    wNamespace.setText(Const.NVL(input.getNamespace(), "") );
    wEventHub.setText( Const.NVL(input.getEventHubName(), "") );
    wSasKeyName.setText(Const.NVL(input.getSasKeyName(), "") );
    wSasKey.setText(Const.NVL(input.getSasKey(), "") );
    wConsumerGroup.setText(Const.NVL(input.getConsumerGroupName(), "") );
    wEventHubConnectionString.setText(Const.NVL(input.getEventHubConnectionString(), "") );
    wStorageContainerName.setText(Const.NVL(input.getStorageContainerName(), "") );
    wStorageConnectionString.setText(Const.NVL(input.getStorageConnectionString(), "") );
    wBatchSize.setText(Const.NVL(input.getBatchSize(), "") );
    wPrefetchSize.setText(Const.NVL(input.getPrefetchSize(), "") );
    wOutputField.setText(Const.NVL(input.getOutputField(), ""));
    wPartitionIdField.setText(Const.NVL(input.getPartitionIdField(), ""));
    wOffsetField.setText(Const.NVL(input.getOffsetField(), ""));
    wHostField.setText(Const.NVL(input.getHostField(), ""));
    wSequenceNumberField.setText(Const.NVL(input.getSequenceNumberField(), ""));
    wEnquedTimeField.setText(Const.NVL(input.getEnquedTimeField(), ""));
  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    input.setNamespace( wNamespace.getText() );
    input.setEventHubName( wEventHub.getText() );
    input.setSasKeyName( wSasKeyName.getText() );
    input.setSasKey( wSasKey.getText() );
    input.setConsumerGroupName( wConsumerGroup.getText() );
    input.setEventHubConnectionString( wEventHubConnectionString.getText() );
    input.setStorageContainerName( wStorageContainerName.getText() );
    input.setStorageConnectionString( wStorageConnectionString.getText() );
    input.setBatchSize( wBatchSize.getText() );
    input.setPrefetchSize( wPrefetchSize.getText() );
    input.setOutputField( wOutputField.getText() );
    input.setPartitionIdField( wPartitionIdField.getText() );
    input.setOffsetField( wOffsetField.getText() );
    input.setSequenceNumberField( wSequenceNumberField.getText() );
    input.setHostField( wHostField.getText() );
    input.setEnquedTimeField( wEnquedTimeField.getText() );

    dispose();
  }
}
