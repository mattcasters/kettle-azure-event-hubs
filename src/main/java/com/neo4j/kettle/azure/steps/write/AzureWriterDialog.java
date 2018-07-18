package com.neo4j.kettle.azure.steps.write;

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
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class AzureWriterDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = AzureWriterMeta.class; // for i18n purposes, needed by Translator2!!
  
  private Text wStepname;
  private TextVar wNamespace;
  private TextVar wEventHub;
  private TextVar wSasKeyName;
  private TextVar wSasKey;
  private TextVar wBatchSize;
  private ComboVar wMessageField;


  private AzureWriterMeta input;

  public AzureWriterDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta)inputMetadata, transMeta, stepname );
    input = (AzureWriterMeta) inputMetadata;

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
    wlEventHub.setText( "Event Hubs instance name" );
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

    Label wlMessageField = new Label( shell, SWT.RIGHT );
    wlMessageField.setText( "Message field" );
    props.setLook( wlMessageField );
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment( 0, 0 );
    fdlMessageField.right = new FormAttachment( middle, -margin );
    fdlMessageField.top = new FormAttachment( lastControl, 2*margin );
    wlMessageField.setLayoutData( fdlMessageField );
    wMessageField = new ComboVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageField );
    wMessageField.addModifyListener( lsMod );
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment( middle, 0 );
    fdMessageField.right = new FormAttachment( 100, 0 );
    fdMessageField.top = new FormAttachment( wlMessageField, 0, SWT.CENTER );
    wMessageField.setLayoutData( fdMessageField );
    lastControl = wMessageField;

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
    wBatchSize.setText(Const.NVL(input.getBatchSize(), "") );
    wMessageField.setText(Const.NVL(input.getMessageField(), ""));

    try {
      wMessageField.setItems( transMeta.getPrevStepFields( stepname ).getFieldNames() );
    } catch ( KettleStepException e ) {
      // Ignore
    }
  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    input.setNamespace( wNamespace.getText() );
    input.setEventHubName( wEventHub.getText() );
    input.setBatchSize( wBatchSize.getText() );
    input.setSasKeyName( wSasKeyName.getText() );
    input.setSasKey( wSasKey.getText() );
    input.setMessageField( wMessageField.getText() );
    dispose();
  }
}
