package com.neo4j.kettle.azure;

import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;

import java.util.function.Consumer;

  // The general notification handler is an object that derives from Consumer<> and takes an ExceptionReceivedEventArgs object
  // as an argument. The argument provides the details of the error: the exception that occurred and the action (what EventProcessorHost
  // was doing) during which the error occurred. The complete list of actions can be found in EventProcessorHostActionStrings.\
  //
  public class ErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs>{

    @Override
    public void accept(ExceptionReceivedEventArgs t) {
      System.out.println("SAMPLE: Host " + t.getHostname() + " received general error notification during " + t.getAction() + ": " + t.getException().toString());
    }
  }