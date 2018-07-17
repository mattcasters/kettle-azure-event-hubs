package com.neo4j.kettle.azure;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class TestRead {

  private static final int NR_ROWS = 10;

  public static void main( String[] args) throws Exception {

    final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
      .setNamespaceName("mattcasters")
      .setEventHubName("kettle")
      .setSasKeyName("KettleReadKey")
      .setSasKey("Endpoint=sb://mattcasters.servicebus.windows.net/;SharedAccessKeyName=KettleReadKey;SharedAccessKey=eLMX4Y3bgQw/+qvxGzPFx5F9QTT1RmTbj+Qds+a9TcE=");

    // Create the instance of EventProcessorHost using the most basic constructor. This constructor uses Azure Storage for
    // persisting partition leases and checkpoints. The host name, which identifies the instance of EventProcessorHost, must be unique.
    // You can use a plain UUID, or use the createHostName utility method which appends a UUID to a supplied string.
    //

    EventProcessorHost host = new EventProcessorHost(
      EventProcessorHost.createHostName("KettleHost"), // unique Hostname string
      "kettle", // Event hub name
      "$Default",  // consumer group name
      "Endpoint=sb://mattcasters.servicebus.windows.net/;SharedAccessKeyName=KettleReadKey;SharedAccessKey=eLMX4Y3bgQw/+qvxGzPFx5F9QTT1RmTbj+Qds+a9TcE=", // event hub connection string
      "DefaultEndpointsProtocol=https;AccountName=kettlestorage;AccountKey=7/hH2UHmnfNSXFLKK5WvCAkAs+GcrfsHv8uON05tpKgMah4ytKBxhf3ma7eJAyXOaINDtq2ngwjdf+VBTG7SGA==;EndpointSuffix=core.windows.net", // storage connection string
      "kettlestorage"  // storage container name
    );


    System.out.println("Registering host named " + host.getHostName());
    EventProcessorOptions options = new EventProcessorOptions();
    options.setExceptionNotification(new ErrorNotificationHandler());

    final Gson gson = new GsonBuilder().create();

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final EventHubClient eventHubClient = EventHubClient.createSync( connStr.toString(), executorService );

    host.registerEventProcessor(EventProcessor.class, options)
      .whenComplete((unused, e) -> {
        // whenComplete passes the result of the previous stage through unchanged,
        // which makes it useful for logging a result without side effects.
        //
        if (e != null)
        {
          System.out.println("Failure while registering: " + e.toString());
          if (e.getCause() != null)
          {
            System.out.println("Inner exception: " + e.getCause().toString());
          }
        }
      })
      .thenAccept((unused) -> {
        // This stage will only execute if registerEventProcessor succeeded.
        // If it completed exceptionally, this stage will be skipped.
        //
        System.out.println("Press enter to stop.");
        try {
          System.in.read();
        } catch (Exception e) {
          System.out.println("Keyboard read failed: " + e.toString());
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
        System.out.println("Failure while unregistering: " + e.toString());
        if (e.getCause() != null) {
          System.out.println("Inner exception: " + e.getCause().toString());
        }
        return null;
      })
      .get(); // Wait for everything to finish before exiting main!

  }





}
