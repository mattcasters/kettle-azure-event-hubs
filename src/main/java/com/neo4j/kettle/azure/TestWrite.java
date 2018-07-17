package com.neo4j.kettle.azure;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestWrite {

  private static final int NR_ROWS = 10;

  public static void main( String[] args) throws Exception {

    final ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder()
      .setNamespaceName("mattcasters")
      .setEventHubName("kettle")
      .setSasKeyName("KettleWriterKey")
      .setSasKey("Endpoint=sb://mattcasters.servicebus.windows.net/;SharedAccessKeyName=KettleWriterKey;SharedAccessKey=o1XuSIHhU/ZPYNQQbXoePwzrfRAkxcIKnnwbaImvV8c=");

    final Gson gson = new GsonBuilder().create();

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final EventHubClient eventHubClient = EventHubClient.createSync( connectionStringBuilder.toString(), executorService );

    for (int i=0;i<NR_ROWS;i++) {
      System.out.println("Writing message #"+(i+1)+" / "+NR_ROWS);
      String payload = "Test Message " + Integer.toString( i );
      byte[] payloadBytes = gson.toJson( payload ).getBytes( Charset.defaultCharset() );
      EventData sendEvent = EventData.create( payloadBytes );

      // NOTE: To batch 'm up, send a LinkedList<EventData>
      // ALSO: We can add a Partition option using an input field or variable text
      //
      eventHubClient.sendSync( sendEvent );
    }

    System.out.println("Done");

    // close the client at the end of your program
    //
    eventHubClient.closeSync();

    // Shut down the executor service as well
    //
    executorService.shutdown();
  }
}
