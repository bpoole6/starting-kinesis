package com.austin.poole.starting;

import com.austin.poole.Constants;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Logger;

import static com.austin.poole.Constants.getKinesisClient;

public class Consumer {
  static Logger  logger = Logger.getLogger(Consumer.class.getName());

  public static void main(String[] args) throws InterruptedException {
    receiveEvents();
  }
  public static void receiveEvents() throws InterruptedException {

    KinesisClient kinesisClient = getKinesisClient();
    DescribeStreamResponse desc = kinesisClient
            .describeStream(DescribeStreamRequest.builder().streamName(Constants.STREAM_NAME).build());
    String shardId = desc.streamDescription().shards().get(0).shardId(); //

    // Prepare the shard iterator request with the stream name 
    // and identifier of the shard to which the record was written
    GetShardIteratorRequest getShardIteratorRequest
            = GetShardIteratorRequest
            .builder()
            .streamName(Constants.STREAM_NAME)
            .shardId(shardId)
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON.name())
            .build();

    GetShardIteratorResponse getShardIteratorResponse
            = kinesisClient
            .getShardIterator(getShardIteratorRequest);

    // Get the shard iterator from the Shard Iterator Response
    String shardIterator = getShardIteratorResponse.shardIterator();

    while (shardIterator != null) {
      // Prepare the get records request with the shardIterator
      GetRecordsRequest getRecordsRequest
              = GetRecordsRequest
              .builder()
              .shardIterator(shardIterator)
              .limit(300)
              .build();

      // Read the records from the shard
      GetRecordsResponse getRecordsResponse
              = kinesisClient.getRecords(getRecordsRequest);

      List<Record> records = getRecordsResponse.records();

      logger.info("count " + records.size());

      // log content of each record
      records.forEach(record -> {
        byte[] dataInBytes = record.data().asByteArray();
        logger.info("\t"+new String(dataInBytes, StandardCharsets.UTF_8));
      });

      shardIterator = getRecordsResponse.nextShardIterator();
      Thread.sleep(1000);
    }
  }

}