package com.austin.poole.starting;

import com.austin.poole.Constants;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

import static com.austin.poole.Constants.getKinesisClient;

public class Producer {

    private static final Logger logger =
            Logger.getLogger(Producer.class.getName());

    private static final String PARTITION_KEY = "partitionKey";

    public static void main(String[] args) {
        sendEvents();
    }


    public static void sendEvents() {
        KinesisClient kinesisClient = getKinesisClient();
        // Create collection of 5 PutRecordsRequestEntry objects
        // for adding to the Kinesis Data Stream
        List<PutRecordsRequestEntry> putRecordsRequestEntryList
                = new ArrayList<>();
        for (int i = 0; i < 5; i++) {

            String dataStr = generateDeviceData();
            SdkBytes data = SdkBytes.fromByteArray(dataStr.getBytes(StandardCharsets.UTF_8));

            PutRecordsRequestEntry putRecordsRequestEntry =
                    PutRecordsRequestEntry.builder()
                            .partitionKey(PARTITION_KEY) // Partition key
                            .data(data) // Data Blob
                            .build();

            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        // Create the request for putRecords method
        PutRecordsRequest putRecordsRequest
                = PutRecordsRequest
                .builder()
                .streamName(Constants.STREAM_NAME)
                .records(putRecordsRequestEntryList)
                .build();

        PutRecordsResponse putRecordsResult = kinesisClient
                .putRecords(putRecordsRequest);

        logger.info("Put records Result" + putRecordsResult);
        kinesisClient.close();
    }



    private static String generateDeviceData() {
        Random random = new Random();
        UUID id = UUID.randomUUID();
        int temperature = random.nextInt(999);
        return String.format("%s|%03d°C", id, temperature);
    }
}