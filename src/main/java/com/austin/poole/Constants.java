package com.austin.poole;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class Constants {
    static{
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
    }

    public static final String STREAM_NAME = "first_data_stream";
    public static final Region REGION = Region.US_EAST_2;

    public static KinesisClient getKinesisClient() {
        return KinesisClient.builder().region(REGION).build();
    }
}
