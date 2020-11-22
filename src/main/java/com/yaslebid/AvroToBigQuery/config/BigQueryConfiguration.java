package com.yaslebid.AvroToBigQuery.config;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class BigQueryConfiguration {

    public static final String PROJECT_ID = "avroparserapplication";

    public static final String DATASET_NAME = "ClientDS";
    public static final String TABLE_CLIENT = "Client";
    public static final String TABLE_CLIENT_MANDATORY = "ClientMandatory";

    public static final Storage STORAGE = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    public static final String BUCKET_NAME = "yl_avro_bucket";
    public static final String BUCKET_ID = "gs://".concat(BUCKET_NAME + "/");
    public static final Bucket BUCKET = STORAGE.get(BUCKET_NAME);

    public static final String SUBSCRIPTION_NAME = "projects/avroparserapplication/subscriptions/avroParserSubscription";

}
