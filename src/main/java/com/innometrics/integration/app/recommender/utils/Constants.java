package com.innometrics.integration.app.recommender.utils;

/**
 * @author andrew, Innometrics
 */
public class Constants {
    public static final String PARTITION_ID = "partitionId";
    public static final String ESTIMATION = "estimation";
    public static final String PREFERENCE = "preference";

    public static final String TOPOLOGY_TRAINING_SOURCE = "training_source";
    public static final String TOPOLOGY_TRAINING_PARTITION = "training_partition";
    public static final String TOPOLOGY_TRAINING_CALC = "training_calculation";
    public static final String TOPOLOGY_TRAINING_WRITING = "training_result_writing";
    public static final String TOPOLOGY_TRAINING_QE = "training_quality_evaluation";


    public static final String DEFAULT_STREAM = "default_stream";
    public static final String QE_STREAM = "quality_evaluation_stream";

    public static final String BOLT_IDX = "bolt_index";
    public static final String DEFAULT_ID_COLUMN = "id";
    public static final String TASTE_USER_ID_COLUMN = "user_id";
    public static final String TASTE_ITEM_ID_COLUMN = "item_id";
    public static final String TASTE_PREFERENCE_COLUMN = "preference";
    public static final String TASTE_TIMESTAMP_COLUMN = "timestamp";
    public static final String MYSQL_SERVICE_KEY = "p-mysql";
    public static final String DATABASE_KEY = "database";
    public static final String SERVICES_KEY = "VCAP_SERVICES";

    public static final String USER_TABLE = "user";
    public static final String USER_PROFILE_ID_COLUMN = "profileId";

    public static final String ITEM_TABLE = "item";
    public static final String ITEM_REFERENCE_COLUMN = "reference";
    public static final String ITEM_HIT_COLUMN = "hit";

    public static final String PROFILE_CLOUD_HOST = "profileCloud.host";
    public static final String PROFILE_CLOUD_PORT = "profileCloud.port";
    public static final String APP_KEY = "appKey";
    public static final String COMPANY_ID = "companyId";
    public static final String BUCKET_ID = "bucketId";
    public static final String APP_ID = "appId";


    public static final String ALLOWED_DOMAIN = "allowedDomain";

    public static final String RULE_COLLECTION_PREFIX = "RULE_";

    public static final String REQUEST_PARAM_PROFILE_ID = "profileId";
    public static final String REQUEST_PARAM_RULE_ID = "ruleId";
    public static final String REQUEST_PARAM_SIZE = "size";

    public static final String CROSS_REF_IDX = "crossRefIdx";
    public static final int NR_CROSS_REF = 2;


    public static int BATCH_LIMIT = 10000;
}
