package com.couchbase.test.sdk;

import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

public class SDKClient {
    static Logger logger = LogManager.getLogger(SDKClient.class);

    public Server master;
    public String bucket;
    public String scope;
    public String collection;

    private Bucket bucketObj;
    private Cluster cluster;

    public Collection connection;

    public SDKClient(Server master, String bucket, String scope, String collection) {
        super();
        this.master = master;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = collection;
    }

    public SDKClient() {
        super();
    }

    public void initialiseSDK() throws Exception {
        logger.info("Connection to the cluster");
        this.connectCluster();
        this.connectBucket(bucket);
        this.selectCollection(scope, collection);
    }

    public void connectCluster(){
        try{
            ClusterEnvironment env = ClusterEnvironment.builder()
//                    .seedNodes(SeedNode.create(this.server.ip, Optional.of(Integer.parseInt(this.server.memcached_port)), Optional.of(Integer.parseInt(this.server.port))))
                    .timeoutConfig(TimeoutConfig.builder().kvTimeout(Duration.ofSeconds(10)))
                    .build();
            ClusterOptions cluster_options = ClusterOptions.clusterOptions(master.rest_username, master.rest_password).environment(env);
            this.cluster = Cluster.connect(master.ip, cluster_options);
            logger.info("Cluster connection is successful");
        }
        catch (AuthenticationFailureException e) {
            logger.info(String.format("cannot login from user: %s/%s",master.rest_username, master.rest_password));
        }
    }

    public void disconnectCluster(){
        // Disconnect and close all buckets
        this.cluster.disconnect();
        // Just close an environment
        this.cluster.environment().shutdown();
    }

    private void connectBucket(String bucket){
        this.bucketObj = this.cluster.bucket(bucket);
    }

    private void selectCollection(String scope, String collection) {
        this.connection = this.bucketObj.scope(scope).collection(collection);
    }
}
