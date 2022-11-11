package com.couchbase.test.sdk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.Lock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.test.sdk.SDKClient;

public class SDKClientPool {
    static Logger logger = LogManager.getLogger(SDKClientPool.class);
    private HashMap<String, HashMap> clients;

    public SDKClientPool() {
        super();
        this.clients = new HashMap<String, HashMap>();
    }

    public void shutdown() {
        logger.debug("Closing clients from SDKClientPool");
        ArrayList<SDKClient> sdk_clients;
        for(Map.Entry<String, HashMap> m: this.clients.entrySet()){
            sdk_clients = (ArrayList)(m.getValue()).get("idle_clients");
            sdk_clients.addAll((ArrayList)m.getValue().get("busy_clients"));
            for(SDKClient sdk_client: sdk_clients)
                sdk_client.disconnectCluster();
        }
        // Reset the clients HM
        this.clients = new HashMap<String, HashMap>();
    }

    public void force_close_clients_for_bucket(String bucket_name) {
        if (! this.clients.containsKey(bucket_name))
            return;

        HashMap<String, Object> hm = this.clients.get(bucket_name);
        ArrayList<SDKClient> sdk_clients;
        sdk_clients = (ArrayList)(hm.get("idle_clients"));
        sdk_clients.addAll((ArrayList)hm.get("busy_clients"));
        for(SDKClient sdk_client: sdk_clients) {
            sdk_client.disconnectCluster();
        }
        this.clients.remove(bucket_name);
    }

    public void create_clients(String bucket_name, Server server, int req_clients) throws Exception {
        HashMap<String, Object> bucket_hm;
        if (this.clients.containsKey(bucket_name))
            bucket_hm = this.clients.get(bucket_name);
        else {
            bucket_hm = new HashMap<String, Object>();
            bucket_hm.put("lock", new Object());
            bucket_hm.put("idle_clients", new ArrayList<SDKClient>());
            bucket_hm.put("busy_clients", new ArrayList<SDKClient>());
            this.clients.put(bucket_name, bucket_hm);
        }

        for(int i=0; i<req_clients; i++) {
            SDKClient tem_client = new SDKClient(server, bucket_name);
            tem_client.initialiseSDK();
            ((ArrayList)bucket_hm.get("idle_clients")).add(tem_client);
        }
    }

    public SDKClient get_client_for_bucket(String bucket_name, String scope, String collection) {
        if (! this.clients.containsKey(bucket_name))
            return null;

        SDKClient client = null;
        String col_name = scope + collection;
        HashMap<String, Object> col_hm;
        HashMap<String, Object> hm = this.clients.get(bucket_name);
        while (client == null) {
            synchronized(hm.get("lock")) {
                if (hm.containsKey(col_name)) {
                    col_hm = (HashMap)hm.get(col_name);
                    // Increment tasks' reference counter using this client object
                    client = (SDKClient)col_hm.get("client");
                    col_hm.replace("counter", (int)col_hm.get("counter")+1);
                }
                else if (! ((ArrayList)hm.get("idle_clients")).isEmpty()) {
                    ArrayList idle_clients = (ArrayList)hm.get("idle_clients");
                    client = (SDKClient)idle_clients.remove(idle_clients.size()-1);
                    client.selectCollection(scope, collection);
                    ((ArrayList)hm.get("busy_clients")).add(client);
                    // Create scope/collection reference using the client object
                    col_hm = new HashMap<String, Object>();
                    hm.put(col_name, col_hm);
                    col_hm.put("client", client);
                    col_hm.put("counter", 1);
                }
            }
        }
        return client;
    }

    public void release_client(SDKClient client) {
        if (! this.clients.containsKey(client.bucket))
            return;

        HashMap<String, Object> hm = this.clients.get(client.bucket);
        String col_name = client.scope + client.collection;
        synchronized(hm.get("lock")) {
            if ((int)((HashMap)hm.get(col_name)).get("counter") == 1) {
                hm.remove(col_name);
                ((ArrayList)hm.get("busy_clients")).remove(client);
                ((ArrayList)hm.get("idle_clients")).add(client);
            }
            else
                ((HashMap)hm.get(col_name)).replace("counter", (int)((HashMap)hm.get(col_name)).get("counter") - 1);
        }
    }
}
