package com.couchbase.test.rest;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;

public class RestClient {
    final static Logger logger = LogManager.getLogger(RestClient.class);
    private String host;
    private String port;
    private String user;
    private String password;

    public RestClient(String host, String port, String user, String password) {
        super();
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public String header(){
        String authString = this.user + ":" + this.password;
        byte[] authEncBytes = Base64.getEncoder().encode(authString.getBytes());
        String authStringEnc = new String(authEncBytes);
        return authStringEnc;
    }

    public JsonNode _http_request(String endpoint){
        return _http_request(endpoint, "GET", "");
    }

    public JsonNode _http_request(String endpoint, String method){
        return _http_request(endpoint, method, "");
    }

    public JsonNode _http_request(String endpoint, String method, String m){
        String output = null;
        try {
            String base_url = "http://"+this.host + ":" + this.port;

            URL url = new URL(base_url + endpoint);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(method);
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setRequestProperty("Authorization", "Basic " + header());
            if (m.length() > 0){
                // Send post request
                conn.setDoOutput(true);
                DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
                wr.writeBytes(m);
                wr.flush();
                wr.close();
            }
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK && 
                    conn.getResponseCode() != HttpURLConnection.HTTP_ACCEPTED) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            output =  br.lines().collect(Collectors.joining("\n"));
            conn.disconnect();

        } catch (IOException e) {
            e.printStackTrace();
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode actualObj = mapper.readTree(output);
            return actualObj;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
