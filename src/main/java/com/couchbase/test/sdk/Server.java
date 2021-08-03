package com.couchbase.test.sdk;

public class Server {
    public String ip;
    public String port;
    public String username = "root";
    public String password = "couchbase";
    public String rest_username = "Administrator";
    public String rest_password = "password";
    public String memcached_port = "11210";

    public Server(String ip, String port, String rest_username, String rest_password,
            String memcached_port) {
        super();
        this.ip = ip;
        this.port = port;
        this.rest_username = rest_username;
        this.rest_password = rest_password;
        this.memcached_port = memcached_port;
    }
}