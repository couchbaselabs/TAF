package com.mongo.loader;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.couchbase.test.sdk.Server;

public class MongoSDKClient {

	static Logger logger = LogManager.getLogger(MongoSDKClient.class);
	private MongoDatabase database;
	public MongoCollection<Document> collection;
    public Server master;
	private String databaseName;
	private String collectionName;
	private MongoClient mongoClient;
	private MongoClientSettings settings;

	public MongoSDKClient(Server master, String databaseName, String collectionName, boolean atlas) {
		String initials = "mongodb";
		if(atlas) {
			initials = initials + "+srv";
		}
		initials = initials + "://" + master.rest_username + ":" + master.rest_password + "@";
		this.settings = MongoClientSettings.builder()
				.applyConnectionString(new ConnectionString(initials + master.ip + ":" + master.port))
				.build();

		this.databaseName = databaseName;
		this.collectionName = collectionName;
	}
	
	public void connectCluster() {
		System.out.println("Connection Database: " + databaseName);
		System.out.println("Connecting Collection: " + collectionName);

		this.mongoClient = MongoClients.create(this.settings);
		this.database = this.mongoClient.getDatabase(this.databaseName);
		WriteConcern wc = new WriteConcern(0).withJournal(false);
		this.collection = this.database.getCollection(collectionName).withWriteConcern(wc);
	}

	public void disconnectCluster(){
        // Disconnect and close all buckets
        this.mongoClient.close();
    }
	
    public void dropDatabase(){
        // Disconnect and close all buckets
        this.database.drop();
    }
}