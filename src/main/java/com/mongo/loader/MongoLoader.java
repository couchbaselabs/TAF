package com.mongo.loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.test.docgen.DocRange;
import com.couchbase.test.docgen.WorkLoadSettings;
import com.couchbase.test.sdk.Server;
import com.couchbase.test.taskmanager.TaskManager;

import com.couchbase.test.docgen.DRConstants;

public class MongoLoader {
    static Logger logger = LogManager.getLogger(MongoLoader.class);

    public static void main(String[] args) {

        logger.info("#################### Starting Java Based Doc-Loader ####################");

        Options options = new Options();

        Option name = new Option("n", "node", true, "IP Address");
        name.setRequired(true);
        options.addOption(name);

        Option rest_username = new Option("user", "username", true, "Username");
        rest_username.setRequired(true);
        options.addOption(rest_username);

        Option rest_password = new Option("pwd", "password", true, "Password");
        rest_password.setRequired(true);
        options.addOption(rest_password);

        Option bucket = new Option("b", "bucket", true, "Bucket");
        bucket.setRequired(true);
        options.addOption(bucket);

        Option scope = new Option("scope", true, "Scope");
        options.addOption(scope);

        Option collection = new Option("c", "collection", true, "Collection");
        options.addOption(collection);

        Option port = new Option("p", "port", true, "MongoDB Port");
        port.setRequired(true);
        options.addOption(port);

        Option create_s = new Option("create_s", "create_s", true, "Creates Start");
        options.addOption(create_s);

        Option create_e = new Option("create_e", "create_e", true, "Creates Start");
        options.addOption(create_e);

        Option read_s = new Option("read_s", "read_s", true, "Read Start");
        options.addOption(read_s);

        Option read_e = new Option("read_e", "read_e", true, "Read End");
        options.addOption(read_e);

        Option update_s = new Option("update_s", "update_s", true, "Update Start");
        options.addOption(update_s);

        Option update_e = new Option("update_e", "update_e", true, "Update End");
        options.addOption(update_e);

        Option delete_s = new Option("delete_s", "delete_s", true, "Delete Start");
        options.addOption(delete_s);

        Option delete_e = new Option("delete_e", "delete_e", true, "Delete End");
        options.addOption(delete_e);

        Option expiry_s = new Option("expiry_s", "expiry_s", true, "Expiry Start");
        options.addOption(expiry_s);

        Option expiry_e = new Option("expiry_e", "expiry_e", true, "Expiry End");
        options.addOption(expiry_e);

        Option create = new Option("cr", "create", true, "Creates%");
        options.addOption(create);

        Option update = new Option("up", "update", true, "Updates%");
        options.addOption(update);

        Option delete = new Option("dl", "delete", true, "Deletes%");
        options.addOption(delete);

        Option expiry = new Option("ex", "expiry", true, "Expiry%");
        options.addOption(expiry);

        Option read = new Option("rd", "read", true, "Reads%");
        options.addOption(read);

        Option workers = new Option("w", "workers", true, "Workers");
        options.addOption(workers);

        Option ops = new Option("ops", "ops", true, "Ops/Sec");
        options.addOption(ops);

        Option keySize = new Option("keySize", "keySize", true, "Size of the key");
        options.addOption(keySize);

        Option docSize = new Option("docSize", "docSize", true, "Size of the doc");
        options.addOption(docSize);

        Option loadType = new Option("loadType", "loadType", true, "Hot/Cold");
        options.addOption(loadType);

        Option keyType = new Option("keyType", "keyType", true, "Random/Sequential/Reverse");
        options.addOption(keyType);

        Option keyPrefix = new Option("keyPrefix", "keyPrefix", true, "String");
        options.addOption(keyPrefix);

        Option valueType = new Option("valueType", "valueType", true, "");
        options.addOption(valueType);

        Option validate = new Option("validate", "validate", true, "Validate Data during Reads");
        options.addOption(validate);

        Option gtm = new Option("gtm", "gtm", true, "Go for max doc ops");
        options.addOption(gtm);
        
        Option altas = new Option("atlas", "atlas", true, "Is this Mongo Atlas");
        options.addOption(altas);

        Option deleted = new Option("deleted", "deleted", true, "To verify deleted docs");
        options.addOption(deleted);

        Option transaction_load = new Option("transaction_patterns", true, "Transaction load pattern");
        options.addOption(transaction_load);

        Option durability = new Option("durability", true, "Durability Level");
        options.addOption(durability);

        Option mutate = new Option("mutate", true, "mutate");
        options.addOption(mutate);

        Option maxTTL = new Option("maxTTL", true, "Expiry Time");
        options.addOption(maxTTL);

        Option maxTTLUnit = new Option("maxTTLUnit", true, "Expiry Time unit");
        options.addOption(maxTTLUnit);

        Option retry = new Option("retry", true, "Retry failures n times");
        options.addOption(retry);

        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            formatter.printHelp("Supported Options", options);
            System.exit(1);
            return;
        }

        Server master = new Server(cmd.getOptionValue("node"),
        		cmd.getOptionValue("port"), cmd.getOptionValue("username"), cmd.getOptionValue("password"), null);
//        Server master = new Server("mongo.cbqeoc.com:27017/", "27017", "Administrator", "password", "27017");
        TaskManager tm = new TaskManager(Integer.parseInt(cmd.getOptionValue("workers", "10")));
        WorkLoadSettings ws = new WorkLoadSettings(
                cmd.getOptionValue("keyPrefix", "test_docs-"),
                Integer.parseInt(cmd.getOptionValue("keySize", "20")),
                Integer.parseInt(cmd.getOptionValue("docSize", "256")),
                Integer.parseInt(cmd.getOptionValue("cr", "100")),
                Integer.parseInt(cmd.getOptionValue("rd", "0")),
                Integer.parseInt(cmd.getOptionValue("up", "0")),
                Integer.parseInt(cmd.getOptionValue("dl", "0")),
                Integer.parseInt(cmd.getOptionValue("ex", "0")),
                Integer.parseInt(cmd.getOptionValue("workers", "10")),
                Integer.parseInt(cmd.getOptionValue("ops", "10000")),
                cmd.getOptionValue("loadType", null),
                cmd.getOptionValue("keyType", "SimpleKey"),
                cmd.getOptionValue("valueType", "Hotel"),
                false, false, false,
                Integer.parseInt(cmd.getOptionValue("mutate", "0"))
                );
        HashMap<String, Number> dr = new HashMap<String, Number>();
        dr.put(DRConstants.create_s, Long.parseLong(cmd.getOptionValue(DRConstants.create_s, "0")));
        dr.put(DRConstants.create_e ,Long.parseLong(cmd.getOptionValue(DRConstants.create_e, "0")));
        dr.put(DRConstants.read_s ,Long.parseLong(cmd.getOptionValue(DRConstants.read_s, "0")));
        dr.put(DRConstants.read_e ,Long.parseLong(cmd.getOptionValue(DRConstants.read_e, "0")));
        dr.put(DRConstants.update_s ,Long.parseLong(cmd.getOptionValue(DRConstants.update_s, "0")));
        dr.put(DRConstants.update_e ,Long.parseLong(cmd.getOptionValue(DRConstants.update_e, "0")));
        dr.put(DRConstants.delete_s ,Long.parseLong(cmd.getOptionValue(DRConstants.delete_s, "0")));
        dr.put(DRConstants.delete_e ,Long.parseLong(cmd.getOptionValue(DRConstants.delete_e, "0")));
        dr.put(DRConstants.expiry_s ,Long.parseLong(cmd.getOptionValue(DRConstants.expiry_s, "0")));
        dr.put(DRConstants.expiry_e ,Long.parseLong(cmd.getOptionValue(DRConstants.expiry_e, "0")));

        DocRange range = new DocRange(dr);
        ws.dr = range;
        DocumentGenerator dg = null;
        try {
            dg = new DocumentGenerator(ws, ws.keyType, ws.valueType);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        ArrayList<MongoSDKClient> clients = new ArrayList<MongoSDKClient>();
        for (int i = 0; i < ws.workers; i++) {
            try {
                MongoSDKClient client = new MongoSDKClient(master,
                		cmd.getOptionValue("bucket"),
                		cmd.getOptionValue("collection"),
                		Boolean.parseBoolean(cmd.getOptionValue("atlas")));
                client.connectCluster();
                clients.add(client);
                String th_name = "Loader" + i;
                tm.submit(new WorkLoadGenerate(th_name, dg, client));
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        tm.getAllTaskResult();
        tm.shutdown();
        for (MongoSDKClient client : clients) {
            client.disconnectCluster();
        }
    }
}
