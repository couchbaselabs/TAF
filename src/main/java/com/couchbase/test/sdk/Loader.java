package com.couchbase.test.sdk;

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

import com.couchbase.test.docgen.DocumentGenerator;
import com.couchbase.test.docgen.WorkLoadSettings;
import com.couchbase.test.key.SimpleKey;
import com.couchbase.test.loadgen.WorkLoadGenerate;
import com.couchbase.test.taskmanager.TaskManager;

public class Loader {
    static Logger logger = LogManager.getLogger(Loader.class);

    public static void main(String[] args) {
        logger.info("#################### Starting Java Based Doc-Loader ####################");

        Options options = new Options();

        Option name = new Option("n", "node", true, "IP Address");
        name.setRequired(true);
        options.addOption(name);

        Option rest_username = new Option("user", "rest_username", true, "Username");
        rest_username.setRequired(true);
        options.addOption(rest_username);

        Option rest_password = new Option("pwd", "rest_password", true, "Password");
        rest_password.setRequired(true);
        options.addOption(rest_password);

        Option bucket = new Option("b", "bucket", true, "Bucket");
        bucket.setRequired(true);
        options.addOption(bucket);

        Option port = new Option("p", "port", true, "Memcached Port");
        port.setRequired(true);
        options.addOption(port);

        Option create = new Option("cr", "create", true, "Creates%");
        options.addOption(create);

        Option update = new Option("up", "update", true, "Updates%");
        options.addOption(update);

        Option delete = new Option("dl", "delete", true, "Deletes%");
        options.addOption(delete);

        Option read = new Option("rd", "read", true, "Reads%");
        options.addOption(read);

        Option workers = new Option("w", "workers", true, "Workers");
        options.addOption(workers);

        Option ops = new Option("ops", "ops", true, "Ops/Sec");
        options.addOption(ops);

        Option items = new Option("items", "items", true, "Current Items");
        options.addOption(items);

        Option start = new Option("start", "start", true, "Start Index");
        options.addOption(start);

        Option end = new Option("end", "end", true, "End Index");
        options.addOption(end);

        Option keySize = new Option("keySize", "keySize", true, "Size of the key");
        options.addOption(keySize);

        Option loadType = new Option("loadType", "loadType", true, "Hot/Cold");
        options.addOption(loadType);

        Option keyType = new Option("keyType", "keyType", true, "Random/Sequential/Reverse");
        options.addOption(keyType);

        Option valueType = new Option("valueType", "valueType", true, "");
        options.addOption(valueType);

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

        Server master = new Server(cmd.getOptionValue("node"), cmd.getOptionValue("port"), cmd.getOptionValue("rest_username"), cmd.getOptionValue("rest_password"), cmd.getOptionValue("port"));
        TaskManager tm = new TaskManager(10);

        WorkLoadSettings ws = new WorkLoadSettings(
                cmd.getOptionValue("keyPrefix", "test_docs-"),
                Integer.parseInt(cmd.getOptionValue("itr", "0")),
                Integer.parseInt(cmd.getOptionValue("start", "0")),
                Integer.parseInt(cmd.getOptionValue("end", "0")),
                Integer.parseInt(cmd.getOptionValue("keySize", "20")),
                Integer.parseInt(cmd.getOptionValue("docSize", "5000")),
                Integer.parseInt(cmd.getOptionValue("cr", "0")),
                Integer.parseInt(cmd.getOptionValue("rd", "0")),
                Integer.parseInt(cmd.getOptionValue("up", "0")),
                Integer.parseInt(cmd.getOptionValue("dl", "0")),
                Integer.parseInt(cmd.getOptionValue("workers", "10")),
                Integer.parseInt(cmd.getOptionValue("ops", "10000")),
                Integer.parseInt(cmd.getOptionValue("items", "0")),
                cmd.getOptionValue("loadType", null),
                cmd.getOptionValue("keyType", null),
                cmd.getOptionValue("valueType", null)
                );
        DocumentGenerator dg = null;
        try {
            dg = new DocumentGenerator(ws, "RandomKey", null);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        for (int i = 0; i < ws.workers; i++) {
            try {
                SDKClient client = new SDKClient(master, cmd.getOptionValue("bucket"), "_default", "_default");
                client.initialiseSDK();
                String th_name = "Loader" + i;
                tm.submit(new WorkLoadGenerate(th_name, dg, client));
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        tm.getAllTaskResult();
    }
}