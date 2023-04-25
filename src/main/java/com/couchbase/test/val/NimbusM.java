package com.couchbase.test.val;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Random;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

import com.couchbase.test.docgen.WorkLoadSettings;

public class NimbusM {
	Faker faker;
	private Random random;
	String padding = "000000000000000000000000000000000000";
	static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static final String lower = upper.toLowerCase(Locale.ROOT);
    static final String digits = "0123456789";
    static final char[] key_chars = (upper + lower + digits).toCharArray();
    String randomString = null;
    int randomStringLength = 0;

	public NimbusM(WorkLoadSettings ws) {
		super();
		this.random = new Random();
		this.random.setSeed(ws.keyPrefix.hashCode());
		char[] str_buf = new char[4096];
        Random random_obj = new Random();
        random_obj.setSeed(ws.keyPrefix.hashCode());

        for (int index=0; index<4096; index++) {
			str_buf[index] = key_chars[random_obj.nextInt(key_chars.length)];
        }

        this.randomString = String.valueOf(str_buf);
        String temp = this.randomString;
        this.randomStringLength = randomString.length();
        for (int i = 0; i < ws.docSize/this.randomStringLength+2; i++) {
            this.randomString = this.randomString.concat(temp);
        }
        this.randomStringLength = this.randomString.length();
        
        this.faker = new Faker(random);
	}

    private String get_random_string(int length, Random random_obj) {
        if(length>0) {
            int _slice = random_obj.nextInt(this.randomStringLength - length);
            return this.randomString.substring(_slice, length+_slice);
        }
        return "";
    }

	public JsonObject next(String key) {
		this.random = new Random();
        this.random.setSeed(key.hashCode());

        JsonObject jsonObject = JsonObject.create();
		int temp_int = this.random.nextInt(9) + 1;
		jsonObject.put("type", this.get_random_string(10, this.random));
		jsonObject.put("width", this.random.nextInt(100));
		jsonObject.put("height", this.random.nextInt(100));
		jsonObject.put("clickable", this.random.nextBoolean());
		jsonObject.put("actions", this.get_random_string(10, this.random));
		
		String temp = Integer.toString(this.random.nextInt(1000000));
		int padd = 36 - temp.length();
		jsonObject.put("conversationId", this.padding.substring(0, padd) + temp);

		jsonObject.put("timestamp", new Timestamp(System.currentTimeMillis()).toString());
		temp = Integer.toString(this.random.nextInt(1000000));
		padd = 36 - temp.length();
		jsonObject.put("uid", this.padding.substring(0, padd) + temp);

		jsonObject.put("roomId", this.get_random_string(10, this.random));
		jsonObject.put("roomTitle", this.get_random_string(10, this.random));
		jsonObject.put("roomStreamers", this.get_random_string(10, this.random));
		jsonObject.put("pixel", this.random.nextInt(4096));

		ArrayList<String> users = new ArrayList<String>();
		for(int i=0; i < temp_int; i++) {
			temp = Integer.toString(this.random.nextInt(1000000));
			users.add(this.padding.substring(0, padd) + temp);	
		}
		jsonObject.put("showTo", users);
		jsonObject.put("content", this.faker.lorem().sentences(1).get(0));
		return jsonObject;
	}

}
