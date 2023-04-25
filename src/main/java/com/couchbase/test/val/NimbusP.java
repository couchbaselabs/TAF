package com.couchbase.test.val;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

import com.couchbase.test.docgen.WorkLoadSettings;

public class NimbusP {
	Faker faker;
	private Random random;
	private ArrayList<String> addresses = new ArrayList<String>();
	private ArrayList<String> city = new ArrayList<String>();
	private ArrayList<String> country = new ArrayList<String>();
	private ArrayList<String> emails = new ArrayList<String>();
	private ArrayList<String> names = new ArrayList<String>();
	String padding = "000000000000000000000000000000000000";

	public NimbusP(WorkLoadSettings ws) {
		super();
		this.random = new Random();
		this.random.setSeed(ws.keyPrefix.hashCode());
        faker = new Faker(random);
		for (int index=0; index<4096; index++) {
            addresses.add(faker.address().streetAddress());
            city.add(faker.address().city());
            country.add(faker.address().country());
            String fn = faker.name().firstName();
            String ln = faker.name().lastName();
            names.add(faker.name().fullName());
            emails.add(fn + '.' + ln + "@querty.com");
            country.add(faker.address().country());
        }
	}

	public JsonObject next(String key) {
		this.random = new Random();
		JsonObject jsonObject = JsonObject.create();
		LocalDateTime now = LocalDateTime.now();
		this.random.setSeed(key.hashCode());
		int index = this.random.nextInt(4096);
		jsonObject.put("address", this.addresses.get(index));
		jsonObject.put("city", this.city.get(index));
		jsonObject.put("country", this.country.get(index));
		jsonObject.put("email", this.emails.get(index));
		String temp = Integer.toString(this.random.nextInt(1000000));
		int padd = 36 - temp.length();
		jsonObject.put("conversationId", this.padding.substring(0, padd) + temp);

		jsonObject.put("userName", faker.name().fullName());
		temp = Integer.toString(this.random.nextInt(1000000));
		padd = 36 - temp.length();
		jsonObject.put("uid", this.padding.substring(0, padd) + temp);

		jsonObject.put("unreadCount", this.random.nextInt(1000));
		jsonObject.put("lastMessageDate", now.plus(this.random.nextInt(1000), ChronoUnit.WEEKS).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
		jsonObject.put("lastReadDate", now.plus(this.random.nextInt(1000), ChronoUnit.WEEKS).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
		return jsonObject;
	}

}
