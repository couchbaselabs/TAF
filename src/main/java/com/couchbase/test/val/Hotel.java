package com.couchbase.test.val;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Date;
import java.util.ArrayList;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

import com.couchbase.test.docgen.WorkLoadSettings;

public class Hotel {
    Faker faker;
    private Random random;
    private ArrayList<String> addresses = new ArrayList<String>();
    private ArrayList<String> city = new ArrayList<String>();
    private ArrayList<String> country = new ArrayList<String>();
    private List<String> htypes = Arrays.asList("Inn", "Hostel", "Place", "Center", "Hotel", "Motel", "Suites");
    private ArrayList<String> emails = new ArrayList<String>();
    private ArrayList<ArrayList<String>> likes = new ArrayList<ArrayList<String>>();
    private ArrayList<String> names = new ArrayList<String>();
    private ArrayList<String> url = new ArrayList<String>();
    private ArrayList<ArrayList<JsonObject>> reviews = new ArrayList<ArrayList<JsonObject>>();

    public Hotel(WorkLoadSettings ws) {
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
            emails.add(fn + '.' + ln + "@hotels.com");
            country.add(faker.address().country());

            ArrayList<String> temp = new ArrayList<String>();
            int numLikes = this.random.nextInt(10);
            for (int n = 0; n <= numLikes; n++) {
                temp.add(faker.name().fullName());
            }
            this.likes.add(temp);
            url.add(faker.internet().url());
            this.setReviewsArray();
        }
    }

    public void setReviewsArray() {
        int numReviews = this.random.nextInt(10);
        LocalDateTime now = LocalDateTime.now();
        ArrayList<JsonObject> temp = new ArrayList<JsonObject>();
        for (int n = 0; n <= numReviews; n++) {
            JsonObject review = JsonObject.create();
            review.put("author", faker.name().fullName());
            review.put("date", now.plus(n, ChronoUnit.WEEKS).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            JsonObject ratings = JsonObject.create();
            ratings.put("Check in / front desk", this.random.nextInt(5));
            ratings.put("Cleanliness", this.random.nextInt(5));
            ratings.put("Overall", this.random.nextInt(5));
            ratings.put("Rooms", this.random.nextInt(5));
            ratings.put("Value", this.random.nextInt(5));
            review.put("ratings", ratings);
            temp.add(review);
        }
        this.reviews.add(temp);
    }

    public JsonObject next(String key) {
        this.random = new Random();
        JsonObject jsonObject = JsonObject.create();
        this.random.setSeed(key.hashCode());
        int index = this.random.nextInt(4096);
        jsonObject.put("address", this.addresses.get(index));
        jsonObject.put("city", this.city.get(index));
        jsonObject.put("country", this.country.get(index));
        jsonObject.put("email", this.emails.get(index));
        jsonObject.put("free_breakfast", this.random.nextBoolean());
        jsonObject.put("free_parking", this.random.nextBoolean());
        jsonObject.put("phone", faker.phoneNumber().phoneNumber());
        jsonObject.put("name", this.names.get(index));
        jsonObject.put("price", 500 + this.random.nextInt(1500));
        jsonObject.put("avg_rating", this.random.nextFloat()*5);
        jsonObject.put("public_likes", this.likes.get(index));
        jsonObject.put("reviews", this.reviews.get(index));
        jsonObject.put("type", this.htypes.get(index % htypes.size()));
        jsonObject.put("url", this.url.get(index));
        return jsonObject;
    }

}
