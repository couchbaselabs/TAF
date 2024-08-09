package com.couchbase.test.val;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.test.docgen.WorkLoadSettings;
import com.github.javafaker.Faker;

import ai.djl.MalformedModelException;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;

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
    private int mutate;
    private List<String> mutate_field_list = new ArrayList<>();

    public WorkLoadSettings ws;
    public static float[] flt_buf;
    public static int flt_buf_length;

    public Hotel() {
        super();
    }

    public Hotel(WorkLoadSettings ws) {
        super();
        this.ws = ws;
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
        Hotel.flt_buf = new float[1024*1024];

        for (int index=0; index<1024*1024; index++) {
            float x = this.random.nextFloat();
            Hotel.flt_buf[index] = x;
        }
        Hotel.flt_buf_length = Hotel.flt_buf.length;
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
    public static byte[] floatsToBytes(float[] floats) {
        byte bytes[] = new byte[Float.BYTES * floats.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);
        return bytes;
    }

    public static String convertToBase64Bytes(float[] floats) {
        return Base64.getEncoder().encodeToString(floatsToBytes(floats));
      }

    private float[] get_float_array(int length, Random random_obj) {
        int _slice = random_obj.nextInt(Hotel.flt_buf_length - length);
//        return flt_buf_al.subList(_slice, _slice + length);
        return Arrays.copyOfRange(Hotel.flt_buf, _slice, _slice+length);
    }

    public JsonObject next(String key) {
        this.random = new Random();
        float[] vector = null;
        JsonObject jsonObject = JsonObject.create();
        this.random.setSeed((key).hashCode());
        if(this.ws.mutated > 0){
            this.random.setSeed((key+Integer.toString(this.ws.mutated)).hashCode());
        }
        int index = this.random.nextInt(4096);
        jsonObject.put("free_breakfast", this.random.nextBoolean());
        jsonObject.put("free_parking", this.random.nextBoolean());
        jsonObject.put("phone", faker.phoneNumber().phoneNumber());
        jsonObject.put("name", this.names.get(index));
        jsonObject.put("price", 500 + this.random.nextInt(1500));
        jsonObject.put("avg_rating", this.random.nextFloat()*5);
        jsonObject.put("address", this.addresses.get(index));
        jsonObject.put("city", this.city.get(index));
        jsonObject.put("country", this.country.get(index));
        jsonObject.put("email", this.emails.get(index));
        jsonObject.put("name", this.names.get(index));
        jsonObject.put("public_likes", this.likes.get(index));
        jsonObject.put("reviews", this.reviews.get(index));
        jsonObject.put("type", this.htypes.get(index % htypes.size()));
        jsonObject.put("url", this.url.get(index));
        jsonObject.put("mutate", this.mutate);
        if(this.ws.mutated > 0 && !this.ws.mutate_field.isEmpty()){
            this.random.setSeed((key).hashCode());
            index = this.random.nextInt(4096);
            mutate_field_list = Arrays.asList(this.ws.mutate_field.split(","));
            if(!mutate_field_list.contains("address"))
                jsonObject.put("address", this.addresses.get(index));
            if(!mutate_field_list.contains("city"))
                jsonObject.put("city", this.city.get(index));
            if(!mutate_field_list.contains("country"))
                jsonObject.put("country", this.country.get(index));
            if(!mutate_field_list.contains("email"))
                jsonObject.put("email", this.emails.get(index));
            if(!mutate_field_list.contains("name"))
                jsonObject.put("name", this.names.get(index));
            if(!mutate_field_list.contains("public_likes"))
                jsonObject.put("public_likes", this.likes.get(index));
            if(!mutate_field_list.contains("reviews"))
                jsonObject.put("reviews", this.reviews.get(index));
            if(!mutate_field_list.contains("type"))
                jsonObject.put("type", this.htypes.get(index % htypes.size()));
            if(!mutate_field_list.contains("url"))
                jsonObject.put("url", this.url.get(index));
            if(!mutate_field_list.contains("free_breakfast"))
                jsonObject.put("free_breakfast", this.random.nextBoolean());
            if(!mutate_field_list.contains("free_parking"))
                jsonObject.put("free_parking", this.random.nextBoolean());
            if(!mutate_field_list.contains("phone"))
                jsonObject.put("phone", faker.phoneNumber().phoneNumber());
            if(!mutate_field_list.contains("price"))
                jsonObject.put("price", 500 + this.random.nextInt(1500));
            if(!mutate_field_list.contains("avg_rating"))
                jsonObject.put("avg_rating", this.random.nextFloat()*5);
        }
        jsonObject.put("mutate", this.ws.mutated);
        if(this.ws.mockVector) {
            vector = this.get_float_array(this.ws.dim, this.random);
            if(this.ws.base64)
                jsonObject.put("vector", convertToBase64Bytes(vector));
            else {
                JsonArray floatVector = JsonArray.create();
                for (int i = 0; i < vector.length; i++) {
                    floatVector.add(Float.valueOf(vector[i]));
                }
                jsonObject.put("vector", floatVector);
            }
        }
        return jsonObject;
    }
}
