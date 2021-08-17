package com.couchbase.test.val;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import com.couchbase.test.docgen.DocType.Attributes;
import com.couchbase.test.docgen.DocType.Details;
import com.couchbase.test.docgen.DocType.Dimensions;
import com.couchbase.test.docgen.DocType.Hobby;
import com.couchbase.test.docgen.DocType.Location;
import com.couchbase.test.docgen.DocType.Person;
import com.couchbase.test.docgen.WorkLoadSettings;
import com.couchbase.test.dictionary.Dictionary;

public class SimpleValue {
    public WorkLoadSettings ws;
    private static int fixedSize = 454;
    static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static final String lower = upper.toLowerCase(Locale.ROOT);
    static final String digits = "0123456789";
    static final char[] key_chars = (upper + lower + digits).toCharArray();
    String randomString = null;
    int randomStringLength = 0;

    public SimpleValue(WorkLoadSettings ws) {
        super();
        this.ws = ws;
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
    }

    private String get_random_string(String key, int length, Random random_obj) {
        if(length>0) {
            int _slice = random_obj.nextInt(this.randomStringLength - length);
            return this.randomString.substring(_slice, length+_slice);
        }
        return "";
    }

    public Person next(String key) {
        Random random_obj = new Random();
        random_obj.setSeed(key.hashCode());
        Person person = new Person(this.get_name(random_obj), this.get_int(random_obj), this.get_animals(random_obj),
                new Attributes(this.get_colour(random_obj), new Dimensions(this.get_int(random_obj), this.get_int(random_obj)),
                        Collections.singletonList(
                                new Hobby(this.get_hobby(random_obj), this.get_hobby(random_obj),
                                        new Details(new Location(this.get_double(random_obj), this.get_double(random_obj)))))),
                this.get_gender(random_obj), this.get_marital_status(random_obj), this.ws.mutated,
                this.get_random_string(key, this.ws.docSize - fixedSize, random_obj));
        return person;
    }

    private String get_marital_status(Random random_obj) {
        int num = random_obj.nextInt(Dictionary.MARITAL_STATUSES_LENGTH);
        return Dictionary.MARITAL_STATUSES.get(num);
    }

    private String get_gender(Random random_obj) {
        int num = random_obj.nextInt(Dictionary.GENDER_LENGTH);
        return Dictionary.GENDER.get(num);
    }

    private String get_name(Random random_obj) {
        int num = random_obj.nextInt(this.randomStringLength);
        return this.randomString.charAt(num) + "John";
    }

    private String get_hobby(Random random_obj) {
        int num = random_obj.nextInt(Dictionary.HOBBY_LENGTH);
        return Dictionary.HOBBIES.get(num);
    }

    private String get_colour(Random random_obj) {
        int num = random_obj.nextInt(Dictionary.COLOR_LENGTH);
        return Dictionary.COLOR.get(num);
    }

    private List<String> get_animals(Random random_obj) {
        int num = random_obj.nextInt(Dictionary.COLOR_LENGTH-2);
        return Dictionary.COLOR.subList(num, num+2);
    }

    private int get_int(Random random_obj) {
        return random_obj.nextInt(100);
    }

    private double get_double(Random random_obj) {
        return random_obj.nextDouble()*100;
    }

}