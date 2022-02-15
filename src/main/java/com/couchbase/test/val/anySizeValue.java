package com.couchbase.test.val;

import com.couchbase.test.docgen.anySize.Person1;
import com.couchbase.test.docgen.WorkLoadSettings;

import java.util.Locale;
import java.util.Random;

import com.couchbase.test.dictionary.Dictionary;

public class anySizeValue {
    public WorkLoadSettings ws;
    private static int fixedSize = 9;
    static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static final String lower = upper.toLowerCase(Locale.ROOT);
    static final String digits = "0123456789";
    static final char[] key_chars = (upper + lower + digits).toCharArray();
    String randomString = null;
    int randomStringLength = 0;

    public anySizeValue(WorkLoadSettings ws) {
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

    public Person1 next(String key) {
        Random random_obj = new Random();
        random_obj.setSeed(key.hashCode());
        Person1 person = new Person1(this.ws.mutated,
        		this.get_random_string(key, this.ws.docSize - fixedSize, random_obj));
        return person;
    }
}
