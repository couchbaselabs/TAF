package com.couchbase.test.key;

import java.util.Locale;
import java.util.Random;

import com.couchbase.test.docgen.WorkLoadSettings;

public class RandomKey extends SimpleKey{
    // Value in bytes
    static final short max_key_len = 250;
    static final int max_doc_size = 20971520;

    static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static final String lower = upper.toLowerCase(Locale.ROOT);
    static final String digits = "0123456789";
    static final char[] key_chars = (upper + lower + digits).toCharArray();
    String randomString = null;
    int randomStringLength = 0;

    public RandomKey() {
        super();
    }

    public RandomKey(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        Random random_obj = new Random();
        random_obj.setSeed(ws.keyPrefix.hashCode());
        char[] str_buf = new char[4096];
        for (int index=0; index<4096; index++) {
            str_buf[index] = RandomKey.key_chars[random_obj.nextInt(RandomKey.key_chars.length)];
        }
        this.randomString = String.valueOf(str_buf);
        this.randomStringLength = randomString.length();
    }

    public String next(long doc_index) {
        Random random_obj = new Random();
        random_obj.setSeed(doc_index);
        String counter = Long.toString(doc_index);
        return this.get_random_string(ws.keySize - counter.length()-1, random_obj) + "-" +counter;
    }

    public String get_random_string(int length, Random random_obj) {
        int _slice = random_obj.nextInt(randomStringLength - ws.keySize);
        return randomString.substring(_slice, _slice + length);
    }
}
