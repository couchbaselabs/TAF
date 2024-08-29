package com.couchbase.test.val;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.javafaker.Faker;

import com.couchbase.test.docgen.WorkLoadSettings;

public class siftBigANN {
    Faker faker;
    private Random random;
    public WorkLoadSettings ws;
//    t1M = {"vector": None, "size": [5, 6, 7, 8, 9, 10], "color": "Green", "brand": "Nike", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 1}
//    t2M = {"vector": None, "size": [6, 7, 8, 9, 10], "color": "Green", "brand": "Nike", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 2}
//    t5M = {"vector": None, "size": [7, 8, 9, 10], "color": "Red", "brand": "Nike", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 2.5}
//    t10M = {"vector": None, "size": [8, 9, 10], "color": "Red", "brand": "Adidas", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 3}
//    t20M = {"vector": None, "size": [9, 10], "color": "Red", "brand": "Adidas", "country": "Canada", "category": "Shoes", "type": "Apparel", "avg_review": 3.5}
//    t50M = {"vector": None, "size": [10], "color": "Red", "brand": "Adidas", "country": "Canada", "category": "Jeans", "type": "Apparel", "avg_review": 4}
//    t100M = {"vector": None, "color": "Red", "brand": "Adidas", "country": "Canada", "category": "Jeans", "type": "Denim", "avg_review": 4.5}
    FileInputStream inputStream = null;
    File fh = null;
    private FileInputStream mutateInputStream = null;
    private long mutateCount;
    private int remainingCount;
    
    public siftBigANN(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.random = new Random();
        this.random.setSeed(ws.keyPrefix.hashCode());
        
        try {
            this.inputStream = new FileInputStream(this.ws.baseVectorsFilePath);
            if(this.ws.creates > 0) {
                this.inputStream.skip(ws.dr.create_s * 132);
                this.mutateCount = this.ws.dr.create_e - this.ws.dr.create_s;
            }
            else if(this.ws.updates > 0) {
                this.inputStream.skip(ws.dr.update_s * 132 + this.ws.mutated * 132);
                this.mutateCount = this.ws.dr.update_e - this.ws.dr.update_s - this.ws.mutated;
                if(this.ws.mutated > 0)
                    this.mutateInputStream  = new FileInputStream(this.ws.baseVectorsFilePath);
            }
            this.remainingCount = this.ws.mutated;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] floatsToBytes(float[] floats) {
        byte bytes[] = new byte[Float.BYTES * floats.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);
        return bytes;
    }

    public static String convertToBase64Bytes(float[] floats) {
        return Base64.getEncoder().encodeToString(floatsToBytes(floats));
      }

    public Object next(String key) throws IOException {
        int id = Integer.parseInt(key.split("-")[1]) - 1 + this.ws.mutated;
        float[] vector =  new float[128];
        if(this.mutateCount > 0) {
            byte[] byteArray = new byte[(int) 4];
            this.inputStream.read(byteArray);
            int dim = ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if(dim > 128)
                System.out.println(dim);
            byte[] byteVector = new byte[(int) dim];
            this.inputStream.read(byteVector);
            int i = 0;
            for (byte b : byteVector) {
                vector[i++] = (float)Byte.toUnsignedInt(b);
            }
            this.mutateCount -= 1;
        } else if(this.remainingCount > 0) {
            byte[] byteArray = new byte[(int) 4];
            this.mutateInputStream.read(byteArray);
            int dim = ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if(dim > 128)
                System.out.println(dim);
            byte[] byteVector = new byte[(int) dim];
            this.mutateInputStream.read(byteVector);
            int i = 0;
            for (byte b : byteVector) {
                vector[i++] = (float)Byte.toUnsignedInt(b);
            }
        }

        if(ws.dr.create_s >= 0 && ws.dr.create_e <= 1000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 5, "Green",
                        "Nike", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 5, "Green",
                        "Nike", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
        if(ws.dr.create_s >= 1000000 && ws.dr.create_e <= 2000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 6, "Green",
                        "Nike", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 6, "Green",
                        "Nike", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
        if(ws.dr.create_s >= 2000000 && ws.dr.create_e <= 5000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 7, "Red",
                        "Nike", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 7, "Red",
                        "Nike", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
        if(ws.dr.create_s >= 5000000 && ws.dr.create_e <= 10000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 8, "Blue",
                        "Adidas", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 8, "Blue",
                        "Adidas", "USA", "Shoes", "Casual", 1.0f, this.ws.mutated);
        if(ws.dr.create_s >= 10000000 && ws.dr.create_e <= 20000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 9, "Purple",
                        "Puma", "Canada", "Shoes", "Casual", 1.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 9, "Purple",
                        "Puma", "Canada", "Shoes", "Casual", 1.0f, this.ws.mutated);
        if(ws.dr.create_s >= 20000000 && ws.dr.create_e <= 50000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 10, "Pink",
                        "Asics", "Australia", "Jeans", "Casual", 1.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 10, "Pink",
                        "Asics", "Australia", "Jeans", "Casual", 1.0f, this.ws.mutated);
        if(ws.dr.create_s >= 50000000 && ws.dr.create_e <= 100000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 11, "Yellow",
                        "Brook", "England", "Shirt", "Formal", 1.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 11, "Yellow",
                        "Brook", "England", "Shirt", "Formal", 1.0f, this.ws.mutated);
        if(ws.dr.create_s >= 100000000 && ws.dr.create_e <= 200000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 12, "Brown",
                        "Hoka", "India", "Shorts", "Sports", 2.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 12, "Brown",
                        "Hoka", "India", "Shorts", "Sports", 2.0f, this.ws.mutated);
        if(ws.dr.create_s >= 200000000 && ws.dr.create_e <= 500000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 13, "Magenta",
                        "New Balance", "Mexico", "Bottoms", "Sneakers", 5.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 13, "Magenta",
                        "New Balance", "Mexico", "Bottoms", "Sneakers", 5.0f, this.ws.mutated);
        if(ws.dr.create_s >= 500000000 && ws.dr.create_e <= 1000000000)
            if(this.ws.base64)
                return new Product2(id, convertToBase64Bytes(vector), 14, "Indigo",
                        "Vans", "France", "Top", "Sandals", 10.0f, this.ws.mutated);
            else
                return new Product1(id, vector, 14, "Indigo",
                        "Vans", "France", "Top", "Sandals", 10.0f, this.ws.mutated);
        return null;
    }
    

    public class Product1 {
        @JsonProperty
        private int id;
        @JsonProperty
        private float[] embedding;
        @JsonProperty
        private int size;
        @JsonProperty
        private String color;
        @JsonProperty
        private String brand;
        @JsonProperty
        private String country;
        @JsonProperty
        private String category;
        @JsonProperty
        private String type;
        @JsonProperty
        private float review;
        @JsonProperty
        private int mutate;

        @JsonCreator
        public
        Product1(
                @JsonProperty("idx")int id,
                @JsonProperty("embedding") float[] vector,
                @JsonProperty("size") int i,
                @JsonProperty("color") String color,
                @JsonProperty("brand") String brand,
                @JsonProperty("country") String country,
                @JsonProperty("category") String category,
                @JsonProperty("type") String type,
                @JsonProperty("review") float review,
                @JsonProperty("mutate") int mutate){
            this.id = id;
            this.embedding = vector;
            this.size = i;
            this.color = color;
            this.brand = brand;
            this.country = country;
            this.category = category;
            this.type = type;
            this.review = review;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getMutate() {
            return mutate;
        }

        public void setMutate(int mutate) {
            this.mutate = mutate;
        }

        public void setReview(float review) {
            this.review = review;
        }

        public float[] getEmbedding() {
            return embedding;
        }

        public void setEmbedding(float[] embedding) {
            this.embedding = embedding;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public String getColor() {
            return color;
        }

        public void setColor(String color) {
            this.color = color;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public float getReview() {
            return review;
        }

        public void setReview(Float review) {
            this.review = review;
        }
    }

    public class Product2 {
        @JsonProperty
        private int id;
        @JsonProperty
        private String embedding;
        @JsonProperty
        private int size;
        @JsonProperty
        private String color;
        @JsonProperty
        private String brand;
        @JsonProperty
        private String country;
        @JsonProperty
        private String category;
        @JsonProperty
        private String type;
        @JsonProperty
        private float review;
        @JsonProperty
        private int mutate;

        @JsonCreator
        public
        Product2(
                @JsonProperty("idx")int id,
                @JsonProperty("embedding") String vector,
                @JsonProperty("size") int i,
                @JsonProperty("color") String color,
                @JsonProperty("brand") String brand,
                @JsonProperty("country") String country,
                @JsonProperty("category") String category,
                @JsonProperty("type") String type,
                @JsonProperty("review") float review,
                @JsonProperty("mutate") int mutate){
            this.id = id;
            this.embedding = vector;
            this.size = i;
            this.color = color;
            this.brand = brand;
            this.country = country;
            this.category = category;
            this.type = type;
            this.review = review;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getMutate() {
            return mutate;
        }

        public void setMutate(int mutate) {
            this.mutate = mutate;
        }

        public void setReview(float review) {
            this.review = review;
        }

        public String getEmbedding() {
            return embedding;
        }

        public void setEmbedding(String embedding) {
            this.embedding = embedding;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public String getColor() {
            return color;
        }

        public void setColor(String color) {
            this.color = color;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public float getReview() {
            return review;
        }

        public void setReview(Float review) {
            this.review = review;
        }
    }
}
