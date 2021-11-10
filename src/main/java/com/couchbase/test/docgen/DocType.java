package com.couchbase.test.docgen;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DocType {

    public static class Dimensions {
        private  int height;
        private  int weight;

        @JsonCreator
        public
        Dimensions(@JsonProperty("height") int height, @JsonProperty("weight") int weight) {
            this.height = height;
            this.weight = weight;
        }

        @JsonGetter
        int height() {
            return height;
        }

        @JsonGetter
        int weight() {
            return weight;
        }
    }

    public static class Location {
        private  double lat;
        private  double lon;

        @JsonCreator
        public
        Location(@JsonProperty("lat") double lat, @JsonProperty("lon") double lon) {
            this.lat = lat;
            this.lon = lon;
        }

        @JsonGetter
        double lat() {
            return lat;
        }

        @JsonGetter
        double lon() {
            return lon;
        }
    }

    public static class Details {

        private  Location location;

        @JsonCreator
        public
        Details(@JsonProperty("location") Location location) {
            this.location = location;
        }

        @JsonGetter
        Location location() {
            return location;
        }
    }

    public static class Body {

        private  String body;

        @JsonCreator
        public
        Body(@JsonProperty("body") String body) {
            this.body = body;
        }

        @JsonGetter
        String body() {
            return body;
        }
    }

    public static class Hobby {
        private  String type;
        private  String name;
        private  Details details;

        @JsonCreator
        public
        Hobby(
                @JsonProperty("type") String type,
                @JsonProperty("name") String name,
                @JsonProperty("details") Details details) {
            this.type = type;
            this.name = name;
            this.details = details;
        }

        @JsonGetter
        String type() {
            return type;
        }

        @JsonGetter
        String name() {
            return name;
        }

        @JsonGetter
        Details details() {
            return details;
        }
    }

    public static class Attributes {
        private  String hair;
        private  Dimensions dimensions;
        private  List<Hobby> hobbies;

        @JsonCreator
        public
        Attributes(
                @JsonProperty("hair") String hair,
                @JsonProperty("dimensions") Dimensions dimensions,
                @JsonProperty("hobbies") List<Hobby> hobbies) {
            this.hair = hair;
            this.dimensions = dimensions;
            this.hobbies = hobbies;
        }

        @JsonGetter
        String hair() {
            return hair;
        }

        @JsonGetter
        Dimensions dimensions() {
            return dimensions;
        }

        @JsonGetter
        List<Hobby> hobbies() {
            return hobbies;
        }
    }

    public static class Person {
        private String name;
        private int age;
        private List<String> animals;
        private Attributes attributes;
        private String body;
        private String gender;
        private String marital;
        private int mutated;

        @JsonCreator
        public
        Person(
                @JsonProperty("name") String name,
                @JsonProperty("age") int age,
                @JsonProperty("animals") List<String> animals,
                @JsonProperty("attributes") Attributes attributes,
                @JsonProperty("gender") String gender,
                @JsonProperty("marital") String marital,
                @JsonProperty("mutated") int mutated,
                @JsonProperty("body") String body) {
            this.name = name;
            this.age = age;
            this.animals = animals;
            this.attributes = attributes;
            this.gender = gender;
            this.marital = marital;
            this.mutated = mutated;
            this.body = body;
        }

        @JsonGetter
        String name() {
            return name;
        }

        public void setName(String new_name) {
            name = new_name;
        }

        @JsonGetter
        int age() {
            return age;
        }

        @JsonGetter
        List<String> animals() {
            return animals;
        }

        @JsonGetter
        Attributes attributes() {
            return attributes;
        }

        @JsonGetter
        String body() {
            return body;
        }

        @JsonGetter
        String gender() {
            return gender;
        }

        @JsonGetter
        String marital() {
            return marital;
        }

        @JsonGetter
        int mutated() {
            return mutated;
        }
    }
}
