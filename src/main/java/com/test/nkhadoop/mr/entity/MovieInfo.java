package com.test.nkhadoop.mr.entity;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.util.Times;
import org.checkerframework.checker.units.qual.A;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Time;

public class MovieInfo implements Writable {
    private String UserID="";
    private String Gender="";
    private String Age="";
    private String Occupation="";
    private String ZipCode="";
    private String MovieID="";
    private String Title="";
    private String Genres="";
    private String Rating="";
    private String Timestamp="";

    public MovieInfo(){};

    public MovieInfo(String userID, String gender, String age, String occupation, String zipCode, String movieID, String title, String genres, String rating, String timestamp) {
        UserID = userID;
        Gender = gender;
        Age = age;
        Occupation = occupation;
        ZipCode = zipCode;
        MovieID = movieID;
        Title = title;
        Genres = genres;
        Rating = rating;
        Timestamp = timestamp;
    }

    public String getUserID() {
        return UserID;
    }

    public void setUserID(String userID) {
        UserID = userID;
    }

    public String getGender() {
        return Gender;
    }

    public void setGender(String gender) {
        Gender = gender;
    }

    public String getAge() {
        return Age;
    }

    public void setAge(String age) {
        Age = age;
    }

    public String getOccupation() {
        return Occupation;
    }

    public void setOccupation(String occupation) {
        Occupation = occupation;
    }

    public String getZipCode() {
        return ZipCode;
    }

    public void setZipCode(String zipCode) {
        ZipCode = zipCode;
    }

    public String getMovieID() {
        return MovieID;
    }

    public void setMovieID(String movieID) {
        MovieID = movieID;
    }

    public String getTitle() {
        return Title;
    }

    public void setTitle(String title) {
        Title = title;
    }

    public String getGenres() {
        return Genres;
    }

    public void setGenres(String genres) {
        Genres = genres;
    }

    public String getRating() {
        return Rating;
    }

    public void setRating(String rating) {
        Rating = rating;
    }

    public String getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(String timestamp) {
        Timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "MovieInfo{" +
                "UserID='" + UserID + '\'' +
                ", Gender='" + Gender + '\'' +
                ", Age='" + Age + '\'' +
                ", Occupation='" + Occupation + '\'' +
                ", ZipCode='" + ZipCode + '\'' +
                ", MovieID='" + MovieID + '\'' +
                ", Title='" + Title + '\'' +
                ", Genres='" + Genres + '\'' +
                ", Rating='" + Rating + '\'' +
                ", Timestamp='" + Timestamp + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(UserID);
        dataOutput.writeUTF(Gender);
        dataOutput.writeUTF(Age);
        dataOutput.writeUTF(Occupation);
        dataOutput.writeUTF(ZipCode);
        dataOutput.writeUTF(MovieID);
        dataOutput.writeUTF(Title);
        dataOutput.writeUTF(Genres);
        dataOutput.writeUTF(Rating);
        dataOutput.writeUTF(Timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        UserID=dataInput.readUTF();
        Gender=dataInput.readUTF();
        Age =dataInput.readUTF();
        Occupation=dataInput.readUTF();
        ZipCode=dataInput.readUTF();
        MovieID=dataInput.readUTF();
        Title=dataInput.readUTF();
        Genres=dataInput.readUTF();
        Rating=dataInput.readUTF();
        Timestamp=dataInput.readUTF();

    }
}
