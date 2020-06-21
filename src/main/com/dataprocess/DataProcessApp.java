package com.dataprocess;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.SQLException;
import java.text.ParseException;


@SpringBootApplication
public class DataProcessApp {

    public static void main(String[] args) throws ParseException, SQLException {
        SpringApplication.run(DataProcessApp.class, args);
    }

}
