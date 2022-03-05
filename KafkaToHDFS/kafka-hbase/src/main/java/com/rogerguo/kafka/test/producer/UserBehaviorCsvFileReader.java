package com.rogerguo.kafka.test.producer;

import com.csvreader.CsvReader;
import com.rogerguo.kafka.test.beans.UserBehavior;

import java.io.IOException;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * @Description: 读取数据
 * @author: willzhao E-mail: zq2599@gmail.com
 * @date: 2020/5/2 15:02
 */
public class UserBehaviorCsvFileReader implements Supplier<UserBehavior> {

    private final String filePath;
    private CsvReader csvReader;

    public UserBehaviorCsvFileReader(String filePath) throws IOException {

        this.filePath = filePath;
        try {
            csvReader = new CsvReader(filePath);
            csvReader.readHeaders();
        } catch (IOException e) {
            throw new IOException("Error reading TaxiRecords from file: " + filePath, e);
        }
    }

    @Override
    public UserBehavior get() {
        UserBehavior userBehavior = null;
        try{
            if(csvReader.readRecord()) {
                csvReader.getRawRecord();
                userBehavior = new UserBehavior(
                        Long.valueOf(csvReader.get(0)),
                        Long.valueOf(csvReader.get(1)),
                        Long.valueOf(csvReader.get(2)),
                        csvReader.get(3),
                        new Date(Long.valueOf(csvReader.get(4))*1000L));
            }
        } catch (IOException e) {
            throw new NoSuchElementException("IOException from " + filePath);
        }

        if (null==userBehavior) {
            throw new NoSuchElementException("All records read from " + filePath);
        }

        return userBehavior;
    }
}