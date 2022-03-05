package com.rogerguo.kafka.test.cache;

public class Item {

    private String v_id="";
    private long date;
    private String longitude="";
    private String latitude="";

    public Item(String v_id, long date, String longitude, String latitude) {
        this.v_id=v_id;
        this.date=date;
        this.longitude=longitude;
        this.latitude=latitude;
    }
    public long getDate() {
        return date;
    }
    public String getV_id() {
        return v_id;
    }

    @Override
    public String toString() {
        return this.date + "," + this.latitude + "," + this.longitude + ";";
    }
}
