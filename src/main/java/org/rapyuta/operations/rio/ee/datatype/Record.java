package org.rapyuta.operations.rio.ee.datatype;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 17/09/18.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */

@DefaultCoder(AvroCoder.class)
public class Record {
    protected Long time;
    protected String sku;
    protected String dev_id;

    public Long getTime() {return time;}
    public void setTime(Long time) {this.time = time;}

    public void setSku(String sku) {
        this.sku = sku;
    }
    public String getSku() {
        return sku;
    }

    public String getDev_id() {return dev_id;}
    public void setDev_id(String dev_id) {this.dev_id = dev_id;}

    public List<Point> ToKeyedPointData(){
        return new ArrayList<>();
//        return list;
    }

    @Override public boolean equals(Object other) {
        boolean result = false;
        if (other instanceof Record) {
            Record that = (Record) other;
            result = (this.getDev_id() == that.getDev_id()
                    && this.getSku() == that.getSku()
                    && this.getTime() == that.getTime());

        }
        else result = false;
        return result;
    }

}