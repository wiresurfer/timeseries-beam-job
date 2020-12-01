package org.rapyuta.operations.rio.ee.datatype;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.values.KV;

/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 12/09/18.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */

@DefaultCoder(AvroCoder.class)
public class Point {
    String key;

    public Long getTs() {
        return ts;
    }

    Long ts;
    Double value;

    public Point(String key,Long ts, Double value) {
        this.key = key;
        this.ts = ts;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Double getValue() {
        return value;
    }

    public Point() {
    }

    public KV<String,Point> ToKV(){
        return KV.of(key, this);
    }

    @Override public boolean equals(Object other) {
        boolean result = false;
        if (other instanceof Point) {
            Point that = (Point) other;
            result = (this.key == that.key
                    && this.ts == that.ts
                    && this.value == that.value);

        }
        return result;
    }
}
