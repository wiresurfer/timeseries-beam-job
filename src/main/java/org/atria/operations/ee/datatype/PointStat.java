package org.atria.operations.ee.datatype;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 12/09/18.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */

@DefaultCoder(AvroCoder.class)
public class PointStat implements Serializable {
    public String key;
    public Long ts =0l;
    public Long ts_end =0l;
    public Long count=0l;
    public Double mean = 0.0;
    public Double stddev = 0.0;
    public Double sum = 0.0;
    public Double min = Double.MAX_VALUE;
    public Double max = Double.MIN_VALUE;
    public Double first = Double.MIN_VALUE;
    public Double last = Double.MIN_VALUE;

    public PointStat() {
    }

    public PointStat(String key) {
        this.key = key;
        this.count = 1l;
    }

    public PointStat(String key, Long ts, Long count, Double mean, Double stddev, Double sum, Double min, Double max) {
        this.key = key;
        this.ts = ts;
        this.count = count;
        this.mean = mean;
        this.stddev = stddev;
        this.sum = sum;
        this.min = min;
        this.max = max;
    }

    public PointStat(String key, Long ts,Long ts_end,
                     Long count, Double first, Double last,
                     Double mean, Double stddev,
                     Double sum,
                     Double min, Double max) {
        this.key = key;
        this.ts = ts;
        this.ts_end = ts_end;

        this.count = count;
        this.first = first;
        this.last = last;

        this.mean = mean;
        this.stddev = stddev;

        this.sum = sum;

        this.min = min;
        this.max = max;
    }

    public static TableRow toTableRow(PointStat cs) {
//        new TableFieldSchema().setName("dev_id").setType("STRING").setMode("REQUIRED"),
//                new TableFieldSchema().setName("p_id").setType("STRING").setMode("REQUIRED"),
//                new TableFieldSchema().setName("rec_t").setType("TIMESTAMP").setMode("REQUIRED"),
//                new TableFieldSchema().setName("create_t").setType("TIMESTAMP").setMode("REQUIRED"),
//                new TableFieldSchema().setName("count").setType("DOUBLE").setMode("REQUIRED"),
//                new TableFieldSchema().setName("avg").setType("DOUBLE").setMode("REQUIRED"),
//                new TableFieldSchema().setName("sum").setType("DOUBLE").setMode("REQUIRED"),
//                new TableFieldSchema().setName("min").setType("DOUBLE").setMode("REQUIRED"),
//                new TableFieldSchema().setName("max").setType("DOUBLE").setMode("REQUIRED"),
//                new TableFieldSchema().setName("stddev").setType("DOUBLE").setMode("REQUIRED")
        Date date = new Date();
        String[] parts = cs.key.split("\\.");
        String dev_id = parts[0];
        String p_id = String.join(".",
                Arrays.copyOfRange(parts,1,parts.length));
        return new TableRow()
                .set("dev_id", dev_id)
                .set("p_id", p_id)
                .set("rec_t", new Long((cs.ts/1000)))
                .set("create_t",  new Long(Math.round(date.getTime() / 1000)))
                .set("count", cs.count)
                .set("avg", cs.mean)
                .set("sum", cs.sum)
                .set("min", cs.min)
                .set("max", cs.max)
                .set("stddev", cs.stddev);
    }

    public PointStat accumulateValue(Long ts, Long ts_end, Double x) {
        // TS, key, first remain unchanged
        this.ts = ts < this.ts ? ts: this.ts == 0l ? ts : this.ts;
        this.ts_end = ts_end  > this.ts_end ? ts_end : this.ts_end;

        this.first = this.first == Double.MIN_VALUE ? x : this.first;
        this.last = ts_end  >= this.ts_end ? x : this.ts_end == 0l ? x : this.last;
        if(x < this.min )
            this.min = x;
        if(x > this.max)
            this.max = x;


        this.sum = this.sum + x;


//        Double var = ((this.count)*this.stddev*this.stddev   + (x - this.mean)*(x - this.mean))/(this.count + 1);
        Double Sn_1 = this.stddev;
        Long N = this.count + 1;
        Double Xn_1 = this.mean;
        Double Xn = this.sum / N;

        Double var = ((N - 1)*Sn_1*Sn_1   + (x - Xn_1)* (x - Xn))/N;


        this.stddev = Math.sqrt(var);;
        this.count = N;
        this.mean = Xn;
        return this;
    }



    public  static PointStat accumulate(PointStat a, PointStat b){

        if(a.key == null && b.key == null)
            return a;
        else if(a.key == null && a.count == 0)
            return b;
        else if(b.key == null && b.count == 0)
            return a;

        if(!a.key.equals(b.key))
            return null;

        if(a.count == 0 && b.count == 0)
            return null;

        if(a.ts == 0)
            a.ts = b.ts;

        if(b.ts == 0)
            b.ts = a.ts;


        PointStat out = new PointStat();
        out.key = a.key;
        out.count = a.count + b.count;
        out.max = a.max >= b.max ? a.max : b.max;
        out.min = a.min <= b.min ? a.min : b.min;
        out.sum = a.sum + b.sum;
        out.ts = a.ts <= b.ts ? a.ts : b.ts;
        out.ts_end = a.ts_end >= b.ts_end ? a.ts_end : b.ts_end;

        out.first = a.ts <= b.ts ? a.first : b.first;
        out.last = a.ts_end >= b.ts_end ? a.last: b.last;

        out.mean = (a.sum  + b.sum)/(a.count + b.count);
        out.stddev = Math.sqrt(
                a.count * ( Math.pow(a.stddev,2) +  Math.pow(a.mean - out.mean, 2)) +
                        b.count * ( Math.pow(b.stddev,2) +  Math.pow(b.mean - out.mean, 2))
        )/(a.count + b.count) ;

        return out;

    }

    public static PointStat copy(PointStat a) {
        return new PointStat(a.key, a.ts, a.ts_end,
                a.count,
                a.first,a.last,
                a.mean,a.stddev,
                a.sum, a.min,a.max);
    }
    public static PointStat accumulateIterable(Iterable<PointStat> a) {
        PointStat out = new PointStat();
        Iterator<PointStat> a_it = a.iterator();
        while(a_it.hasNext())
        {
            PointStat a_ = a_it.next();
            out = PointStat.accumulate(out,a_);

        }
        return out;
    }


    public static PointStat accumulateIterable(Long start, Long end, Iterable<PointStat> a) {
        PointStat out = new PointStat();
        out.ts = start;
        out.ts_end = end;

        Iterator<PointStat> a_it = a.iterator();
        while(a_it.hasNext())
        {
            PointStat a_ = a_it.next();
            out = PointStat.accumulate(out,a_);

        }
        PointStat finalOut = PointStat.copy(out);
        finalOut.ts = start;
        finalOut.ts_end = end;

        return finalOut;
    }


    public static Iterable<PointStat> accumulateIterable(Iterable<PointStat> a, Iterable<PointStat> b) {
        PointStat out = new PointStat();
        Iterator<PointStat> a_it = a.iterator();
        Iterator<PointStat> b_it = b.iterator();
        while(a_it.hasNext())
        {
            PointStat a_ = a_it.next();
            out = PointStat.accumulate(out,a_);

        }

        while(b_it.hasNext())
        {
            PointStat b_ = b_it.next();
            out = PointStat.accumulate(out,b_);
        }

        ArrayList<PointStat> fin = new ArrayList<>();
        fin.add(out);
        return fin;
    }


    @Override
    public String toString() {
        return String.join(
                ", ",
                key,
                ts.toString(),ts_end.toString(),
                count.toString(),mean.toString(),stddev.toString(),
                sum.toString(),min.toString(),max.toString(),
                first.toString(), last.toString()
        );
    }

    @Override public boolean equals(Object other) {
        boolean result = false;
        if (other instanceof PointStat) {
            PointStat that = (PointStat) other;
            result = (this.key == that.key
                    && this.ts == that.ts
                    && this.ts_end == that.ts_end
                    && this.count == that.ts_end
                    && this.first == that.first
                    && this.last == that.last
                    && this.mean == that.mean
                    && this.stddev == that.stddev
                    && this.sum == that.sum
                    && this.min == that.min
                    && this.max == that.max

            );
        }
        return result;
    }
}
