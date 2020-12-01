package org.rapyuta.operations.rio.vault.datatype;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class ScadaRecord implements Serializable {
    private String siteId;
    private String asset;
    private String tag_name;
    private String tag_value;
    private String quality;
    private Long rec_date_time;
    private int created_at;

    public ScadaRecord setSiteId(String siteId) {
        this.siteId = siteId;
        return  this;

    }
    public ScadaRecord setAsset(String asset) {
        this.asset = asset;
        return  this;
    }
    public ScadaRecord setTag_name(String tag_name) {
        this.tag_name = tag_name;
        return  this;
    }
    public ScadaRecord setTag_value(String tag_value) {
        this.tag_value = tag_value;
        return  this;
    }
    public ScadaRecord setQuality(String quality) {
        this.quality = quality;
        return  this;
    }
    public ScadaRecord setRec_date_time(Long rec_date_time) {
        this.rec_date_time = rec_date_time;
        return  this;
    }
    public ScadaRecord setCreated_at(int created_at) {
        this.created_at = created_at;
        return  this;
    }

    public ScadaRecord(String siteId, String asset, String tag_name, String tag_value, String quality, Long rec_date_time, int created_at) {
        super();
        this.siteId = siteId;
        this.asset = asset;
        this.tag_name = tag_name;
        this.tag_value = tag_value;
        this.quality = quality;
        this.rec_date_time = rec_date_time;
        this.created_at = created_at;
    }

    public String keyBySiteAssetTag() {
        return String.join(".", Arrays.asList(siteId, asset, tag_name));

    }

    public String keyBySiteAsset() {
        return String.join(".", Arrays.asList(siteId, asset));

    }
    public String keyBySiteTag() {
        return String.join(".", Arrays.asList(siteId, tag_name));

    }
    public String keyByAssetTag() {
        return String.join(".", Arrays.asList(asset, tag_name));

    }

    public String getSiteId() {
        return siteId;
    }
    public String getAsset() {
        return asset;
    }
    public String getTag_name() {
        return tag_name;
    }
    public String getTag_value() {
        return tag_value;
    }


    public Double getValue() {
        return NumberUtils.toDouble(tag_value, -9999.0);
    }
    public String getQuality() {
        return quality;
    }
    public Long getRec_date_time() {
        return rec_date_time;
    }
    public int getCreated_at() {
        return created_at;
    }


    @Override public boolean equals(Object other) {
        boolean result = false;
        if (other instanceof ScadaRecord) {
            ScadaRecord that = (ScadaRecord) other;
            result = (this.getSiteId() == that.getSiteId()
                    && this.getAsset() == that.getAsset()
                    && this.getTag_name() == that.getTag_name()
                    && this.getTag_value() == that.getTag_value()
                    && this.getQuality() == that.getQuality()
                    && this.getRec_date_time() == that.getRec_date_time()
                    && this.getCreated_at() == that.getCreated_at()
            );
        }
        return result;
    }
    public static KV<String,ScadaRecord> toScadaRecord(String source, InputRecord t) {
        try {
            Pair<String, String> assetTag = JsonToTableRow.parseTag(t.getTa());

            List<String> parts_split = new ArrayList<String>(
                    Arrays.asList(source.split("/"))
            );
            String site = parts_split.get(0);
            String table = parts_split.get(1);
            long ts = Long.parseLong(t.getT());// / 1000;
            Date date = new Date();


            String value = t.getV();
            String quality = t.getQ();

            ScadaRecord record =  new ScadaRecord(
                    site,
                    assetTag.getKey(),
                    assetTag.getValue(),
                    value,
                    quality,
                    ts,
                    Math.round(date.getTime() / 1000)

            );
            return  KV.of(table,record);
        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
            System.out.println(t.getTa());
//            ex.printStackTrace();
            return null;
        }
    }
    public  static TableRow toTableRow(ScadaRecord s) {
        return   new com.google.api.services.bigquery.model.TableRow()
                    .set("turbine", s.getAsset())
                    .set("tag_name", s.getTag_name())
                    .set("tag_value", s.getTag_value())
                    .set("quality", s.getQuality())
                    .set("rec_date_time", s.getRec_date_time())
                    .set("created_at", s.getCreated_at());
    }


    public  static TableRow toTableRowSolar(ScadaRecord s) {
        return   new com.google.api.services.bigquery.model.TableRow()
                .set("root", s.getAsset())
                .set("tag_name", s.getTag_name())
                .set("tag_value", s.getTag_value())
                .set("quality", s.getQuality())
                .set("created_at", s.getRec_date_time()/1000);
    }
}
