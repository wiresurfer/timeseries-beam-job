package org.atria.operations.vault.datatype;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public final class InputRecord implements Serializable {

    private String t = "";
    private String ta = "";
    private String q = "";
    private String v = "";

    public String getT() { return t; }
    public void setT(String t) {this.t = t;}
    public String getTa() {return ta;}
    public void setTa(String ta) {this.ta = ta;}
    public String getQ() {return q;}
    public void setQ(String q) {this.q = q;}
    public String getV() {return v;}
    public void setV(String v) {this.v = v;}


    public KV<String,TableRow> toTableRow(String source) {
        try {
            Pair<String, String> assetTag = JsonToTableRow.parseTag(this.getTa());

            List<String> parts_split = new ArrayList<String>(
                    Arrays.asList(source.split("/"))
            );
            String site = parts_split.get(0);
            String table = parts_split.get(1);
            long ts = Long.parseLong(this.getT());// / 1000;
            Date date = new Date();

            TableRow row = new com.google.api.services.bigquery.model.TableRow()
//                        .set("siteId", site)
                    .set("turbine", assetTag.getKey())
                    .set("tag_name", assetTag.getValue())
                    .set("tag_value", this.getV())
                    .set("quality", this.getQ())
                    .set("rec_date_time", ts)
                    .set("created_at", Math.round(date.getTime() / 1000));
            return  KV.of(table,row);
        }
        catch(Exception ex)
        {
            return null;
        }
    }
}