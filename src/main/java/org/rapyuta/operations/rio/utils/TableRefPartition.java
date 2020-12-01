package org.rapyuta.operations.rio.utils;

/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 29/06/18.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;

//2019-11-04T20:30:00.000000


public class TableRefPartition implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {
    private final String projectId;
    private final String datasetId;
    private final String pattern;
    private final String table;

    public static TableRefPartition perDay(String projectId, String datasetId, String tablePrefix) {
        return new TableRefPartition(projectId, datasetId, "yyyyMMdd", tablePrefix + "$");
    }

    private TableRefPartition(String projectId, String datasetId, String pattern, String table) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.pattern = pattern;
        this.table = table;
    }

    @Override
    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
        DateTimeFormatter partition = DateTimeFormat.forPattern(pattern).withZone(
//                                        DateTimeZone.forOffsetHoursMinutes(5,30)
                                          DateTimeZone.UTC
                                      );

        TableReference reference = new TableReference();
        reference.setProjectId(this.projectId);
        reference.setDatasetId(this.datasetId);
//        Long ts = (Long)(input.getValue().get("ts_end"));
        Long ts = input.getTimestamp().getMillis();
        if(ts == null)
        {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            ts = timestamp.getTime();

        }
        Instant ins = new Instant().withMillis(ts);
        Boolean cuspentry = ins.get(DateTimeFieldType.hourOfDay()) == 23 && ins.get(DateTimeFieldType.minuteOfHour()) == 44;
        if(cuspentry)
            ins = ins.plus(Duration.standardMinutes(20l));

        String par = ins.toString(partition);
        par = input.getValue().get("ts_end").toString();
        par = par.split("T")[0];
        par = par.replace("-","");
        reference.setTableId(table + par );
        return new TableDestination(reference, null);
    }
}