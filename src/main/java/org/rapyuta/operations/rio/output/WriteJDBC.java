package org.rapyuta.operations.rio.output;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.rapyuta.operations.rio.ee.datatype.PointStat;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 16/04/19.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */
public class WriteJDBC {


    public static JdbcIO.Write<PointStat> WritePointToJDBC(String db, String user, String pass) {
        return JdbcIO.<PointStat>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "org.postgresql.Driver", "jdbc:postgresql://192.168.1.149:5432/postgres")
                        .withUsername("postgres")
//                        .withPassword(pass)
                )
                .withStatement("insert into pointstat (client_slug,plant_slug,device_type,source_key,stream_name,count,mean,stddev,sum,min,max,first,last, updated_at)" +
                                    "values(?, ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ? , ?, ?)" +
                        "ON CONFLICT (client_slug,plant_slug,source_key,stream_name) DO UPDATE" +
                        "   SET count = excluded.count," +
                        "       mean = excluded.mean," +
                        "       stddev = excluded.stddev," +
                        "       sum = excluded.sum," +
                        "       min = excluded.min," +
                        "       max = excluded.max," +
                        "       first = excluded.first," +
                        "       last = excluded.last," +
                        "       updated_at = excluded.updated_at"
                )
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<PointStat>() {
                    public void setParameters(PointStat element, PreparedStatement query)

                            throws SQLException {
                                String[] parts =element.key.split("\\.");
                                String client_slug = parts[0];
                                String plant_slug = parts[1];
                                String device_type = parts[2];
                                String source_key = parts[3];
                                String stream_name = parts[4];



                                query.setString(1, client_slug);
                                query.setString(2, plant_slug);
                                query.setString(3, device_type);
                                query.setString(4, source_key);
                                query.setString(5, stream_name);

//                                query.setDate(5, new java.sql.Date(element.ts));


                                query.setLong(6, element.count);
                                query.setDouble(7, element.mean);
                                query.setDouble(8, element.stddev);
                                query.setDouble(9, element.sum);

                                query.setDouble(10, element.min);
                                query.setDouble(11, element.max);
                                query.setDouble(12, element.first);
                                query.setDouble(13, element.last);
                                query.setDate(14
                                        , new java.sql.Date(element.ts_end));
                    }
                });
    }
}
