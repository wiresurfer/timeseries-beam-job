package org.atria.operations.vault.datatype;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 18/06/18.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */
class JsonToTableRow {

    private static final Map<String,String> TagConfig = new HashMap<>();
    static {
        TagConfig.put("Hiriyur.Site.", "Hiriyur.");
        TagConfig.put("Hiriyur.Assets.", "Hiriyur.Assets.");
        TagConfig.put("Kabali.Site.", "Kabali.");
        TagConfig.put("Kabali.Assets.", "Kabali.Assets.");
        TagConfig.put("Koghalli.Site.", "Koghalli.");
        TagConfig.put("Koghalli.Assets.", "Koghalli.Assets.");
        TagConfig.put("Ryapte.Site.", "Ryapte.");
        TagConfig.put("Ryapte.Assets.", "Ryapte.Assets.");
        TagConfig.put("Atria GV Palli.Site.", "Atria GV Palli.");
        TagConfig.put("Atria GV Palli.Turbines.", "Atria GV Palli.Turbines.");
        TagConfig.put("Atria Borampally.Site.", "Atria Borampally.");
        TagConfig.put("Atria Borampally.Turbines.", "Atria Borampally.Turbines.");
        TagConfig.put("Chitradurga.Site.", "Chitradurga.");
        TagConfig.put("Chitradurga.Turbines.", "Chitradurga.Turbines.");
        TagConfig.put("Kayathar_UA.Kayathar.", "Kayathar_UA.Kayathar.");
        TagConfig.put("Kukuru_UA.UA.", "Kukuru_UA.UA.");
        TagConfig.put("AtriaBB.", "AtriaBB.");
    }


    static Pair<String,String> tagToAssetId(String tag, String prefix) {
        String parts = tag.replace(prefix, "");
        List<String> parts_split = new ArrayList<String>(
                Arrays.asList(parts.split("\\."))
        );
        String assetId = parts_split.get(0);
        String tagName = String.join(".",  parts_split.subList(1, parts_split.size()));
        return new ImmutablePair(assetId, tagName);
    }
    public static Pair<String,String> parseTag(String tag) {


        Set<String> prefixKeys = TagConfig.keySet();
//        System.out.println(tag);
        String matched =  prefixKeys.stream().filter(x -> tag.startsWith(x)).findFirst().get();
        if(matched != null)
        {
            String prefix = TagConfig.get(matched);
            return tagToAssetId(tag, prefix);
        }
        else {

            return null;
        }

    }



    public static List<KV<String,ScadaRecord>> fromPubsubMessage(PubsubMessage json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            final String topic = json.getAttribute("mqtt_topic");
            final String origin = topic.replace("gateway/","");

            Map<String,List<InputRecord>> myObjects = mapper.readValue(json.getPayload(), new TypeReference<Map<String, List<InputRecord>>>() {});
            List<InputRecord> records =  myObjects.get("data");

            if(records!= null && records.size() > 0){
                List<KV<String,ScadaRecord>>  values = records.stream()
                        .map(r ->  ScadaRecord.toScadaRecord(origin,r))
                        .collect(Collectors.toList());

                System.out.print(".");
//                System.out.println("Processed from: " + values.get(0).getValue().getRec_date_time() + " to " + values.get(values.size() -1).getValue().getRec_date_time());
                return values;
            }
            else {
                return null;
            }
        }catch (Exception ex) {
            System.out.println("-");
            return null;
        }
    }

}
