package org.rapyuta.operations.rio.output;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.sdk.transforms.DoFn;

import javax.annotation.Nullable;
import java.util.UUID;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 16/04/19.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */
public class WriteDataStore {

    public static class CreateEntityFn extends DoFn<String, Entity> {
        private final String namespace;
        private final String kind;
        private final com.google.datastore.v1.Key ancestorKey;

        static com.google.datastore.v1.Key makeAncestorKey(@Nullable String namespace, String kind) {

            com.google.datastore.v1.Key.Builder keyBuilder = makeKey(kind, "root");
            if (namespace != null) {
                keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
            }
            return keyBuilder.build();
        }


        CreateEntityFn(String namespace, String kind) {
            this.namespace = namespace;
            this.kind = kind;

            // Build the ancestor key for all created entities once, including the namespace.
            ancestorKey = makeAncestorKey(namespace, kind);
        }

        public Entity makeEntity(String content) {
            Entity.Builder entityBuilder = Entity.newBuilder();

            // All created entities have the same ancestor Key.
            com.google.datastore.v1.Key.Builder keyBuilder = makeKey(ancestorKey, kind, UUID.randomUUID().toString());
            // NOTE: Namespace is not inherited between keys created with DatastoreHelper.makeKey, so
            // we must set the namespace on keyBuilder. TODO: Once partitionId inheritance is added,
            // we can simplify this code.
            if (namespace != null) {
                keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
            }

            entityBuilder.setKey(keyBuilder.build());
//            entityBuilder.
//            entityBuilder.getMutableProperties().put("content", makeValue(content).build());

            return entityBuilder.build();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(makeEntity(c.element()));
        }
    }

}
