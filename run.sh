export JAVA_OPTS='-XshowSettings:vm -XX:+UnlockExperimentalVMOptions -Xms512m -Xmx2048m -XX:MaxRAMFraction=1'
scala -classpath ./beaamjob_2.12-0.1.0-SNAPSHOT.jar org.atria.operations.dg.DGAggregateStream --streaming=true --project=atria-ee-platform --tempLocation=gs://real_time_temp_data  --gcsUploadBufferSizeBytes=2621440
