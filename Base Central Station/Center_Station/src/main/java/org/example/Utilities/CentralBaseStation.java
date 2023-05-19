package org.example.Utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CentralBaseStation {
    private static final String bootstrapServers = "localhost:9092";
    private static final String topic = "weather-status-messages";

    private static final String DATA_DIR = "data";
    private static final String PARQUET_DIR = "Base_Central_Station_Parquet_Files";

    private static final MessageType parquetSchema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("station_id")
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("s_no")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("battery_status")
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("status_timestamp")
//            .optionalGroup()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("humidity")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("temperature")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("wind_speed")
//            .named("Weather")
            .named("WeatherStatus");


    private static final Map<Long, byte[]> keyStore = new HashMap<>();
    private static final Map<Path, ParquetWriter<Group>> writerStore = new HashMap<>();

    public static void invoke() throws Exception {
        KafkaAPI kafkaAPI = new KafkaAPI(bootstrapServers, topic);

        // Initialize RocksDB
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, DATA_DIR);

        // Initialize HDFS
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path rootDir = new Path(PARQUET_DIR);
        if (!fs.exists(rootDir)) {
            fs.mkdirs(rootDir);
        }
        HashMap<String,Integer> va = new HashMap<>();
        va.put("1",1);va.put("2",1);va.put("3",1);va.put("4",1);va.put("5",1);va.put("6",1);va.put("7",1);va.put("8",1);va.put("9",1);va.put("10",1);

        int currentBatchSize = 0;
        while (true) {
            List<String> message = kafkaAPI.consumeMessages();
            List<Group> groupBuffer = new ArrayList<>();
            for(int i=0;i< message.size();i++){
                // Remove the prefix and suffix to get the key-value pairs
                Pattern pattern = Pattern.compile("station_id=(\\d+), s_no=(\\d+), battery_status='(\\w+)', status_timestamp=(\\d+), weather=\\{humidity=(\\d+(?:\\.\\d+)?), temperature=(\\d+(?:\\.\\d+)?), wind_speed=(\\d+(?:\\.\\d+)?)\\}");

                Matcher matcher = pattern.matcher(message.get(i));
                HashMap<String, String> map = new HashMap<>();
                if (matcher.find()) {
                    map.put("station_id", matcher.group(1));
                    map.put("s_no", matcher.group(2));
                    map.put("battery_status", matcher.group(3));
                    map.put("status_timestamp", matcher.group(4));
                    map.put("humidity", matcher.group(5));
                    map.put("temperature", matcher.group(6));
                    map.put("wind_speed", matcher.group(7));
                }
                WeatherStatus weatherStatus = new WeatherStatus(map);
                // Update BitCask store
//                byte[] key = createKey(weatherStatus.getStationId(), weatherStatus.getSNo());
//                db.put(key, serialize(weatherStatus));

                LocalDate date = LocalDate.ofEpochDay(Long.parseLong(weatherStatus.getStatusTimestamp()) / 86400);
                Path parquetPath = new Path(rootDir, String.format("%04d/%02d/%02d/%d",
                        date.getYear(), date.getMonthValue(), date.getDayOfMonth(), Integer.parseInt(weatherStatus.getStationId())) + "_" + va.get(weatherStatus.getStationId()) + ".parquet");
                ParquetWriter<Group> writer = writerStore.get(parquetPath);
                if (writer == null) {
                    writer = createParquetWriter(fs, parquetPath, conf);
                    writerStore.put(parquetPath, writer);
                }

                writer.write(weatherStatus.toGroup(parquetSchema));
                currentBatchSize++;
                System.out.println(currentBatchSize);
                if (currentBatchSize >= 50) { // Flush every 10K records
                        writer.close();
                        currentBatchSize = 0;
                    va.put(weatherStatus.getStationId(), va.get(weatherStatus.getStationId())+1);
                        writerStore.remove(parquetPath);
                }
            }
        }
    }

    private static byte[] createKey(String stationId, String sNo) {
        return (stationId + "-" + sNo).getBytes();
    }

    private static byte[] serialize(WeatherStatus weatherStatus) throws IOException {
        // Use Java serialization for simplicity
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(weatherStatus);
        return bos.toByteArray();
    }
    private static ParquetWriter<Group> createParquetWriter(FileSystem fs, Path parquetPath, Configuration conf) throws IOException {
        return  CustomParquetWriter.builder(HadoopOutputFile.fromPath(parquetPath, conf))
                .withType(parquetSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(Mode.CREATE)
                .build();
    }

}
/*
// process each message and create a Group object
                Group group = weatherStatus.toGroup(parquetSchema);
                groupBuffer.add(group);
                // increment the current batch size
                currentBatchSize++;
                // check if the batch size has been reached
                if (currentBatchSize >= 50) {
                    System.out.println("current Batch Size" + currentBatchSize);
                    // start a new thread that writes the batch of records to Parquet
                    List<Group> copyOfGroupBuffer = new ArrayList<>(groupBuffer);
                    Thread writerThread = new Thread(new ParquetWriterRunnable(copyOfGroupBuffer, fs, rootDir, conf));
                    writerThread.start();
                    // reset the batch size counter and clear the buffer
                    currentBatchSize = 0;
                    groupBuffer.clear();
                }
 */