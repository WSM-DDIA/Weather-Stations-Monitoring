package baseCentralStation;

import baseCentralStation.Utilities.KafkaAPI;
import baseCentralStation.Utilities.Parsing;
import baseCentralStation.Utilities.WeatherStatusMessage;
import baseCentralStation.Utilities.WriteParquetFile;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class CentralStation {
    private static final String bootstrapServers = "localhost:9092";
    private static final String topic = "weather-status-messages";
    private static final String data_dir = "Parquet_Files_Data";

    private static final Map<Long, byte[]> keyStore = new HashMap<>();


    public static void invoke() throws Exception {

        Parsing parser = new Parsing();

        // Initialize KafkaAPI
        KafkaAPI kafkaAPI = new KafkaAPI(bootstrapServers, topic);

        // Initialize RocksDB
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        RocksDB invalidMessageChannel = RocksDB.open(options, data_dir);

        // setup parquet files
        WriteParquetFile writer = new WriteParquetFile();
        while (true) {
            List<String> records = kafkaAPI.consumeMessages();
            for (String record : records) {
                if(Parsing.validate(record)){
                    System.out.println("Message Received Successfully & is valid");
                    WeatherStatusMessage weatherStatus = new WeatherStatusMessage(Parsing.parse(record));

                    // Update BitCask store
//                byte[] key = createKey(weatherStatus.getStationId(), weatherStatus.getSNo());
//                db.put(key, serialize(weatherStatus));

                    // write data to parquet files
                    writer.write(weatherStatus);

                }else{
                    System.out.println("invalid message");
                    invalidMessageChannel.put("Invalid".getBytes(), record.getBytes());
                }

            }
        }
    }

    private static byte[] createKey(String stationId, String sNo) {
        return (stationId + "-" + sNo).getBytes();
    }

    private static byte[] serialize(WeatherStatusMessage weatherStatus) throws IOException {
        // Use Java serialization for simplicity
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(weatherStatus);
        return bos.toByteArray();
    }

}
