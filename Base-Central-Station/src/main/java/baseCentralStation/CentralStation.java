package baseCentralStation;

import baseCentralStation.Utilities.*;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.util.List;

public class CentralStation {
    private static final String bootstrapServers = "localhost:9092";
    private static final int bitCaskPort = 4240;
    private static final String topic = "weather-status-messages";
    private static final String data_dir = "Parquet_Files_Data";

    public static void invoke() throws Exception {
        // Initialize KafkaAPI
        KafkaAPI kafkaAPI = new KafkaAPI(bootstrapServers, topic);

        // Initialize RocksDB
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        RocksDB invalidMessageChannel = RocksDB.open(options, data_dir);

        BitCaskClient bitCaskClient = new BitCaskClient();
        bitCaskClient.startConnection("localhost", bitCaskPort);

        // setup parquet files
        WriteParquetFile writer = new WriteParquetFile();
        while (true) {
            List<String> records = kafkaAPI.consumeMessages();
            for (String record : records) {
                if (Parsing.validate(record)) {
                    System.out.println("Message Received Successfully & is valid");
                    WeatherStatusMessage weatherStatus = new WeatherStatusMessage(Parsing.parse(record));

                    // Update BitCask store
                    bitCaskClient.sendMessage(weatherStatus.getStationId(), weatherStatus.toString());

                    // write data to parquet files
                    writer.write(weatherStatus);
                } else {
                    System.out.println("invalid message");
                    invalidMessageChannel.put("Invalid".getBytes(), record.getBytes());
                }

            }
        }
    }
}
