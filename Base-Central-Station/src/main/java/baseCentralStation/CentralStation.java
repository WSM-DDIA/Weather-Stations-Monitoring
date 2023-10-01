package baseCentralStation;

import baseCentralStation.Utilities.*;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.util.List;

public class CentralStation {
    private static final String bootstrapServers = "localhost:9092";
    private static final int bitCaskPort = 4240;
    private static final String bitCaskIP = "localhost";
    private static final String topic = "weather-status-messages";
    private static final String dataDirectory = "Parquet_Files_Data";
    private static final String bitCaskDirectory = "/home/bazina/IdeaProjects/Weather-Stations-Monitoring/Base-Central-Station/src/main/resources/";

    /**
     * This method is the main method of the Central Station.
     * It initializes the KafkaAPI, RocksDB, BitCaskClient and ParquetWriter.
     * It consumes messages from the Kafka topic and writes them to the BitCask store and Parquet files.
     *
     * @throws Exception if the KafkaAPI, RocksDB, BitCaskClient or ParquetWriter fail to initialize
     */
    public static void invoke() throws Exception {
        KafkaAPI kafkaAPI = new KafkaAPI(bootstrapServers, topic);

        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        RocksDB invalidMessageChannel = RocksDB.open(options, dataDirectory);

        BitCaskClient bitCaskClient = new BitCaskClient();
        bitCaskClient.startConnection(bitCaskIP, bitCaskPort);
        bitCaskClient.open(bitCaskDirectory);

        StationParquetFileWriter stationParquetFileWriter = new StationParquetFileWriter();
        while (true) {
            List<String> records = kafkaAPI.consumeMessages();
            for (String record : records) {
                if (Parsing.validate(record)) {
                    System.out.println("Message Received Successfully & is valid");
                    WeatherStatusMessage weatherStatus = new WeatherStatusMessage(Parsing.parse(record));

                    String status = bitCaskClient.put(Integer.parseInt(weatherStatus.getStationId()), weatherStatus.toString());
                    System.out.println(status);

                    stationParquetFileWriter.write(weatherStatus);
                } else {
                    System.out.println("invalid message");
                    invalidMessageChannel.put("Invalid".getBytes(), record.getBytes());
                }

            }
        }
    }
}
