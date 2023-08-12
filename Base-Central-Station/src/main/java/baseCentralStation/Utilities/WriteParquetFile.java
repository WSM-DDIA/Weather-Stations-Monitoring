package baseCentralStation.Utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class WriteParquetFile {

    private static final String parquetDirectory = "Parquet_Files_Directory";
    private static final long time = System.currentTimeMillis();
    private final Path rootDirectory;
    private final FileSystem parquetFile;
    private final Configuration configuration;
    private static final MessageType parquetSchema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("station_id")
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("s_no")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("battery_status")
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("status_timestamp")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("humidity")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("temperature")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("wind_speed")
            .named("WeatherStatus");
    private static final HashMap<String, Integer> parquetSize = new HashMap<>();
    private static final HashMap<String,Integer> parquetVersion = new HashMap<>();
    private static final Map<Path, ParquetWriter<Group>> writerStore = new HashMap<>();

    public WriteParquetFile() throws IOException {
        // Initialize HDFS
        this.configuration = new Configuration();
        this.parquetFile = FileSystem.get(this.configuration);
        this.rootDirectory = new Path(parquetDirectory);
        if (!this.parquetFile.exists(rootDirectory)) {
            this.parquetFile.mkdirs(rootDirectory);
        }

        for (int i = 1 ; i <= 10 ; i++){
            parquetVersion.put(String.valueOf(i), 1);
            parquetSize.put(String.valueOf(i), 0);
        }
    }

    public void write(WeatherStatusMessage weatherStatus) throws IOException {
        LocalDate date = LocalDate.ofEpochDay(Long.parseLong(weatherStatus.getStatusTimestamp()) / 86400);
        Path parquetPath = new Path(rootDirectory,
                String.format("%04d/%02d/%02d/%s/",
                        date.getYear(),
                        date.getMonthValue(),
                        date.getDayOfMonth(),
                        "Station_" + weatherStatus.getStationId()
                ) + "Version_" + parquetVersion.get(weatherStatus.getStationId()) + "_" + time);

        ParquetWriter<Group> writer = writerStore.get(parquetPath);
        if (writer == null) {
            writer = createParquetWriter(parquetPath, configuration);
            writerStore.put(parquetPath, writer);
        }

        writer.write(weatherStatus.toGroup(parquetSchema));
        parquetSize.put(weatherStatus.getStationId(), parquetSize.get(weatherStatus.getStationId()) + 1);

        if (parquetSize.get(weatherStatus.getStationId()) >= 50) { // Flush every 1000 records
            writer.close();
            renameFile(parquetPath.toString(), new File(parquetPath.toString()));
            parquetSize.put(weatherStatus.getStationId(), 0);
            parquetVersion.put(weatherStatus.getStationId(), parquetVersion.get(weatherStatus.getStationId()) + 1);
            writerStore.remove(parquetPath);
        }
    }

    private void renameFile(String fileNameToRename, File fileToRename) {
        File renamedFileWithoutSuffix = new File(fileNameToRename + ".parquet");
        boolean rename = fileToRename.renameTo(renamedFileWithoutSuffix);
        if (!rename)
            System.out.println("Failed to rename file");
    }



    private static ParquetWriter<Group> createParquetWriter(Path parquetPath, Configuration conf) throws IOException {
        return  CustomParquetWriter.builder(HadoopOutputFile.fromPath(parquetPath, conf))
                .withType(parquetSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build();
    }
}
