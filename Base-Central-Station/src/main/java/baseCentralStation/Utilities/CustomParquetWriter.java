package baseCentralStation.Utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class CustomParquetWriter extends ParquetWriter<Group> {

    CustomParquetWriter(Path file, WriteSupport<Group> writeSupport,
                        CompressionCodecName compressionCodecName,
                        int blockSize, int pageSize, boolean enableDictionary,
                        boolean enableValidation,
                        ParquetProperties.WriterVersion writerVersion,
                        Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    /**
     * Create a new {@link Builder} for configuring a {@link CustomParquetWriter}.
     *
     * @param file the file name for this parquet file
     * @return a builder that can be used to create the writer
     */
    public static Builder builder(OutputFile file) {
        return new Builder(file);
    }

    /**
     * Builder class to configure the {@link CustomParquetWriter} that we are going to build.
     */
    public static class Builder extends ParquetWriter.Builder<Group, Builder> {
        private final Map<String, String> extraMetaData = new HashMap<>();
        private MessageType type = null;

        private Builder(OutputFile file) {
            super(file);
        }

        /**
         * Sets type for the builder.
         *
         * @param type the schema of the data that will be written
         * @return this builder for method chaining.
         */
        public Builder withType(MessageType type) {
            this.type = type;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Group> getWriteSupport(Configuration conf) {
            return new CustomGroupWriteSupport(type, extraMetaData);
        }

    }
}