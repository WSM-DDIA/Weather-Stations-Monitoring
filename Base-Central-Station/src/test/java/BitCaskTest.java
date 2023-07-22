import bitCask.storage.BitCask;
import bitCask.util.Constants;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class BitCaskTest {
    @Before
    public void tearDown() {
        Arrays.stream(Objects.requireNonNull(new File(Constants.dbDirectory).listFiles()))
                .forEach(File::delete);
    }

    @Test
    public void protoBufReading() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);

        int key = 1;
        String value = "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}";
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(key).array(), value.getBytes());

        byte[] retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(key).array());
        assertEquals(value.getBytes(), retrievedValue);
    }

    @Test
    public void protoBufReadingAfterCompaction() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}".getBytes());

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:16, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:90.0, temperature:14.7, wind_speed:19.5}}".getBytes());
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(2).array(), "{station_id:2, s_no:62, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.2321, temperature:14.7, wind_speed:119.5}}".getBytes());

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(1).array(), "{station_id:1, s_no:126, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:813.0, temperature:141.7, wind_speed:1239.5}}".getBytes());
        bitCask.put(ByteBuffer.allocate(Integer.BYTES).putInt(3).array(), "{station_id:3, s_no:63, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:833.0, temperature:1421.7, wind_speed:192.5}}".getBytes());

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.mergeAndCompaction();

        bitCask = new BitCask(Constants.dbDirectory);

        byte[] retrievedValue = bitCask.get(ByteBuffer.allocate(Integer.BYTES).putInt(1).array());
        assertEquals("{station_id:1, s_no:126, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:813.0, temperature:141.7, wind_speed:1239.5}}".getBytes(), retrievedValue);
    }
}
