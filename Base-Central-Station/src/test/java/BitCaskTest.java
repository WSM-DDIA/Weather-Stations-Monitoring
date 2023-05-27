import bitCask.storage.BitCask;
import bitCask.util.Constants;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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

        String key = "1";
        String value = "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}";
        bitCask.put(key, value);

        String retrievedValue = bitCask.get("1");
        assertEquals(value, retrievedValue);
    }

    @Test
    public void protoBufReadingAfterCompaction() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("1", "{station_id:1, s_no:6, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.0, temperature:14.7, wind_speed:19.5}}");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("1", "{station_id:1, s_no:16, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:90.0, temperature:14.7, wind_speed:19.5}}");
        bitCask.put("2", "{station_id:2, s_no:62, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:83.2321, temperature:14.7, wind_speed:119.5}}");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("1", "{station_id:1, s_no:126, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:813.0, temperature:141.7, wind_speed:1239.5}}");
        bitCask.put("3", "{station_id:3, s_no:63, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:833.0, temperature:1421.7, wind_speed:192.5}}");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.mergeAndCompaction();

        bitCask = new BitCask(Constants.dbDirectory);

        String retrievedValue = bitCask.get("1");
        assertEquals("{station_id:1, s_no:126, battery_status:'medium', status_timestamp:1685210887, weather:{humidity:813.0, temperature:141.7, wind_speed:1239.5}}", retrievedValue);
    }

    @Test
    public void get() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);

        String key = "A";
        String value = "1";
        bitCask.put(key, value);
        key = "B";
        value = "2";
        bitCask.put(key, value);

        String retrievedValue = bitCask.get("A");
        assertEquals("1", retrievedValue);
    }

    @Test
    public void get_afterCompaction() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "1");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "2");
        bitCask.put("AC", "3");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "3");
        bitCask.put("B", "3");

        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.mergeAndCompaction();

        bitCask = new BitCask(Constants.dbDirectory);

        String retrievedValue = bitCask.get("A");
        assertEquals("3", retrievedValue);
    }

    @Test
    public void put() {
    }

    @Test
    public void mergeAndCompaction() throws IOException {
        BitCask bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "1");
        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "2");
        bitCask = new BitCask(Constants.dbDirectory);
        bitCask.put("A", "3");
        bitCask = new BitCask(Constants.dbDirectory);

        bitCask.mergeAndCompaction();

        assertEquals(5, Objects.requireNonNull(new File(Constants.dbDirectory).listFiles()).length);

        String retrievedValue = bitCask.get("A");
        assertEquals("3", retrievedValue);
    }
}
