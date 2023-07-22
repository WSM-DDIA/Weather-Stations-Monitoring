package baseCentralStation.Utilities;

import com.google.common.primitives.Ints;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RequestParameterBuilder {
    protected static byte[] bitCaskFirstParametersAsBytes(int key, byte operationType) throws IOException {
        byte[] keyBytes = Ints.toByteArray(key);
        int keySize = keyBytes.length;

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(new byte[]{operationType});
        byteArrayOutputStream.write(Ints.toByteArray(keySize));
        byteArrayOutputStream.write(keyBytes);

        return byteArrayOutputStream.toByteArray();
    }

    protected static byte[] bitCaskSetParametersAsBytes(int key, String value) throws IOException {
        byte[] firstParameter = bitCaskFirstParametersAsBytes(key, (byte) 2);

        byte[] valueBytes = WeatherStatusMessage.parseValueToBytesArray(value);
        int valueSize = valueBytes.length;

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(firstParameter);
        byteArrayOutputStream.write(Ints.toByteArray(valueSize));
        byteArrayOutputStream.write(valueBytes);

        return byteArrayOutputStream.toByteArray();
    }
}
