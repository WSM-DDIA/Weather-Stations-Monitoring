package baseCentralStation.Utilities;

import com.google.common.primitives.Ints;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RequestParameterBuilder {
    /**
     * Builds the parameters for a get and delete request to the BitCask server.
     *
     * @param key the key of the value
     * @return the parameters as a byte array
     * @throws IOException if the conversion fails
     */
    protected static byte[] bitCaskFirstParametersAsBytes(int key, byte operationType) throws IOException {
        byte[] keyBytes = Ints.toByteArray(key);
        int keySize = keyBytes.length;

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(new byte[]{operationType});
        byteArrayOutputStream.write(Ints.toByteArray(keySize));
        byteArrayOutputStream.write(keyBytes);

        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Builds the parameters for a set request to the BitCask server.
     *
     * @param key   the key of the value
     * @param value the value to be stored
     * @return the parameters as a byte array
     * @throws IOException if the conversion fails
     */
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

    /**
     * Builds the parameters for an open request to the BitCask server.
     *
     * @param directory the directory of the BitCask server
     * @return the parameters as a byte array
     * @throws IOException if the conversion fails
     */
    protected static byte[] bitCaskOpenParametersAsBytes(String directory) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write((byte) 0);
        byteArrayOutputStream.write(directory.getBytes());

        return byteArrayOutputStream.toByteArray();
    }
}
