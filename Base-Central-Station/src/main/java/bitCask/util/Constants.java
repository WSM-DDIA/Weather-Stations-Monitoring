package bitCask.util;

public class Constants {
    public static final String FILE_PREFIX = "file_";
    public static final String REPLICA_FILE_PREFIX = "replica_file_";

    public static final String HINT_FILE_PREFIX = "hint_";

    public static final String dbDirectory = "/home/bazina/IdeaProjects/Weather-Stations-Monitoring/Base-Central-Station/src/main/resources/";

    public static final long MEMORY_LIMIT = 32768;

    private static byte[] convertByteArrayStringToByteArray(String value) {
        String[] byteValues = value.substring(1, value.length() - 1).split(",");
        byte[] bytes = new byte[byteValues.length];
        for (int i = 0, len = bytes.length; i < len; i++) {
            bytes[i] = Byte.parseByte(byteValues[i].trim());
        }
        return bytes;
    }
}
