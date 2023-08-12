package bitCask.util;

public class DiskResponse {
    String fileName;
    String replicaFileName;
    long valuePosition;

    public DiskResponse(String fileName, long valuePosition) {
        this.fileName = fileName;
        this.replicaFileName = DirectoryConstants.getFileTimeStamp(fileName) + DirectoryConstants.ReplicaExtension;
        this.valuePosition = valuePosition;
    }

    public String getFileName() {
        return fileName;
    }

    public String getReplicaFileName() {
        return replicaFileName;
    }

    public long getValuePosition() {
        return valuePosition;
    }
}
