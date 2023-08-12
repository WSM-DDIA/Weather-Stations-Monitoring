package bitCask.util;

import lombok.Getter;

@Getter
public class DiskResponse {
    String fileName;
    String replicaFileName;
    long valuePosition;

    public DiskResponse(String fileName, long valuePosition) {
        this.fileName = fileName;
        this.replicaFileName = DirectoryConstants.getFileTimeStamp(fileName) + DirectoryConstants.ReplicaExtension;
        this.valuePosition = valuePosition;
    }
}
