package bitCask.handler;

import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

public record DeleteHandler(byte[] key) implements MessageHandler {
    @Override
    public byte[] execute(BitCask bitCask) {
        try {
            bitCask.delete(key);
            return Ints.toByteArray(200);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
