package bitCask.handler;

import bitCask.storage.BitCask;
import com.google.common.primitives.Ints;

public record DeleteHandler(byte[] key) implements MessageHandler {
    /**
     * Executes the delete command which deletes the key.
     *
     * @param bitCask {@link BitCask} instance
     * @return byte array representation of status
     */
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
