package bitCask.handler;

import bitCask.exception.InvalidCommandException;
import com.google.common.primitives.Ints;

import java.util.Arrays;

public class HandlerFactory {
    public MessageHandler parseMessage(byte[] message) throws InvalidCommandException {
        int cursor = 0;

        byte operationTypeByte = message[0];
        byte[] parameters = Arrays.copyOfRange(message, cursor + 1, message.length);

        return switch (operationTypeByte) {
            case Constants.GET -> parseGetMessage(parameters);
            case Constants.SET -> parseSetMessage(parameters);
            default -> throw new InvalidCommandException("Operation not supported");
        };
    }

    private MessageHandler parseSetMessage(byte[] parameters) {
        int cursor = 0;

        int keySize = Ints.fromByteArray(Arrays.copyOfRange(parameters, cursor, Integer.BYTES));
        cursor += Integer.BYTES;
        byte[] keyBytes = Arrays.copyOfRange(parameters, cursor, cursor + keySize);
        cursor += keySize;

        int valueSize = Ints.fromByteArray(Arrays.copyOfRange(parameters, cursor, cursor + Integer.BYTES));
        cursor += Integer.BYTES;
        byte[] valueBytes = Arrays.copyOfRange(parameters, cursor, cursor + valueSize);

        return new SetHandler(keyBytes, valueBytes);
    }

    private MessageHandler parseGetMessage(byte[] parameters) {
        int keySize = Ints.fromByteArray(Arrays.copyOfRange(parameters, 0, Integer.BYTES));
        byte[] keyBytes = Arrays.copyOfRange(parameters, Integer.BYTES, Integer.BYTES + keySize);

        return new GetHandler(keyBytes);
    }
}
