package bitCask.handler;

import bitCask.exception.InvalidCommandException;
import com.google.common.primitives.Ints;

import java.util.Arrays;

public class HandlerFactory {
    /**
     * Parses given input byte array to a concrete {@link MessageHandler} implementation.
     *
     * @param message byte array representation of input
     * @return concrete implementation of {@link MessageHandler} interface
     * @throws InvalidCommandException If the given command is an invalid command
     */
    public MessageHandler parseMessage(byte[] message) throws InvalidCommandException {
        int cursor = 0;

        byte operationTypeByte = message[0];
        byte[] parameters = Arrays.copyOfRange(message, cursor + 1, message.length);

        return switch (operationTypeByte) {
            case Operations.OPEN -> parseOpenMessage(parameters);
            case Operations.GET -> parseGetMessage(parameters);
            case Operations.SET -> parseSetMessage(parameters);
            case Operations.DELETE -> parseDeleteMessage(parameters);
            default -> throw new InvalidCommandException("Operation not supported");
        };
    }

    /**
     * Parses the SET command.
     *
     * @param parameters byte array representation of input
     * @return concrete implementation of {@link MessageHandler} interface as {@link SetHandler}
     */
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

    /**
     * Parses the GET command.
     *
     * @param parameters byte array representation of input
     * @return concrete implementation of {@link MessageHandler} interface as {@link GetHandler}
     */
    private MessageHandler parseGetMessage(byte[] parameters) {
        int keySize = Ints.fromByteArray(Arrays.copyOfRange(parameters, 0, Integer.BYTES));
        byte[] keyBytes = Arrays.copyOfRange(parameters, Integer.BYTES, Integer.BYTES + keySize);

        return new GetHandler(keyBytes);
    }

    /**
     * Parses the OPEN command.
     *
     * @param parameters byte array representation of input
     * @return concrete implementation of {@link MessageHandler} interface as {@link OpenHandler}
     */
    private MessageHandler parseOpenMessage(byte[] parameters) {
        String dbDirectory = new String(parameters);
        return new OpenHandler(dbDirectory);
    }

    /**
     * Parses the DELETE command.
     *
     * @param parameters byte array representation of input
     * @return concrete implementation of {@link MessageHandler} interface as {@link DeleteHandler}
     */
    private MessageHandler parseDeleteMessage(byte[] parameters) {
        int keySize = Ints.fromByteArray(Arrays.copyOfRange(parameters, 0, Integer.BYTES));
        byte[] keyBytes = Arrays.copyOfRange(parameters, Integer.BYTES, Integer.BYTES + keySize);

        return new DeleteHandler(keyBytes);
    }
}
