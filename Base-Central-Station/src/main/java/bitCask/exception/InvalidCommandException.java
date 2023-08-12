package bitCask.exception;

public class InvalidCommandException extends Exception {
    /**
     * Constructs a new invalid command exception with the specified detail message.
     *
     * @param message the detail message.
     */
    public InvalidCommandException(String message) {
        super(message);
    }
}