package bitCask.exception;

public class KeyNotFoundException extends Exception {
    /**
     * Constructs a new key not found exception with the specified detail message.
     *
     * @param message the detail message.
     */
    public KeyNotFoundException(String message) {
        super(message);
    }
}
