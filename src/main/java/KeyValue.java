public class KeyValue {
    public String value;
    public long expiryTimestamp;

    public KeyValue(String value, long expiryTimestamp) {
        this.value = value;
        this.expiryTimestamp = expiryTimestamp;
    }
}