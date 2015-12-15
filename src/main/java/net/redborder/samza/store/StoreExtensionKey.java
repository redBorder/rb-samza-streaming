package net.redborder.samza.store;

public class StoreExtensionKey {
    public final String namespace;
    public final String collection;
    public final String key;
    public final String mergeKey;
    public final Boolean transform;

    private StoreExtensionKey(String namespace, String collection, String key, Boolean transform) {
        this.namespace = namespace;
        this.collection = collection;
        this.key = key;
        this.mergeKey = namespace + collection + key;
        this.transform = transform;
    }

    public static class Builder {
        private String namespace;
        private String collection;
        private String key;
        private Boolean transform = false;

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder transform(Boolean transform) {
            this.transform = transform;
            return this;
        }

        public StoreExtensionKey build() {
            return new StoreExtensionKey(namespace, collection, key, transform);
        }
    }

    @Override
    public String toString() {
        return "Namespace: " + namespace + " Collection: " + collection + " Key: " + key;
    }
}
