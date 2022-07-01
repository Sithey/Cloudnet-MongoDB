package fr.sithey.cloudnet.ext.database.mongodb;

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import de.dytanic.cloudnet.common.concurrent.ITask;
import de.dytanic.cloudnet.common.concurrent.ListenableTask;
import de.dytanic.cloudnet.common.document.gson.JsonDocument;
import de.dytanic.cloudnet.driver.database.Database;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

import static com.mongodb.client.model.Filters.eq;

public class MongoDBDatabase implements Database {

    private static final String TABLE_COLUMN_KEY = "uuid", TABLE_COLUMN_VALUE = "json";

    private final boolean sync;
    private MongoCollection<Document> collection;
    private final ExecutorService executorService;

    public MongoDBDatabase(MongoDBDatabaseProvider mongoDBDatabaseProvider, String collection, boolean sync){
        executorService = mongoDBDatabaseProvider.executorService;
        boolean create = true;
        for (String name : mongoDBDatabaseProvider.mongoDB.listCollectionNames())
            if (name.equals(collection))
                create = false;
        if (create)
            mongoDBDatabaseProvider.mongoDB.createCollection(collection);
        this.collection = mongoDBDatabaseProvider.mongoDB.getCollection(collection);
        this.sync = sync;
    }

    @Override
    public boolean insert(String key, JsonDocument document) {
        Document doc = new Document().append(TABLE_COLUMN_KEY, key).append(TABLE_COLUMN_VALUE, document.toJson());
        doc.append(TABLE_COLUMN_KEY, key);
        if (contains(key)){
            update(key, document);
        }else{
            collection.insertOne(new Document().append(TABLE_COLUMN_KEY, key).append(TABLE_COLUMN_VALUE, document.toJson()));
            System.out.println("insert " + key + ": " + document.toPrettyJson());
        }
        return true;
    }

    @Override
    public boolean update(String key, JsonDocument document) {
        if (contains(key)){
            collection.replaceOne(eq(TABLE_COLUMN_KEY, key), new Document().append(TABLE_COLUMN_KEY, key).append(TABLE_COLUMN_VALUE, document.toJson()));
            System.out.println("update " + key + ": " + document.toPrettyJson());
        }else{
            insert(key, document);
        }
        return true;
    }

    @Override
    public boolean contains(String key) {
        System.out.println("contain " + key);
        return collection.find(eq(TABLE_COLUMN_KEY, key)).first() != null;
    }

    @Override
    public boolean delete(String key) {
        collection.deleteOne(eq(TABLE_COLUMN_KEY, key));
        System.out.println("delete " + key);
        return true;
    }

    @Override
    public JsonDocument get(String key) {
        if (contains(key))
            return JsonDocument.newDocument(collection.find(eq(TABLE_COLUMN_KEY, key)).first().getString(TABLE_COLUMN_VALUE));
        return null;
    }

    @Override
    public List<JsonDocument> get(String fieldName, Object fieldValue) {
        List<JsonDocument> jsons = new ArrayList<>();
        collection.find().forEach((Block<? super Document>) doc -> {
            JsonDocument json = JsonDocument.newDocument(doc.get(TABLE_COLUMN_VALUE));
            if (json.get(fieldName).equals(fieldValue)){
                jsons.add(json);
            }
        });
        return jsons;
    }

    @Override
    public List<JsonDocument> get(JsonDocument filters) {
        List<JsonDocument> jsons = new ArrayList<>();
        collection.find().forEach((Block<? super Document>) doc -> {
            JsonDocument json = JsonDocument.newDocument(doc.get(TABLE_COLUMN_VALUE));
            if (json.toJson().equals(filters.toJson())){
                jsons.add(json);
            }
        });
        return jsons;
    }

    @Override
    public Collection<String> keys() {
        Collection<String> keys = new ArrayList<>();
        collection.find().forEach((Block<? super Document>) doc -> {
            keys.add(doc.getString(TABLE_COLUMN_KEY));
        });
        return keys;
    }

    @Override
    public Collection<JsonDocument> documents() {
        List<JsonDocument> jsons = new ArrayList<>();
        collection.find().forEach((Block<? super Document>) doc -> {
            jsons.add(JsonDocument.newDocument(doc.getString(TABLE_COLUMN_VALUE)));
        });
        return jsons;
    }

    @Override
    public Map<String, JsonDocument> entries() {
        Map<String, JsonDocument> value = new HashMap<>();
        collection.find().forEach((Block<? super Document>) doc -> {
            value.put(doc.getString(TABLE_COLUMN_KEY), JsonDocument.newDocument(doc.getString(TABLE_COLUMN_VALUE)));
        });
        return value;
    }

    @Override
    public Map<String, JsonDocument> filter(BiPredicate<String, JsonDocument> predicate) {
        Map<String, JsonDocument> value = new HashMap<>(entries());
        new HashMap<>(value).forEach((key, json) -> {
            if (!predicate.test(key, json)){
                value.remove(key);
            }
        });
        return value;
    }

    @Override
    public void iterate(BiConsumer<String, JsonDocument> consumer) {
        entries().forEach(consumer);
    }

    @Override
    public void clear() {
        collection.drop();
    }

    @Override
    public long getDocumentsCount() {
        return collection.count();
    }

    @Override
    public boolean isSynced() {
        return sync;
    }

    @Override
    
    public ITask<Boolean> insertAsync(String key, JsonDocument document) {
        return this.schedule(() -> this.insert(key, document));
    }

    @Override
    public  ITask<Boolean> updateAsync(String key, JsonDocument document) {
        return this.schedule(() -> this.update(key, document));
    }

    @Override
    
    public ITask<Boolean> containsAsync(String key) {
        return this.schedule(() -> this.contains(key));
    }

    @Override
    
    public ITask<Boolean> deleteAsync(String key) {
        return this.schedule(() -> this.delete(key));
    }

    @Override
    
    public ITask<JsonDocument> getAsync(String key) {
        return this.schedule(() -> this.get(key));
    }

    @Override
    
    public ITask<List<JsonDocument>> getAsync(String fieldName, Object fieldValue) {
        return this.schedule(() -> this.get(fieldName, fieldValue));
    }

    @Override
    
    public ITask<List<JsonDocument>> getAsync(JsonDocument filters) {
        return this.schedule(() -> this.get(filters));
    }

    @Override
    
    public ITask<Collection<String>> keysAsync() {
        return this.schedule(this::keys);
    }

    @Override
    
    public ITask<Collection<JsonDocument>> documentsAsync() {
        return this.schedule(this::documents);
    }

    @Override
    
    public ITask<Map<String, JsonDocument>> entriesAsync() {
        return this.schedule(this::entries);
    }

    @Override
    
    public ITask<Map<String, JsonDocument>> filterAsync(BiPredicate<String, JsonDocument> predicate) {
        return this.schedule(() -> this.filter(predicate));
    }

    @Override
    
    public ITask<Void> iterateAsync(BiConsumer<String, JsonDocument> consumer) {
        return this.schedule(() -> {
            this.iterate(consumer);
            return null;
        });
    }

    @Override
    
    public ITask<Void> clearAsync() {
        return this.schedule(() -> {
            this.clear();
            return null;
        });
    }

    @Override
    public ITask<Long> getDocumentsCountAsync() {
        return this.schedule(this::getDocumentsCount);
    }

    @Override
    public String getName() {
        return "mongodb";
    }

    @Override
    public void close() throws Exception {

    }
    
    private <T> ITask<T> schedule(Callable<T> callable) {
        ITask<T> task = new ListenableTask<>(callable);
        this.executorService.execute(() -> {
            try {
                Thread.sleep(0, 100000);
                task.call();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        });
        return task;
    }
}
