package fr.sithey.cloudnet.ext.database.mongodb;

import com.mongodb.Block;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import de.dytanic.cloudnet.common.collection.NetorHashMap;
import de.dytanic.cloudnet.common.collection.Pair;
import de.dytanic.cloudnet.common.document.gson.JsonDocument;
import de.dytanic.cloudnet.database.AbstractDatabaseProvider;
import de.dytanic.cloudnet.driver.database.Database;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MongoDBDatabaseProvider extends AbstractDatabaseProvider {

    private static final long NEW_CREATION_DELAY = 600000;

    private final JsonDocument config;

    public MongoClient mongoClient;
    public MongoDatabase mongoDB;
    protected ExecutorService executorService;

    protected final NetorHashMap<String, Long, MongoDBDatabase> cachedDatabaseInstances = new NetorHashMap<>();

    public MongoDBDatabaseProvider(JsonDocument config){
        this.config = config;
         this.executorService = Executors.newCachedThreadPool();
    }


    @Override
    public boolean init() {
        this.mongoClient = MongoClients.create(config.getString("url"));
        this.mongoDB = mongoClient.getDatabase(config.getString("database"));
        return true;
    }

    @Override
    public String getName() {
        return "mongodb";
    }

    @Override
    public Database getDatabase(String name) {
        this.removedOutdatedEntries();
        System.out.println("check " + name);
        if (!this.cachedDatabaseInstances.contains(name)) {
            this.cachedDatabaseInstances.add(name, System.currentTimeMillis() + NEW_CREATION_DELAY, new MongoDBDatabase(this, name, Boolean.parseBoolean(config.getString("async"))));
        }
        return cachedDatabaseInstances.getSecond(name);
    }

    @Override
    public boolean containsDatabase(String name) {
        this.removedOutdatedEntries();

        for (String database : this.getDatabaseNames()) {
            if (database.equalsIgnoreCase(name)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean deleteDatabase(String name) {
        this.cachedDatabaseInstances.remove(name);
        if (containsDatabase(name)){
            getDatabase(name).clear();
        }
        return false;
    }

    @Override
    public Collection<String> getDatabaseNames() {
        Collection<String> dbs = new ArrayList<>();
        mongoDB.listCollectionNames().forEach((Block<? super String>) dbs::add);
        return dbs;
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
        this.executorService.shutdownNow();
    }

    private void removedOutdatedEntries() {
        for (Map.Entry<String, Pair<Long, MongoDBDatabase>> entry : this.cachedDatabaseInstances.entrySet()) {
            if (entry.getValue().getFirst() < System.currentTimeMillis()) {
                this.cachedDatabaseInstances.remove(entry.getKey());
            }
        }
    }
}
