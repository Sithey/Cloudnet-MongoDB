package fr.sithey.cloudnet.ext.database.mongodb;

import de.dytanic.cloudnet.database.AbstractDatabaseProvider;
import de.dytanic.cloudnet.driver.module.ModuleLifeCycle;
import de.dytanic.cloudnet.driver.module.ModuleTask;
import de.dytanic.cloudnet.module.NodeCloudNetModule;

public class CloudNetMongoDBDatabaseModule extends NodeCloudNetModule {
    private static CloudNetMongoDBDatabaseModule instance;

    @ModuleTask(order = 127, event = ModuleLifeCycle.LOADED)
    public void init() {
        instance = this;
        getConfig().getString("url", ""); getConfig().getString("database", "database");
        getConfig().getString("async", "false");
        saveConfig();
    }

    public static CloudNetMongoDBDatabaseModule get() {
        return instance;
    }

    @ModuleTask(order = 125, event = ModuleLifeCycle.LOADED)
    public void registerDatabaseProvider() {
        getRegistry().registerService(AbstractDatabaseProvider.class, "mongodb", new MongoDBDatabaseProvider(getConfig()));
    }

    @ModuleTask(order = 127, event = ModuleLifeCycle.STOPPED)
    public void unregisterDatabaseProvider() {
        getRegistry().unregisterService(AbstractDatabaseProvider.class, "mongodb");
    }
}
