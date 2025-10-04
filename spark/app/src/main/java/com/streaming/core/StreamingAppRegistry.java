// src/main/java/com/streaming/core/StreamingAppRegistry.java
package com.streaming.core;

import com.streaming.exceptions.AppRegistrationException;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry that automatically discovers and registers all StreamingApp implementations.
 * Uses reflection to find all classes implementing StreamingApp interface.
 */
public class StreamingAppRegistry {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamingAppRegistry.class);
    private static final Map<String, Class<? extends StreamingApp>> registeredApps = new HashMap<>();
    
    static {
        try {
            discoverAndRegisterApps();
        } catch (Exception e) {
            logger.error("Failed to initialize StreamingAppRegistry", e);
            throw new AppRegistrationException("Failed to initialize registry", e);
        }
    }
    
    /**
     * Automatically discovers and registers all StreamingApp implementations.
     */
    private static void discoverAndRegisterApps() {
        logger.info("Starting automatic discovery of streaming applications...");
        
        Reflections reflections = new Reflections("com.streaming.apps");
        Set<Class<? extends StreamingApp>> appClasses = 
            reflections.getSubTypesOf(StreamingApp.class);
        
        for (Class<? extends StreamingApp> appClass : appClasses) {
            // Skip abstract classes and interfaces
            if (Modifier.isAbstract(appClass.getModifiers()) || 
                appClass.isInterface()) {
                continue;
            }
            
            try {
                // Instantiate the app to get its name
                StreamingApp appInstance = appClass.getDeclaredConstructor().newInstance();
                String appName = appInstance.getAppName();
                
                if (registeredApps.containsKey(appName)) {
                    throw new AppRegistrationException(
                        String.format("Duplicate app name '%s' found in classes: %s and %s",
                            appName, 
                            registeredApps.get(appName).getName(), 
                            appClass.getName())
                    );
                }
                
                registeredApps.put(appName, appClass);
                logger.info("Registered streaming app: {} -> {}", appName, appClass.getName());
                
            } catch (Exception e) {
                logger.error("Failed to register app class: {}", appClass.getName(), e);
                throw new AppRegistrationException(
                    "Failed to register app: " + appClass.getName(), e);
            }
        }
        
        logger.info("Discovered and registered {} streaming application(s)", 
            registeredApps.size());
    }
    
    /**
     * Gets an instance of the streaming app by name.
     * 
     * @param appName The name of the application
     * @return A new instance of the streaming app
     * @throws AppRegistrationException if app not found or instantiation fails
     */
    public static StreamingApp getApp(String appName) {
        Class<? extends StreamingApp> appClass = registeredApps.get(appName);
        
        if (appClass == null) {
            throw new AppRegistrationException(
                String.format("No streaming app found with name: '%s'. Available apps: %s",
                    appName, registeredApps.keySet())
            );
        }
        
        try {
            return appClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new AppRegistrationException(
                "Failed to instantiate app: " + appName, e);
        }
    }
    
    /**
     * Returns all registered app names.
     * 
     * @return Set of registered app names
     */
    public static Set<String> getRegisteredAppNames() {
        return registeredApps.keySet();
    }
}
