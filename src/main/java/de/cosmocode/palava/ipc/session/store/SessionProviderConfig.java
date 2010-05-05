package de.cosmocode.palava.ipc.session.store;

import de.cosmocode.palava.ipc.IpcSessionConfig;

/**
 * Static constant holder class for session provider config key names.
 *
 * @since 
 * @author Willi Schoenborn
 */
public final class SessionProviderConfig {

    public static final String PREFIX = IpcSessionConfig.PREFIX;
    
    public static final String DEHYDRATION_TIME = PREFIX + "dehydrationTime";

    public static final String DEHYDRATION_TIME_UNIT = PREFIX + "dehydrationTimeUnit";
    
    private SessionProviderConfig() {
        
    }

}
