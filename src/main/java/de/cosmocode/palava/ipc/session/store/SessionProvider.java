/**
 * Copyright 2010 CosmoCode GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.cosmocode.palava.ipc.session.store;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import de.cosmocode.collections.Procedure;
import de.cosmocode.palava.concurrent.BackgroundScheduler;
import de.cosmocode.palava.core.Registry;
import de.cosmocode.palava.core.lifecycle.Disposable;
import de.cosmocode.palava.core.lifecycle.Initializable;
import de.cosmocode.palava.core.lifecycle.LifecycleException;
import de.cosmocode.palava.ipc.*;
import de.cosmocode.palava.jmx.MBeanRegistered;
import de.cosmocode.palava.jmx.MBeanService;
import de.cosmocode.palava.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Tobias Sarnowski
 */
class SessionProvider implements IpcSessionProvider, Runnable,
        Initializable, Disposable, SessionProviderMBean {
    private static final Logger LOG = LoggerFactory.getLogger(SessionProvider.class);

    // store metadata list key (a key, which cannot be possible for a session id)
    private static final String DATA_KEY = "session_data";

    // be default, hydrate after an hour of non-usage
    private static final long DEHYDRATION_TIME = 1;
    private static final TimeUnit DEHYDRATION_TIME_UNIT = TimeUnit.HOURS;

    // dependencies
    private final Store store;
    private ScheduledExecutorService scheduledExecutorService;
    private final Registry registry;

    // configuration
    private final long expirationTime;
    private final TimeUnit expirationTimeUnit;
    private MBeanService mBeanService;

    // memory storage
    private ConcurrentMap<String, Session> sessions;

    private boolean shuttingDown = false;


    @Inject
    public SessionProvider(@IpcSessionStore Store store,
                           Registry registry,
                           @BackgroundScheduler ScheduledExecutorService scheduledExecutorService,
                           @Named(IpcSessionConfig.EXPIRATION_TIME) long expirationTime,
                           @Named(IpcSessionConfig.EXPIRATION_TIME_UNIT) TimeUnit expirationTimeUnit,
                           MBeanService mBeanService)
    {
        this.store = store;
        this.registry = registry;
        this.scheduledExecutorService = scheduledExecutorService;
        this.expirationTime = expirationTime;
        this.expirationTimeUnit = expirationTimeUnit;
        this.mBeanService = mBeanService;
    }


    @Override
    public IpcSession getSession(String sessionId, String identifier) {
        Preconditions.checkState(!shuttingDown, this + " already shuts down; retrieving sessions is not allowed anymore.");

        Session session = null;

        // look for a stored session
        if (sessionId != null) {
            session = sessions.get(sessionId);
        } else {
            LOG.trace("No sessionId given, creating a new one");
        }

        if (session != null) {
            // session found, but valid?

            if (session.isExpired()) {
                // expired
                removeSession(session);
                session = null;

            } else if (session.getIdentifier() != identifier
                    && (session.getIdentifier() != null && !session.getIdentifier().equals(identifier))) {
                // wrong identifier
                LOG.debug("Session {} requested with the wrong identifier {}", session, identifier);
                session = null;
            }
        }

        if (session == null) {
            // no session to use, create a new one
            session = createSession(identifier);
        }

        return session;
    }

    private Session createSession(String identifier) {
        UUID sessionId;
        final Session session;

        // find an unused session id
        do {
            sessionId = UUID.randomUUID();
        } while (sessions.containsKey(sessionId.toString()));

        // create the new session
        session = new Session(sessionId.toString(), identifier, store);
        session.setTimeout(expirationTime, expirationTimeUnit);
        sessions.put(session.getSessionId(), session);

        // throw session create event
        registry.notifySilent(IpcSessionCreateEvent.class, new Procedure<IpcSessionCreateEvent>() {
            @Override
            public void apply(IpcSessionCreateEvent input) {
                input.eventIpcSessionCreate(session);
            }
        });

        LOG.debug("Created {}", session);
        return session;
    }

    private void removeSession(final Session session) {
        // remove from our storage
        sessions.remove(session.getSessionId());
        session.clear();

        // throw session destroy event
        registry.notifySilent(IpcSessionDestroyEvent.class, new Procedure<IpcSessionDestroyEvent>() {
            @Override
            public void apply(IpcSessionDestroyEvent input) {
                input.eventIpcSessionDestroy(session);
            }
        });

        LOG.debug("Destroyed {}", session);
    }


    @Override
    public void run() {
        // watch for expired sessions and sessions to hydrate
        for (Session session: sessions.values()) {
            if (session.isExpired()) {
                removeSession(session);
            } else if (session.isHydrated()) {
                // calculate if the session is unused since a while and hydrate it
                long millisSinceLastUse = System.currentTimeMillis() - session.lastAccessTime().getTime();
                if (millisSinceLastUse >= DEHYDRATION_TIME_UNIT.toMillis(DEHYDRATION_TIME)) {
                    try {
                        session.dehydrate();
                    } catch (IOException e) {
                        LOG.warn("Unable to dehydrate {}: {}", session, e);
                    }
                }
            }
        }
    }

    @Override
    public void initialize() throws LifecycleException {
        // load map
        try {
            ObjectInputStream objin = new ObjectInputStream(store.read(DATA_KEY));
            sessions = (ConcurrentMap<String, Session>) objin.readObject();
            objin.close();
            LOG.info("Loaded {} sessions from store.", sessions.size());
        } catch (IllegalArgumentException e) {
            LOG.info("No session data found; starting with new pool", e);
            sessions = null;
        } catch (IOException e) {
            LOG.error("Cannot load session data; starting with new pool", e);
            sessions = null;
        } catch (ClassNotFoundException e) {
            LOG.error("Incompatible session data; starting with new pool", e);
            sessions = null;
        } catch (IllegalStateException e) {
            LOG.warn("No session pool data found.", e);
            sessions = null;
        }
        if (sessions == null) {
            sessions = new MapMaker().makeMap();
        }

        try {
            store.delete(DATA_KEY);
        } catch (IOException e) {
            // if we cannot delete it, we cannot store it - bad
            throw new LifecycleException(e);
        } catch (IllegalStateException e) {
            // we already mentioned it above
        }

        // set store implementation to loaded sessions
        for (Session session: sessions.values()) {
            session.setStore(store);
        }

        mBeanService.register(this);

        // schedule myself for clean ups
        scheduledExecutorService.scheduleAtFixedRate(this, 1, 15, TimeUnit.MINUTES);
    }

    @Override
    public void dispose() throws LifecycleException {
        shuttingDown = true;

        mBeanService.unregister(this);

        LOG.debug("Storing {} sessions...", sessions.size());

        // dehydrate all sessions
        Map<Exception, Long> thrownExceptions = Maps.newTreeMap(new Comparator<Exception>() {
            @Override
            public int compare(Exception e1, Exception e2) {
                return e1.toString().compareTo(e2.toString());
            }
        });
        for (Session session: sessions.values()) {
            try {
                session.dehydrate();
            } catch (Exception e) {
                Long counter = thrownExceptions.get(e);
                if (counter == null) {
                    counter = 1L;
                } else {
                    counter++;
                }
                thrownExceptions.put(e, counter);
                removeSession(session);
            }
        }
        for (Map.Entry<Exception,Long> entry: thrownExceptions.entrySet()) {
            LOG.error(entry.getValue() + " sessions failed to dehydrate with the following exception", entry.getKey());
        }

        // store the data list
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ObjectOutputStream objout = null;
        try {
            objout = new ObjectOutputStream(buffer);

            objout.writeObject(sessions);
            objout.close();

            store.create(new ByteArrayInputStream(buffer.toByteArray()), DATA_KEY);
        } catch (IOException e) {
            throw new LifecycleException(e);
        }
        LOG.info("Stopped with {} sessions in store.", sessions.size());
    }

    @Override
    public int getSessionCount() {
        return sessions.size();
    }

    @Override
    public int getHydratedSessionCount() {
        int count = 0;
        for (Session session: sessions.values()) {
            if (session.isHydrated()) {
                count++;
            }
        }
        return count;
    }
}
