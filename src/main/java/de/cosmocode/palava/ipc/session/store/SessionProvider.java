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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import de.cosmocode.collections.Procedure;
import de.cosmocode.commons.State;
import de.cosmocode.commons.Stateful;
import de.cosmocode.palava.concurrent.Background;
import de.cosmocode.palava.core.Registry;
import de.cosmocode.palava.core.lifecycle.Disposable;
import de.cosmocode.palava.core.lifecycle.Initializable;
import de.cosmocode.palava.core.lifecycle.LifecycleException;
import de.cosmocode.palava.ipc.IpcSession;
import de.cosmocode.palava.ipc.IpcSessionConfig;
import de.cosmocode.palava.ipc.IpcSessionCreateEvent;
import de.cosmocode.palava.ipc.IpcSessionDestroyEvent;
import de.cosmocode.palava.ipc.IpcSessionProvider;
import de.cosmocode.palava.jmx.MBeanService;
import de.cosmocode.palava.store.Store;

/**
 * Provider and manager for storable {@link IpcSession}s.
 *
 * @author Willi Schoenborn
 * @author Tobias Sarnowski
 */
final class SessionProvider implements IpcSessionProvider, Stateful, Initializable, Runnable, Disposable,
    SessionProviderMBean {

    private static final Logger LOG = LoggerFactory.getLogger(SessionProvider.class);

    // store metadata list key (a key, which cannot be possible for a session id)
    private static final String DATA_KEY = "session_data";

    private final Store store;

    private final ScheduledExecutorService scheduledExecutorService;

    private final Registry registry;

    private final MBeanService mBeanService;

    private final long expirationTime;

    private final TimeUnit expirationTimeUnit;

    private long dehydrationTime = 1;

    private TimeUnit dehydrationTimeUnit = TimeUnit.HOURS;

    private ConcurrentMap<String, Session> sessions;

    private State state = State.NEW;

    @Inject
    public SessionProvider(
        @IpcSessionStore Store store,
        Registry registry,
        @Background ScheduledExecutorService scheduledExecutorService,
        @Named(IpcSessionConfig.EXPIRATION_TIME) long expirationTime,
        @Named(IpcSessionConfig.EXPIRATION_TIME_UNIT) TimeUnit expirationTimeUnit,
        MBeanService mBeanService) {

        this.store = store;
        this.registry = registry;
        this.scheduledExecutorService = scheduledExecutorService;
        this.expirationTime = expirationTime;
        this.expirationTimeUnit = expirationTimeUnit;
        this.mBeanService = mBeanService;
    }

    @Inject(optional = true)
    void setDehydrationTime(@Named(SessionProviderConfig.DEHYDRATION_TIME) long dehydrationTime) {
        this.dehydrationTime = dehydrationTime;
    }

    @Inject(optional = true)
    void setDehydrationTimeUnit(@Named(SessionProviderConfig.DEHYDRATION_TIME_UNIT) TimeUnit dehydrationTimeUnit) {
        this.dehydrationTimeUnit = Preconditions.checkNotNull(dehydrationTimeUnit, "DehydrationTimeUnit");
    }

    @Override
    public State currentState() {
        return state;
    }

    @Override
    public boolean isRunning() {
        return currentState() == State.RUNNING;
    }

    @Override
    public void initialize() throws LifecycleException {
        state = State.STARTING;

        
        try {
            final ObjectInputStream objin = new ObjectInputStream(store.read(DATA_KEY));
            
            try {
                @SuppressWarnings("unchecked")
                final ConcurrentMap<String, Session> map = (ConcurrentMap<String, Session>) objin.readObject();
                
                this.sessions = map;
            } finally {
                objin.close();
            }
            
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
            LOG.warn("No session pool data found.");
            sessions = null;
        }

        if (sessions == null) {
            sessions = new ConcurrentHashMap<String, Session>();
        }

        try {
            store.delete(DATA_KEY);
        } catch (IOException e) {
            throw new LifecycleException(e);
        } catch (IllegalStateException e) {
            LOG.trace("{} did not exist in store", DATA_KEY);
        }

        for (Session session : sessions.values()) {
            session.setStore(store);
        }

        mBeanService.register(this);

        scheduledExecutorService.scheduleAtFixedRate(this, 1, 15, TimeUnit.MINUTES);

        state = State.RUNNING;
    }

    @Override
    public IpcSession getSession(String sessionId, String identifier) {
        Preconditions.checkState(isRunning(),
            "%s is currently in %s state; retrieving sessions is not allowed.", this, state);

        if (sessionId == null) {
            LOG.trace("No sessionId given, creating a new one");
            return createSession(identifier);
        }

        final Session session = sessions.get(sessionId);

        if (session == null) {
            LOG.trace("No session found with id {}, creating new one", sessionId);
            return createSession(identifier);
        }

        if (session.isExpired()) {
            removeSession(session);
            return createSession(identifier);
        }

        if (session.getIdentifier() != identifier
                && (session.getIdentifier() != null && !session.getIdentifier().equals(identifier))) {
            LOG.debug("Session {} requested with the wrong identifier {}", session, identifier);
            return createSession(identifier);
        }

        return session;
    }

    private Session createSession(String identifier) {
        final UUID sessionId = UUID.randomUUID();
        final Session session = new Session(sessionId.toString(), identifier, store);
        session.setTimeout(expirationTime, expirationTimeUnit);
        sessions.put(session.getSessionId(), session);

        registry.notifySilently(IpcSessionCreateEvent.class, new Procedure<IpcSessionCreateEvent>() {

            @Override
            public void apply(IpcSessionCreateEvent input) {
                input.eventIpcSessionCreate(session);
            }

        });

        LOG.debug("Created {}", session);
        return session;
    }

    private void removeSession(final Session session) {
        sessions.remove(session.getSessionId());
        session.clear();

        registry.notifySilently(IpcSessionDestroyEvent.class, new Procedure<IpcSessionDestroyEvent>() {

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
        for (Session session : sessions.values()) {
            if (session.isExpired()) {
                removeSession(session);
            } else if (session.isHydrated()) {
                // calculate if the session is unused since a while and hydrate it
                final long millisSinceLastUse = System.currentTimeMillis() - session.lastAccessTime().getTime();
                if (millisSinceLastUse >= dehydrationTimeUnit.toMillis(dehydrationTime)) {
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
    public int getSessionCount() {
        return sessions.size();
    }

    @Override
    public int getHydratedSessionCount() {
        int count = 0;
        for (Session session : sessions.values()) {
            if (session.isHydrated()) count++;
        }
        return count;
    }

    @Override
    public void dispose() throws LifecycleException {
        state = State.STOPPING;

        try {
            mBeanService.unregister(this);
        } finally {
            LOG.debug("Storing {} sessions...", sessions.size());

            final Map<Exception, Long> thrownExceptions = Maps.newTreeMap(new Comparator<Exception>() {

                @Override
                public int compare(Exception left, Exception right) {
                    return left.toString().compareTo(right.toString());
                }

            });

            for (Session session : sessions.values()) {
                try {
                    if (session.isHydrated()) {
                        session.dehydrate();
                    }
                } catch (IOException e) {
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

            for (Map.Entry<Exception, Long> entry : thrownExceptions.entrySet()) {
                LOG.error(entry.getValue() + " sessions failed to dehydrate with the following exception",
                    entry.getKey());
            }

            final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            ObjectOutputStream objout = null;

            try {
                objout = new ObjectOutputStream(buffer);
                objout.writeObject(sessions);
                store.create(new ByteArrayInputStream(buffer.toByteArray()), DATA_KEY);
            } catch (IOException e) {
                state = State.FAILED;
                throw new LifecycleException(e);
            } finally {
                Closeables.closeQuietly(objout);
            }

            LOG.info("Stopped with {} sessions in store.", sessions.size());
            state = State.TERMINATED;
        }
    }

}
