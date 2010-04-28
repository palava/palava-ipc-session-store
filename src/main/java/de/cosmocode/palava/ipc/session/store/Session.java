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
import com.google.common.collect.Maps;
import de.cosmocode.palava.ipc.AbstractIpcSession;
import de.cosmocode.palava.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

/**
 * @author Tobias Sarnowski
 */
class Session extends AbstractIpcSession implements Serializable {
    private transient static final Logger LOG = LoggerFactory.getLogger(Session.class);

    // store for hydration
    private transient Store store;

    // basic meta data
    private final String sessionId;
    private final String identifier;

    // the data storage
    private Map<Object, Object> data = Maps.newHashMap();


    protected Session(String sessionId, String identifier, Store store) {
        this.sessionId = sessionId;
        this.identifier = identifier;
        this.store = store;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    protected Map<Object, Object> context() {
        if (!isHydrated()) hydrate();
        return data;
    }

    /**
     * @return "+" symbolizes the in-memory session, "-" means, the session is in the storage
     */
    @Override
    public String toString() {
        return "{Session" + (isHydrated()?"+":"-") + ":" + getSessionId() + "/" + getIdentifier() + "}";
    }

    protected void setStore(Store store) {
        this.store = store;
    }

    public boolean isHydrated() {
        return data != null;
    }

    public void dehydrate() throws IOException {
        Preconditions.checkState(isHydrated(), "session %s already dehydrated", this);
        LOG.trace("Dehydrating {}", this);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ObjectOutputStream objout = new ObjectOutputStream(buffer);

        objout.writeObject(this.data);
        objout.close();

        store.create(new ByteArrayInputStream(buffer.toByteArray()), sessionId);

        this.data = null;
    }

    private void hydrate() {
        Preconditions.checkState(!isHydrated(), "session %s is already hydrated", this);
        LOG.trace("Hydrating {}", this);

        try {
            ObjectInputStream objin = new ObjectInputStream(store.read(sessionId));
            this.data = (Map<Object, Object>) objin.readObject();
            objin.close();
            store.delete(sessionId);
        } catch (IOException e) {
            LOG.error("IO exception on loading session data for " + this, e);
            this.data = null;
        } catch (ClassNotFoundException e) {
            LOG.error("Incompatible session data found for " + this, e);
            this.data = null;
        } catch (IllegalStateException e) {
            LOG.error("Session data not found for " + this, e);
            this.data = null;
        } finally {
            if (this.data == null) {
                LOG.warn("Hydrating failed, continuing with a vanilla session.");
                this.data = Maps.newHashMap();
            }
        }
    }
}
