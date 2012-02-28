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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import de.cosmocode.palava.concurrent.BackgroundSchedulerModule;
import de.cosmocode.palava.concurrent.DefaultThreadProviderModule;
import de.cosmocode.palava.core.DefaultRegistryModule;
import de.cosmocode.palava.core.Framework;
import de.cosmocode.palava.core.Palava;
import de.cosmocode.palava.core.lifecycle.LifecycleModule;
import de.cosmocode.palava.ipc.IpcSession;
import de.cosmocode.palava.ipc.IpcSessionProvider;
import de.cosmocode.palava.jmx.FakeMBeanServerModule;
import de.cosmocode.palava.store.MemoryStoreModule;
import de.cosmocode.palava.store.Store;

/**
 * @author Oliver Lorenz
 * @since 2.0
 */
public class SessionTest {

    private final Framework framework = Palava.newFramework(new AbstractModule() {
        @Override
        protected void configure() {
            install(new LifecycleModule());
            install(new DefaultRegistryModule());
            install(new StoreIpcSessionModule());

            install(new FakeMBeanServerModule());
            install(new BackgroundSchedulerModule());
            install(new DefaultThreadProviderModule());
            install(new MemoryStoreModule());
            bind(Store.class).annotatedWith(IpcSessionStore.class).to(Store.class);

            bindConstant().annotatedWith(Names.named("ipc.session.expirationTime")).to(5L);
            bindConstant().annotatedWith(Names.named("ipc.session.expirationTimeUnit")).to(TimeUnit.MINUTES);

            bindConstant().annotatedWith(Names.named("executors.named.background.minPoolSize")).to(1);
            bindConstant().annotatedWith(Names.named("executors.named.background.shutdownTimeout")).to(20L);
            bindConstant().annotatedWith(Names.named("executors.named.background.shutdownTimeoutUnit")).to(TimeUnit.SECONDS);
        }
    }, new Properties());

    @Before
    public void setUp() throws Exception {
        framework.start();
    }

    @After
    public void tearDown() throws Exception {
        framework.stop();
    }

    @Test
    public void putIntoSession() {
        final IpcSessionProvider sessionProvider = framework.getInstance(IpcSessionProvider.class);
        final IpcSession session = sessionProvider.getSession("newSession", "127.0.0.1");
        session.put("test", "testvalue");
        Assert.assertEquals("testvalue", session.get("test"));
    }

    @Test
    public void createNewSession() {
        final IpcSessionProvider sessionProvider = framework.getInstance(IpcSessionProvider.class);
        final IpcSession session1 = sessionProvider.getSession(null, "123.123.123.123");
        final IpcSession session2 = sessionProvider.getSession(session1.getSessionId(), "123.123.123.123");
        Assert.assertSame(session1, session2);
    }

}
