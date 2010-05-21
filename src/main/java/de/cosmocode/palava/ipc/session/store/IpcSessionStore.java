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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;

import de.cosmocode.palava.store.Store;

/**
 * Binding annotation for a {@link Store} which can be used
 * to store dehydrated sessions.
 * 
 * @author Willi Schoenborn
 * @author Tobias Sarnowski
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
    ElementType.METHOD, 
    ElementType.PARAMETER
})
@BindingAnnotation
public @interface IpcSessionStore {
}
