/* Copyright 2021 Norconex Inc.
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
package com.norconex.committer.neo4j;

import static org.apache.commons.lang3.StringUtils.trimToNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.value.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.encrypt.EncryptionUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.security.Credentials;

/**
 * <p>
 * Simple Neo4j client.
 * </p>
 * @author Pascal Essiembre
 */
class Neo4jClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(Neo4jClient.class);

    private final Neo4jCommitterConfig config;

    private Driver neo4jDriver;

    public Neo4jClient(Neo4jCommitterConfig config) {
        this.config = Objects.requireNonNull(
                config, "'config' must not be null.");
        this.neo4jDriver = createNeo4jDriver();
    }

    private Driver createNeo4jDriver() {
        Driver driver;
        Credentials creds = config.getCredentials();
        if (creds.isSet()) {
            driver = GraphDatabase.driver(config.getUri(), AuthTokens.basic(
                    creds.getUsername(),
                    EncryptionUtil.decrypt(
                            creds.getPassword(),
                            creds.getPasswordKey())));
        } else {
            driver = GraphDatabase.driver(config.getUri());
        }
        LOG.info("Neo4j Driver loaded.");
        return driver;
    }

    public void post(Iterator<ICommitterRequest> it) throws CommitterException {
        while (it.hasNext()) {
            try {
                ICommitterRequest req = it.next();
                if (req instanceof UpsertRequest) {
                    postUpsert((UpsertRequest) req);
                } else if (req instanceof DeleteRequest) {
                    postDelete((DeleteRequest) req);
                } else {
                    throw new CommitterException("Unsupported request:" + req);
                }
            } catch (IOException e) {
                throw new CommitterException(
                        "Cannot perform commit request.", e);
            }
        }
    }

    public void close() {
        if (neo4jDriver != null) {
            neo4jDriver.close();
        }
        LOG.info("Neo4j driver closed.");
    }

    private void postUpsert(UpsertRequest req) throws IOException {
        Properties meta = req.getMetadata();
        if (StringUtils.isNotBlank(config.getNodeIdProperty())) {
            meta.set(config.getNodeIdProperty(), req.getReference());
        }
        if (StringUtils.isNotBlank(config.getNodeContentProperty())) {
            meta.set(config.getNodeContentProperty(), IOUtils.toString(
                    req.getContent(), StandardCharsets.UTF_8));
        }
        try (Session session = neo4jDriver.session()) {
            session.writeTransaction(tx -> {
                tx.run(config.getUpsertCypher(), toObjectMap(meta));
                return null;
            });
        }
    }

    private void postDelete(DeleteRequest req) {
        Properties meta = req.getMetadata();
        Optional.ofNullable(trimToNull(config.getNodeIdProperty())).ifPresent(
                fld -> meta.set(fld, req.getReference()));
        try (Session session = neo4jDriver.session()) {
            session.writeTransaction(tx -> {
                tx.run(config.getDeleteCypher(), toObjectMap(meta));
                return null;
            });
        }
    }

    private Map<String, Object> toObjectMap(Properties meta) {
        Map<String, Object> map = new HashMap<>();
        meta.forEach((k, v) -> {
            if (StringUtils.isNotBlank(config.getMultiValuesJoiner())) {
                map.put(k, StringUtils.join(v, config.getMultiValuesJoiner()));
            } else {
                map.put(k, v);
            }
        });

        // Add optional parameters
        config.getOptionalParameters().forEach(param -> {
            map.computeIfAbsent(param, p -> NullValue.NULL);
        });
        return map;
    }
}