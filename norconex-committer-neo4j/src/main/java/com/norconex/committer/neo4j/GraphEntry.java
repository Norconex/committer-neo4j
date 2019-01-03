/* Copyright 2019 Norconex Inc.
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

import java.io.InputStream;

import com.norconex.commons.lang.map.Properties;

/**
 * GraphEntry represents an immutable entry needs to be stored in Neo4j.
 *
 * @author Sylvain Roussy
 */
public class GraphEntry {

    private final String id;
    private final Properties metaData;
    private final InputStream content;

    public GraphEntry(String id, Properties metaData, InputStream content) {
        super();
        this.id = id;
        this.metaData = metaData;
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public Properties getMetaData() {
        return metaData;
    }

    public InputStream getContent() {
        return content;
    }
}
