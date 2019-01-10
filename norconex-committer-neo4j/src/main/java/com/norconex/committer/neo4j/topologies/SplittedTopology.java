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
package com.norconex.committer.neo4j.topologies;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.neo4j.GraphEntry;
import com.norconex.committer.neo4j.Neo4jCommitter;

/**
 * Splitted topology.
 *
 * @author Sylvain Roussy
 */
public class SplittedTopology extends NodeTopology {

    private static final Logger LOGGER = LogManager
            .getLogger(SplittedTopology.class);

    public SplittedTopology(Neo4jCommitter committer) {
        super(committer);
    }

    @Override
    protected String buildStoreQuery(GraphEntry entry) {
        String labelPrimary = getCommitter().getPrimaryLabel();
        String labelOthers = buildAdditionalLabelsQueryPart("SET ", entry);
        String fieldReference = getCommitter().getTargetReferenceField();
        String fieldContent = getCommitter().getTargetContentField();

        String query = StringUtils.join(
            "MERGE (",
                "a:`", labelPrimary, "`{",
                    fieldReference, ":$", NEO4J_PARAM_SOURCE,
                "}",
            ") ",
            labelOthers,
            "MERGE (a)-[:WITH_CONTENT]->(",
                "content:`", labelPrimary, "_content`{",
                    "source:$", NEO4J_PARAM_SOURCE,
                "}",
            ") ",
            "SET content.", fieldContent, "=$", NEO4J_PARAM_CONTENT, " ",
            "MERGE (a)-[:WITH_META]->(",
                "metadata:`", labelPrimary, "_metadata` {",
                    "source:$", NEO4J_PARAM_SOURCE,
                "}",
            ") ",
            "SET metadata+=$", NEO4J_PARAM_METADATA, " ",
            buildParentQueryPart()
        );

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("buildStoreQuery() " + query);
        }
        return query;
    }

    @Override
    protected String buildDeleteQuery() {
        String labelPrimary = getCommitter().getPrimaryLabel();
        String fieldReference = getCommitter().getTargetReferenceField();

        String query = StringUtils.join(
                "MATCH (a:`", labelPrimary, "`{",
                    fieldReference, ":$", NEO4J_PARAM_SOURCE,
                "}) ",
                "MATCH (a)-[:WITH_CONTENT]->(content) ",
                "MATCH (a)-[:WITH_META]->(metadata) ",
                "DETACH DELETE a,content,metadata"
        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("buildDeleteQuery() " + query);
        }
        return query;
    }
}
