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
 * This topology makes one node per entry (content + metadata)
 *
 * @author Sylvain Roussy
 */
public class OneNodeTopology extends NodeTopology {

    private static final Logger LOGGER =
            LogManager.getLogger(OneNodeTopology.class);

    public OneNodeTopology(Neo4jCommitter committer) {
        super(committer);
    }

    @Override
    protected String buildStoreQuery(final GraphEntry entry) {
        String labelPrimary = getCommitter().getPrimaryLabel();
        String labelOthers = buildAdditionalLabelsQueryPart(", ", entry);
        String fieldReference = getCommitter().getTargetReferenceField();
        String fieldContent = getCommitter().getTargetContentField();

        String query = StringUtils.join(
            "MERGE (a:`", labelPrimary, "`{",
                fieldReference, ":$", NEO4J_PARAM_SOURCE,
            "}) ",
            "SET a+=$", NEO4J_PARAM_METADATA, ", ",
                "a.", fieldContent, "=$", NEO4J_PARAM_CONTENT,
            labelOthers,
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
            "DETACH DELETE a"
        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("buildDeleteQuery() " + query);
        }
        return query;
    }
}
