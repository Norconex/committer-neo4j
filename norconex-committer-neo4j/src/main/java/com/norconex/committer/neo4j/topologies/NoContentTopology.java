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
 * Topology with one node bringing metadata only (no content).
 * @author Sylvain Roussy
 */
public class NoContentTopology extends OneNodeTopology{

    private static final Logger LOGGER =
            LogManager.getLogger(NoContentTopology.class);

    public NoContentTopology(Neo4jCommitter committer) {
        super(committer);
    }

    @Override
    protected String buildStoreQuery(GraphEntry entry) {
        String labelPrimary = getCommitter().getPrimaryLabel();
        String labelOthers = buildAdditionalLabelsQueryPart(", ", entry);
        String fieldReference = getCommitter().getTargetReferenceField();
        entry.getMetaData().remove("content");


        String query = StringUtils.join(
            "MERGE (a:`", labelPrimary, "`{",
                fieldReference, ":$", NEO4J_PARAM_SOURCE,
            "})",
            " SET a+=$", NEO4J_PARAM_METADATA,
            labelOthers,
            buildParentQueryPart()
        );

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("buildStoreQuery() " + query);
        }
        return query;
    }
}
