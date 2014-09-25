/*
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package eu.scape_project.rdf;

/**
 * This Vocabulary is used to match the scape-platform-datamodel to Fedora's JCR
 * Properties
 * 
 * @author frank asseg
 * 
 */
public final class  ScapeRDFVocabulary {
    private ScapeRDFVocabulary() {
        super();
    }
    public static final String SCAPE_NAMESPACE = "http://scapeproject.eu/model#";

    public static final String SCAPE_PREFIX = "scape";

    public static final String HAS_REPRESENTATION = "hasRepresentation";

    public static final String HAS_TYPE = "hasType";

    public static final String HAS_SCHEMA = "hasSchema";

    public static final String HAS_BITSTREAM_TYPE = "hasBitstreamType";

    public static final String HAS_BITSTREAM = "hasBitStream";

    public static final String HAS_FILENAME = "hasFileName";

    public static final String HAS_MIMETYPE = "hasMimeType";

    public static final String HAS_INGEST_SOURCE = "hasIngestSource";

    public static final String HAS_TITLE = "hasTitle";

    public static final String HAS_LIFECYCLESTATE = "hasLifeCycleState";

    public static final String HAS_LIFECYCLESTATE_DETAILS = "hasLifeCycleStateDetails";

    public static final String HAS_CURRENT_VERSION = "currentVersion";

    public static final String HAS_VERSION = "hasVersion";

    public static final String HAS_FILE = "hasFile";

    public static final String HAS_ITEM = "hasItem";

    public static final String HAS_REFERENCED_CONTENT = "hasReferencedContent";

    public static final String HAS_INGEST_STATE = "hasIngestState";

    public static final String prefix(String property) {
        return SCAPE_PREFIX + ":" + property;
    }

    public static final String namespace(String property) {
        return SCAPE_NAMESPACE + property;
    }
}
