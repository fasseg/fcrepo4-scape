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

package eu.scape_project.resource.planmanagement;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import eu.scape_project.util.XmlDeclarationStrippingInputstream;
import org.apache.commons.io.IOUtils;
import org.fcrepo.kernel.Datastream;
import org.fcrepo.kernel.RdfLexicon;
import org.fcrepo.kernel.impl.rdf.impl.DefaultIdentifierTranslator;
import org.fcrepo.kernel.rdf.IdentifierTranslator;
import org.fcrepo.kernel.services.DatastreamService;
import org.fcrepo.kernel.services.NodeService;
import org.fcrepo.kernel.services.RepositoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * JAX-RS Resource for Plan search
 * 
 * @author frank asseg
 * 
 */
@Scope("request")
@Path("/scape/plan/sru")
public class PlanSearch {

    @Inject
    private Session session;

    @Autowired
    private DatastreamService datastreamService;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private RepositoryService repositoryService;


    /**
     * Search for plans in Fedora
     * 
     * @param operation
     *            the operation for the SRU request. Currently only
     *            <code>searchAndRetrieve</code> is supported
     * @param query
     *            the query for the SRU requets. Currently only a term query is
     *            supported
     * @param version
     *            the version of the SRU request. Currently only version
     *            <code>1</code> is supported
     * @param offset
     *            the offet of the results
     * @param limit
     *            the maximum amount of results
     * @return a {@link javax.ws.rs.core.Response} which maps to a corresponding HTTP response,
     *         containing a SRU searchAndRetreive XML result document
     * @throws javax.jcr.RepositoryException
     *             if an error occurred while searching for plans in Fedora
     */
    @GET
    @Produces(MediaType.TEXT_XML)
    public Response searchPlans(@QueryParam("operation")
    final String operation, @QueryParam("query")
    final String query, @QueryParam("version")
    final String version, @QueryParam("startRecord")
    final int offset, @QueryParam("maximumRecords")
    @DefaultValue("25")
    final int limit) throws RepositoryException {

        final IdentifierTranslator subjects = new DefaultIdentifierTranslator(); 
        final Model model = this.repositoryService.searchRepository(subjects, ResourceFactory.createResource("info:fedora/objects/scape/plans"),
                this.session, query, limit, 0)
                .getDefaultModel();
        final StmtIterator it = model.listStatements(null, model.createProperty("http://scapeproject.eu/model#hasType"), "PLAN");
        final List<String> uris = new ArrayList<>();
        while (it.hasNext()) {
            final String uri = it.next().getSubject().getURI();
            uris.add(uri);
        }

        /*
         * create a stream from the plan XMLs to be written to the HTTP response
         * the reponse does include the whole of the PLATO XML body so every hit
         * is written from the repo to the httpresponse
         */
        StreamingOutput entity = new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                writeSRUHeader(output, uris.size());
                for (String uri : uris) {
                    writeSRURecord(output, uri);
                }
                writeSRUFooter(output);
            }

            private void writeSRURecord(OutputStream output, String uri) throws IOException {
                final StringBuilder sru = new StringBuilder();
                final String planId = uri.substring((RdfLexicon.RESTAPI_NAMESPACE + Plans.PLAN_FOLDER).length() + 1);
                sru.append("<srw:record>");
                sru.append("<srw:recordPacking>string</srw:recordPacking>");
                sru.append("<srw:recordSchema>http://scapeproject.eu/schema/plato</srw:recordSchema>");
                sru.append("<srw:extraRecordData>");
                sru.append("<planId>").append(planId).append("</planId>");
                sru.append("</srw:extraRecordData>");
                sru.append("<srw:recordData>");
                output.write(sru.toString().getBytes());
                final Datastream plato = datastreamService.findOrCreateDatastream(session, uri.substring(uri.indexOf('/')) + "/plato-xml");
                IOUtils.copy(new XmlDeclarationStrippingInputstream(plato.getBinary().getContent()), output);
                sru.setLength(0);
                sru.append("</srw:recordData>");
                sru.append("</srw:record>");
                output.write(sru.toString().getBytes());
            }

            private void writeSRUFooter(OutputStream output) throws IOException {
                final StringBuilder sru = new StringBuilder();
                sru.append("</srw:records>");
                sru.append("</srw:searchRetrieveResponse>");
                output.write(sru.toString().getBytes());
            }

            private void writeSRUHeader(OutputStream output, int size) throws IOException {
                final StringBuilder sru = new StringBuilder();
                sru.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
                sru.append("<srw:searchRetrieveResponse xmlns:srw=\"http://scapeproject.eu/srw/\">");
                sru.append("<srw:numberOfRecords>" + size + "</srw:numberOfRecords>");
                sru.append("<srw:records>");
                output.write(sru.toString().getBytes("UTF-8"));
            }

        };
        return Response.ok(entity, MediaType.TEXT_XML).build();
    }
}
