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
package eu.scape_project.resource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBException;

import org.fcrepo.http.commons.session.InjectedSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.scape_project.model.File;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.Representation;
import eu.scape_project.service.ConnectorService;
import eu.scape_project.util.ScapeMarshaller;

/**
 * JAX-RS Resource for SRU search
 * 
 * @author frank asseg
 * 
 */

@Component
@Scope("prototype")
@Path("/scape/sru")
public class SRUSearch {

    @InjectedSession
    private Session session;

    @Autowired
    private ConnectorService connectorService;

    private final ScapeMarshaller marshaller;

    public SRUSearch() throws JAXBException {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    /**
     * Exposes a HTTP end point modeled after the SRU specifications to search
     * {@link IntellectualEntity}s
     * 
     * @param operation
     *            the operation to perform. Currently only
     *            <code>searchAndRetrieve</code> are supported
     * @param query
     *            the query of the operation. Currently only a
     *            <code>terms</code> query is supported
     * @param version
     *            the version of the SRU reques. Currently only <code>1</code>
     *            is supported
     * @param offset
     *            the offset of the search
     * @param limit
     *            the maximum number of results
     * @return a {@link Response} which maps to a corresponding HTTP response,
     *         containing the SRU's results as an XML document
     * @throws RepositoryException
     */
    @GET
    @Path("/entities")
    public Response searchIntellectualEntities(@QueryParam("operation")
    final String operation, @QueryParam("query")
    final String query, @QueryParam("version")
    final String version, @QueryParam("startRecord")
    @DefaultValue("0")
    final int offset, @QueryParam("maximumRecords")
    @DefaultValue("25")
    final int limit) throws RepositoryException {

        final List<String> uris = this.connectorService.searchEntities(this.session, query, offset, limit);
        return Response.ok(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                writeSRUHeader(output, uris.size());
                for (String uri : uris) {
                    try {
                        final IntellectualEntity ie = SRUSearch.this.connectorService.fetchEntity(session, uri.substring(uri.lastIndexOf('/') + 1));
                        writeSRURecord(ie, output);
                    } catch (RepositoryException e) {
                        throw new IOException(e);
                    }
                }
                writeSRUFooter(output);
            }
        }).build();
    }

    /**
     * Exposes a HTTP end point modeled after the SRU specifications to search
     * {@link Representation}s
     * 
     * @param operation
     *            the operation to perform. Currently only
     *            <code>searchAndRetrieve</code> are supported
     * @param query
     *            the query of the operation. Currently only a
     *            <code>terms</code> query is supported
     * @param version
     *            the version of the SRU reques. Currently only <code>1</code>
     *            is supported
     * @param offset
     *            the offset of the search
     * @param limit
     *            the maximum number of results
     * @return a {@link Response} which maps to a corresponding HTTP response,
     *         containing the SRU's results as an XML document
     * @throws RepositoryException
     */
    @GET
    @Path("/representations")
    public Response searchRepresentations(@QueryParam("operation")
    final String operation, @QueryParam("query")
    final String query, @QueryParam("version")
    final String version, @QueryParam("startRecord")
    final int offset, @QueryParam("maximumRecords")
    @DefaultValue("25")
    final int limit) throws RepositoryException {

        final List<String> uris = this.connectorService.searchRepresentations(this.session, query, offset, limit);
        return Response.ok(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                writeSRUHeader(output, uris.size());
                for (String uri : uris) {
                    try {
                        final Representation rep = SRUSearch.this.connectorService.fetchRepresentation(session,
                                uri.substring(uri.indexOf(ConnectorService.ENTITY_FOLDER)));
                        writeSRURecord(rep, output);
                    } catch (RepositoryException e) {
                        throw new IOException(e);
                    }
                }
                writeSRUFooter(output);
            }
        }).build();
    }

    /**
     * Exposes a HTTP end point modeled after the SRU specifications to search
     * {@link File}s
     * 
     * @param operation
     *            the operation to perform. Currently only
     *            <code>searchAndRetrieve</code> are supported
     * @param query
     *            the query of the operation. Currently only a
     *            <code>terms</code> query is supported
     * @param version
     *            the version of the SRU reques. Currently only <code>1</code>
     *            is supported
     * @param offset
     *            the offset of the search
     * @param limit
     *            the maximum number of results
     * @return a {@link Response} which maps to a corresponding HTTP response,
     *         containing the SRU's results as an XML document
     * @throws RepositoryException
     */

    @GET
    @Path("/files")
    public Response searchFiles(@QueryParam("operation")
    final String operation, @QueryParam("query")
    final String query, @QueryParam("version")
    final String version, @QueryParam("startRecord")
    final int offset, @QueryParam("maximumRecords")
    @DefaultValue("25")
    final int limit) throws RepositoryException {

        final List<String> uris = this.connectorService.searchFiles(this.session, query, offset, limit);
        return Response.ok(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                writeSRUHeader(output, uris.size());
                for (String uri : uris) {
                    try {
                        final File f = SRUSearch.this.connectorService.fetchFile(session, uri.substring(uri.indexOf(ConnectorService.ENTITY_FOLDER)));
                        writeSRURecord(f, output);
                    } catch (RepositoryException e) {
                        throw new IOException(e);
                    }
                }
                writeSRUFooter(output);
            }
        }).build();
    }

    private void writeSRURecord(Object o, OutputStream output) throws IOException {
        final StringBuilder sru = new StringBuilder();
        sru.append("<srw:record>");
        sru.append("<srw:recordSchema>http://scapeproject.eu/schema/plato</srw:recordSchema>");
        sru.append("<srw:recordData>");
        output.write(sru.toString().getBytes());
        try {
            this.marshaller.serialize(o, output);
        } catch (JAXBException e) {
            throw new IOException(e);
        }
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

}
