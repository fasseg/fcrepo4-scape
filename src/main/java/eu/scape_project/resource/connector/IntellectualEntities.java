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
package eu.scape_project.resource.connector;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBException;

import org.fcrepo.http.commons.session.InjectedSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.scape_project.model.File;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.service.ConnectorService;
import eu.scape_project.util.ScapeMarshaller;

/**
 * JAX-RS Resource for Intellectual Entities
 * 
 * @author frank asseg
 * 
 */
@Component
@Scope("prototype")
@Path("/scape/entity")
public class IntellectualEntities {

    private final ScapeMarshaller marshaller;

    @Autowired
    private ConnectorService connectorService;

    @InjectedSession
    private Session session;

    public IntellectualEntities() throws JAXBException {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    /**
     * Exposes an HTTP end point to Ingest an {@link IntellectualEntity} as
     * defined in the Connector API
     * 
     * @param src
     *            An {@link java.io.InputStream} serving the {@link IntellectualEntity}
     *            's METS representation
     * @return a {@link Response} which maps to a corresponding HTTP response
     * @throws RepositoryException
     */
    @POST
    @Consumes(MediaType.TEXT_XML)
    @Produces(MediaType.TEXT_PLAIN)
    public Response ingestEntity(final InputStream src) throws RepositoryException {
        String id = connectorService.addEntity(this.session, src);
        return Response.status(Status.CREATED).entity(id).build();
    }

    /**
     * Exposes a Http end point for retrieving an {@link IntellectualEntity}'s
     * METS representation
     *
     * @param id
     *            the id of the entity
     * @param useReferences
     *            indicates if {@link File}s actual binary data gets fetched and
     *            put into the repository or if only a reference to an external
     *            URI will be maintained
     * @return a {@link Response} which maps to a corresponding HTTP response
     * @throws RepositoryException
     *             if an error occurred
     */
    @GET
    @Produces(MediaType.TEXT_XML)
    @Path("{id}")
    public Response retrieveEntity(@PathParam("id")
    final String id, @QueryParam("useReferences")
    @DefaultValue("no")
    final String useReferences) throws RepositoryException {

        final boolean refs = useReferences.equalsIgnoreCase("yes");
        final IntellectualEntity ie = connectorService.fetchEntity(this.session, id);
        /* create a streaming METS response using the ScapeMarshaller */
        return Response.ok(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    IntellectualEntities.this.marshaller.serialize(ie, output, refs);
                } catch (JAXBException e) {
                    throw new IOException(e);
                }

            }
        }).build();

    }

    /**
     * Exposes a Http end point for retrieving an distinct version of an
     * {@link IntellectualEntity}'s METS representation
     *
     * @param id
     *            the id of the entity
     * @param versionNumber
     *            the id of the version to retrieve
     * @param useReferences
     *            indicates if {@link File}s actual binary data gets fetched and
     *            put into the repository or if only a reference to an external
     *            URI will be maintained
     * @return a {@link Response} which maps to a corresponding HTTP response
     * @throws RepositoryException
     *             if an error occurred
     */
    @GET
    @Produces(MediaType.TEXT_XML)
    @Path("{id}/{versionNumber}")
    public Response retrieveEntity(@PathParam("id")
    final String id, @PathParam("versionNumber")
    final Integer versionNumber, @QueryParam("useReferences")
    @DefaultValue("no")
    final String useReferences) throws RepositoryException {

        final boolean refs = useReferences.equalsIgnoreCase("yes");
        final IntellectualEntity ie = connectorService.fetchEntity(this.session, id, versionNumber);
        /* create a streaming METS response using the ScapeMarshaller */
        return Response.ok(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    IntellectualEntities.this.marshaller.serialize(ie, output, refs);
                } catch (JAXBException e) {
                    throw new IOException(e);
                }

            }
        }).build();

    }

    /**
     * Exposes an HTTP end point to update an {@link IntellectualEntity}
     *
     * @param entityId
     *            the id of the {@link IntellectualEntity} to update
     * @param src
     *            an {@link java.io.InputStream} serving the {@link IntellectualEntity}
     *            's updated METS representation
     * @return a {@link Response} which maps to a corresponding HTTP response   
     * @throws RepositoryException
     */
    @PUT
    @Path("{id}")
    @Consumes({ MediaType.TEXT_XML })
    public Response updateEntity(@PathParam("id")
    final String entityId, final InputStream src) throws RepositoryException {
        connectorService.updateEntity(this.session, src, entityId);
        return Response.ok().build();
    }

}
