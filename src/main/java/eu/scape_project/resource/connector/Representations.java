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

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.Representation;
import eu.scape_project.service.ConnectorService;
import eu.scape_project.util.ScapeMarshaller;

/**
 * JAX-RS Resource for Representations
 * 
 * @author frank asseg
 * 
 */

@Scope("request")
@Path("/scape/representation")
public class Representations {

    private final ScapeMarshaller marshaller;

    @Autowired
    private ConnectorService connectorService;

    @Inject
    private Session session;

    public Representations() throws JAXBException {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    /**
     * Exposes a HTTP end point for retrieving the current version of a
     * {@link Representation}
     * 
     * @param entityId
     *            the id of the {@link IntellectualEntity}
     * @param repId
     *            the id of the {@link Representation}
     * @return a {@link Response} which maps to a corresponding HTTP response,
     *         containing the {@link Representation} serialized into a XML
     *         document
     * @throws RepositoryException
     *             if an error occurred while retrieving a Representation
     */
    @GET
    @Path("{entity-id}/{rep-id}")
    public Response retrieveRepresentation(@PathParam("entity-id")
    final String entityId, @PathParam("rep-id")
    final String repId) throws RepositoryException {
        final Representation r = connectorService.fetchRepresentation(this.session, entityId, repId, null);
        return Response.ok().entity(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    Representations.this.marshaller.serialize(r, output);
                } catch (JAXBException e) {
                    throw new IOException(e);
                }
            }
        }).build();
    }

    /**
     * Exposes a HTTP end point for retrieving a specific version of a
     * {@link Representation}
     * 
     * @param entityId
     *            the id of the {@link IntellectualEntity}
     * @param repId
     *            the id of the {@link Representation}
     * @param versionId
     *            the id of the {@link Representation}'s version
     * @return a {@link Response} which maps to a corresponding HTTP response,
     *         containing the {@link Representation} serialized into a XML
     *         document
     * @throws RepositoryException
     *             if an error occurred while retrieving a Representation
     */
    @GET
    @Path("{entity-id}/{rep-id}/{version-id}")
    public Response retrieveRepresentation(@PathParam("entity-id")
    final String entityId, @PathParam("rep-id")
    final String repId, @PathParam("version-id")
    final int versionId) throws RepositoryException {
        final Representation r = connectorService.fetchRepresentation(this.session, entityId, repId, versionId);
        return Response.ok().entity(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    Representations.this.marshaller.serialize(r, output);
                } catch (JAXBException e) {
                    throw new IOException(e);
                }
            }
        }).build();
    }

    /**
     * Exposes a HTTP end point for updating {@link Representation}s
     * @param entityId the {@link IntellectualEntity}'s id
     * @param representationId the {@link Representation}'s id
     * @param src the updated {@link Representation} as a XML document
     * @return a {@link Response} which maps to a corresponding HTTP response
     * @throws RepositoryException if en error occurred wihle updating the representation
     */
    @PUT
    @Path("{entity-id}/{rep-id}")
    public Response updateRepresentation(@PathParam("entity-id")
    final String entityId, @PathParam("rep-id")
    final String representationId, final InputStream src) throws RepositoryException {
        this.connectorService.updateRepresentation(session, entityId, representationId, src);
        return Response.ok().build();
    }
}
