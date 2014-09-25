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
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBException;

import org.fcrepo.http.commons.session.InjectedSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.scape_project.service.ConnectorService;
import eu.scape_project.util.ScapeMarshaller;

/**
 * JAX-RS Resource for metadata
 *
 * @author frank asseg
 *
 */

@Component
@Scope("prototype")
@Path("/scape/metadata")
public class Metadata {

    private final ScapeMarshaller marshaller;

    @Autowired
    private ConnectorService connectorService;

    @InjectedSession
    private Session session;

    public Metadata()
            throws JAXBException {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    @GET
    @Path("{path: .*}")
    /**
     * Expose a HTTP end point for retrieving the metadata of an IntellectualEntity, Representation, File or BitStream
     * @param path the path describing the location of the metadata in the repository
     * @return a {@link Response} which maps to a corresponding HTTP response, containing the requested metadata set
     * @throws RepositoryException if an error occurred while retrieving the metadata
     */
    public Response retrieveMetadata(@PathParam("path")
    String path) throws RepositoryException {
        path = ConnectorService.ENTITY_FOLDER + "/" + path;
        final Object md = connectorService.fetchCurrentMetadata(this.session, path);
        return Response.ok().entity(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException,
                    WebApplicationException {
                try {
                    Metadata.this.marshaller.serialize(md, output);
                } catch (JAXBException e) {
                    throw new IOException(e);
                }
            }
        }).build();
    }

    /**
     * Exposes a HTTP end point for updating a specific metadata set
     * @param path the path describing the location of the metadata in the repository
     * @param src the updated metadata XML representation
     * @return a {@link Response} which maps to a corresponding HTTP response
     * @throws RepositoryException if an error occurred while updating the metadata
     */
    @PUT
    @Path("{path: .*}")
    @Consumes({MediaType.TEXT_XML})
    public Response updateMetadata(@PathParam("path") String path, final InputStream src) throws RepositoryException {
        this.connectorService.updateMetadata(this.session, path, src);
        return Response.ok().build();
    }
}
