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

import java.io.InputStream;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.service.ConnectorService;
import eu.scape_project.util.ScapeMarshaller;

/**
 * JAX-RS Resource for Intellectual Entities This implementation exposes the
 * /scape/entity-async endpoint as specified in the Connector API Documentation
 * 
 * @author frank asseg
 * 
 */
@Scope("request")
@Path("/scape/entity-async")
public class AsyncIntellectualEntities {

    private final ScapeMarshaller marshaller;

    @Autowired
    private ConnectorService connectorService;

    @Inject
    private Session session;

    /**
     * Creates and initializes a new {@link AsyncIntellectualEntities}
     *
     * @throws javax.xml.bind.JAXBException
     */
    public AsyncIntellectualEntities() throws JAXBException {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    /**
     * Exposes the HTTP POST endpoint to ingest an entity asynchronously
     * 
     * @param src
     *            The {@link IntellectualEntity}'s METS representation
     * @return A {@link Response} that maps to a corresponding HTTP response
     *         code
     * @throws RepositoryException
     *             If there was an issue queuing this {@link IntellectualEntity}
     *             for an asynchronous ingest
     */
    @POST
    @Produces(MediaType.TEXT_PLAIN)
    public Response ingestEntity(final InputStream src) throws RepositoryException {
        String id = connectorService.queueEntityForIngest(this.session, src);
        return Response.ok(id).build();
    }

}
