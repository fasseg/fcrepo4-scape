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
import java.io.OutputStream;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.LifecycleState;
import eu.scape_project.service.ConnectorService;
import eu.scape_project.util.ScapeMarshaller;

/**
 * JAX-RS Resource for life cycle states
 * 
 * @author frank asseg
 * 
 */
@Scope("request")
@Path("/scape/lifecycle")
public class LifeCycleStates {

    private final ScapeMarshaller marshaller;

    @Autowired
    private ConnectorService connectorService;

    @Inject
    private Session session;

    public LifeCycleStates() throws JAXBException {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    /**
     * Exposes an HTTP end point to fetch the {@link LifecycleState} of an
     * {@link IntellectualEntity}
     * 
     * @param entityId
     *            the {@link IntellectualEntity}'s id
     * @return a {@link Response} which maps to a corresponding HTTP response,
     *         containing a XML representation of the {@link LifecycleState}
     * @throws RepositoryException
     *             if an error occurred while fetching the
     *             {@link LifecycleState}
     */
    @GET
    @Path("{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response retrieveLifeCycleState(@PathParam("id")
    final String entityId) throws RepositoryException {
        final LifecycleState state = connectorService.fetchLifeCycleState(this.session, entityId);
        return Response.ok(new StreamingOutput() {

            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    LifeCycleStates.this.marshaller.serialize(state, output);
                } catch (JAXBException e) {
                    throw new IOException(e);
                }
            }
        }).build();
    }

}
