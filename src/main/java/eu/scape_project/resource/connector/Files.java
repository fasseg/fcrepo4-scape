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

import java.io.FileNotFoundException;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.fcrepo.http.commons.session.InjectedSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.scape_project.model.File;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.Representation;
import eu.scape_project.service.ConnectorService;
import eu.scape_project.util.ContentTypeInputStream;
import eu.scape_project.util.ScapeMarshaller;

/**
 * JAX-RS Resource for Files
 * 
 * @author frank asseg
 * 
 */

@Component
@Scope("prototype")
@Path("/scape/file")
public class Files {

    private final ScapeMarshaller marshaller;

    @Autowired
    private ConnectorService connectorService;

    @InjectedSession
    private Session session;

    public Files() throws JAXBException {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    /**
     * Exposes an HTTP GET end point witch returns the current version binary
     * content of a {@link File} or if references are used for files a HTTP
     * redirect a from the Connector API implementation
     * 
     * @param entityId
     *            the {@link IntellectualEntity}'s id
     * @param repId
     *            the {@link Representation}'s id
     * @param fileId
     *            the {@link File}'s id
     * @return A {@link Response} with the binary content or a HTTP redirect
     * @throws RepositoryException
     *             if an error occurred while fetching the binary content
     */
    @GET
    @Path("{entity-id}/{rep-id}/{file-id}")
    public Response retrieveFile(@PathParam("entity-id")
    final String entityId, @PathParam("rep-id")
    final String repId, @PathParam("file-id")
    final String fileId) throws RepositoryException {
        if (connectorService.isReferencedContent()) {
            IntellectualEntity e = connectorService.fetchEntity(session, entityId);
            for (Representation r : e.getRepresentations()) {
                if (r.getIdentifier().getValue().equals(repId)) {
                    for (File f : r.getFiles()) {
                        if (f.getIdentifier().getValue().equals(fileId)) {
                            return Response.temporaryRedirect(f.getUri()).build();
                        }
                    }
                }
            }
            throw new RepositoryException(new FileNotFoundException());
        } else {
            final ContentTypeInputStream src = connectorService.fetchBinaryFile(this.session, entityId, repId, fileId, null);
            return Response.ok().entity(src).type(src.getContentType()).build();
        }
    }

    /**
     * Exposes an HTTP GET end point witch returns a version of the binary
     * content of a {@link File} or if references are used for files a HTTP
     * redirect a from the Connector API implementation
     * 
     * @param entityId
     *            the {@link IntellectualEntity}'s id
     * @param repId
     *            the {@link Representation}'s id
     * @param fileId
     *            the {@link File}'s id
     * @param versionId
     *            the version's id
     * @return A {@link Response} with the binary content or a HTTP redirect
     * @throws RepositoryException
     *             if an error occurred while fetching the binary content
     */
    @GET
    @Path("{entity-id}/{rep-id}/{file-id}/{version-id}")
    public Response retrieveFile(@PathParam("entity-id")
    final String entityId, @PathParam("rep-id")
    final String repId, @PathParam("file-id")
    final String fileId, @PathParam("version-id")
    final String versionId) throws RepositoryException {
        if (connectorService.isReferencedContent()) {
            IntellectualEntity e = connectorService.fetchEntity(session, entityId);
            for (Representation r : e.getRepresentations()) {
                if (r.getIdentifier().getValue().equals(repId)) {
                    for (File f : r.getFiles()) {
                        if (f.getIdentifier().getValue().equals(fileId)) {
                            return Response.temporaryRedirect(f.getUri()).build();
                        }
                    }
                }
            }
            throw new RepositoryException(new FileNotFoundException());
        } else {
            final ContentTypeInputStream src = connectorService.fetchBinaryFile(this.session, entityId, repId, fileId, versionId);
            return Response.ok().entity(src).type(src.getContentType()).build();
        }
    }
}
