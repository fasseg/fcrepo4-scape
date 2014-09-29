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

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBException;

import org.fcrepo.kernel.FedoraObject;
import org.fcrepo.kernel.impl.rdf.SerializationUtils;
import org.fcrepo.kernel.impl.rdf.impl.DefaultIdentifierTranslator;
import org.fcrepo.kernel.rdf.GraphProperties;
import org.fcrepo.kernel.rdf.IdentifierTranslator;
import org.fcrepo.kernel.services.ObjectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * JAX-RS Resource for Plan life cycle states
 * 
 * @author frank asseg
 * 
 */
@Component
@Scope("prototype")
@Path("/scape/plan-state")
public class PlanLifecycleStates {

    @Inject
    private Session session;

    @Autowired
    private ObjectService objectService;

    /**
     * Retrieve the life cycle state for a plan stored in Fedora
     * 
     * @param planId
     *            the id of the plan
     * @param uriInfo
     *            the {@link javax.ws.rs.core.UriInfo} injected by JAX-RS for having the context
     *            path available
     * @return the plan's current life cycle state
     * @throws javax.jcr.RepositoryException
     *             if an error occurred while fetching the life cycle tate of
     *             the plan
     */
    @GET
    @Path("{id}")
    public Response retrievePlanLifecycleState(@PathParam("id")
    final String planId, @Context
    UriInfo uriInfo) throws RepositoryException {
        /* fetch the plan RDF from fedora */
        final String planUri = "/" + Plans.PLAN_FOLDER + planId;
        final FedoraObject plan = this.objectService.findOrCreateObject(this.session, planUri);

        /* get the relevant information from the RDF dataset */
        final IdentifierTranslator subjects = new DefaultIdentifierTranslator();        final Dataset data = plan.getPropertiesDataset(subjects);
        final Model rdfModel = SerializationUtils.unifyDatasetModel(data);

        final String lifecycle = rdfModel
                .listStatements(subjects.getSubject(plan.getNode().getPath()), rdfModel.getProperty("http://scapeproject.eu/model#hasLifecycleState"),
                        (RDFNode) null).next().getObject().asLiteral().getString();
        return Response.ok(lifecycle, MediaType.TEXT_PLAIN).build();
    }

    /**
     * Update the life cycle state of a plan in Fedora
     * @param planId the id of the plan to update
     * @param state the new life cycle state
     * @return a {@link javax.ws.rs.core.Response} which maps to a corresponding HTTP response
     * @throws javax.jcr.RepositoryException if an error occurred while storing the life cycle state
     * @throws javax.xml.bind.JAXBException if en error occurred while unmarshalling the life cycle state
     */
    @PUT
    @Path("{id}/{state}")
    public Response updateLifecycleState(@PathParam("id")
    final String planId, @PathParam("state")
    String state) throws RepositoryException, JAXBException{
        /* fetch the plan RDF from fedora */
        final String planPath = "/" + Plans.PLAN_FOLDER + planId;
        final FedoraObject plan = this.objectService.findOrCreateObject(this.session, planPath);

        if (!state.startsWith("ENABLED:") && !state.equals("ENABLED") && !state.startsWith("DISABLED:") && !state.equals("DISABLED")) {
            throw new RepositoryException("Illegal state: '" + state + "' only one of [ENABLED:<details>,DISABLED:<details>] is allowed");
        }

        /* delete the existing lifecyclestate and add the new one */
        StringBuilder sparql = new StringBuilder();
        final IdentifierTranslator subjects = new DefaultIdentifierTranslator();
        final String planUri = subjects.getSubject(plan.getNode().getPath()).getURI();

        sparql.append("DELETE {<" + planUri + "> <http://scapeproject.eu/model#hasLifecycleState> ?o} WHERE {<" + planUri
                + "> <http://scapeproject.eu/model#hasLifecycleState> ?o} ;");
        sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasLifecycleState> \"" + state + "\"} WHERE {};");
        final Model errors = plan.updatePropertiesDataset(subjects, sparql.toString()).getNamedModel(GraphProperties.PROBLEMS_MODEL_NAME);
        // TODO: check for errors

        this.session.save();
        return Response.ok().build();
    }
}
