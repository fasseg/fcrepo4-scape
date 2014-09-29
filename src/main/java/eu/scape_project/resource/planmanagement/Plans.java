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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.fcrepo.kernel.Datastream;
import org.fcrepo.kernel.FedoraObject;
import org.fcrepo.kernel.exception.InvalidChecksumException;
import org.fcrepo.kernel.impl.rdf.impl.DefaultIdentifierTranslator;
import org.fcrepo.kernel.rdf.GraphProperties;
import org.fcrepo.kernel.rdf.IdentifierTranslator;
import org.fcrepo.kernel.services.DatastreamService;
import org.fcrepo.kernel.services.NodeService;
import org.fcrepo.kernel.services.ObjectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;

import eu.scape_project.model.plan.PlanData;
import eu.scape_project.model.plan.PlanExecutionState;
import eu.scape_project.model.plan.PlanLifecycleState;

/**
 * JAX-RS Resource for Plans
 * 
 * @author frank asseg
 * 
 */
@Scope("request")
@Path("/scape/plan")
public class Plans {

    static final String PLAN_FOLDER = "objects/scape/plans/";

    @Inject
    private Session session;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private ObjectService objectService;

    @Autowired
    private DatastreamService datastreamService;

    /**
     * Deploy a new Plan in Fedora
     * 
     * @param planId
     *            the id of the plan to deploy
     * @param uriInfo
     *            the {@link javax.ws.rs.core.UriInfo} injected by JAX-RS to have the context
     *            path available
     * @param src
     *            the plan's XML representation
     * @return a {@link javax.ws.rs.core.Response} which maps to a corresponding HTTP response
     * @throws javax.jcr.RepositoryException
     *             if an error occurred while adding the plan to Fedora
     * @throws java.io.IOException
     *             if an error occurred while reading the plan data
     * @throws org.fcrepo.kernel.exception.InvalidChecksumException
     *             if an invalid checksum check occurred
     */
    @PUT
    @Path("{id}")
    public Response deployPlan(@PathParam("id")
    final String planId, @Context
    UriInfo uriInfo, final InputStream src) throws RepositoryException, IOException, InvalidChecksumException {

        /* create a top level object for the plan */
        final String path = PLAN_FOLDER + planId;
        final FedoraObject plan = objectService.findOrCreateObject(this.session, path);
        plan.getNode().addMixin("scape:plan");

        /*
         * we have to read some plan data first so we pull the whole plan into
         * memory
         */
        final ByteArrayOutputStream sink = new ByteArrayOutputStream();
        IOUtils.copy(src, sink);
        final PlanData planData = createDeploymentPlanData(new ByteArrayInputStream(sink.toByteArray()));

        /* add the properties to the RDF graph of the exec state object */
        StringBuilder sparql = new StringBuilder();

        final IdentifierTranslator subjects = new DefaultIdentifierTranslator();
        final String planUri = subjects.getSubject(plan.getNode().getPath()).getURI();

        /* add the exec state to the parent */
        sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasType> \"PLAN\"} WHERE {};");
        if (planData.getTitle() != null) {
            sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasTitle> \"" + planData.getTitle() + "\"} WHERE {};");
        }
        if (planData.getIdentifier() != null) {
            sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasIdentifier> \"" + planData.getIdentifier().getType() + ":"
                    + planData.getIdentifier().getValue() + "\"} WHERE {};");
        }
        if (planData.getDescription() != null) {
            sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasDescription> \"" + planData.getDescription() + "\"} WHERE {};");
        }
        if (planData.getLifecycleState() != null) {
            sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasLifecycleState> \"" + planData.getLifecycleState().getState() + ":"
                    + planData.getLifecycleState().getDetails() + "\"} WHERE {};");
        } else {
            sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasLifecycleState> \"ENABLED:Initial creation\"} WHERE {};");
        }
        if (planData.getExecutionStates() != null) {
            for (PlanExecutionState state : planData.getExecutionStates()) {
                sparql.append("INSERT {<" + planUri + "> <http://scapeproject.eu/model#hasPlanExecutionState> \"" + state.getState() + ":"
                        + state.getTimeStamp() + "\"} WHERE {};");
            }
        }

        /* execute the sparql update */
        final Dataset update = plan.updatePropertiesDataset(subjects, sparql.toString());

        final Model problems = update.getNamedModel(GraphProperties.PROBLEMS_MODEL_NAME);

        // TODO: check the problems and throw an error if applicable

        /* add a datastream holding the plato XML data */
        final Datastream ds = datastreamService.findOrCreateDatastream(this.session, path + "/plato-xml");
        ds.getBinary().setContent(new ByteArrayInputStream(sink.toByteArray()), "text/xml", null, null, datastreamService.getStoragePolicyDecisionPoint());

        /* and persist the changes in fcrepo */
        this.session.save();
        return Response.created(uriInfo.getAbsolutePath()).entity(uriInfo.getAbsolutePath().toASCIIString()).header("Content-Type", "text/plain").build();
    }

    private PlanData createDeploymentPlanData(InputStream src) throws IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        PlanData.Builder data = new PlanData.Builder();
        try {
            builder = factory.newDocumentBuilder();
            Document doc = builder.parse(src);
            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPath xpath = xPathfactory.newXPath();
            data.title(xpath.compile("/plans/plan/properties/@name").evaluate(doc));
            data.description(xpath.compile("/plans/plan/properties/description").evaluate(doc));
            data.lifecycleState(new PlanLifecycleState(eu.scape_project.model.plan.PlanLifecycleState.PlanState.ENABLED, "Initial deployment"));
            return data.build();
        } catch (ParserConfigurationException | SAXException | XPathExpressionException e) {
            throw new IOException(e);
        }
    }

    /**
     * Retrieve a plan's XML representation stored in Fedora
     *
     * @param planId
     *            the id of the plan
     * @return a {@link javax.ws.rs.core.Response} which maps to a corresponding HTTP response,
     *         containing the plan as an XML document
     * @throws javax.jcr.RepositoryException
     *             if an error occurred while fetching the plan from Fedora
     */
    @GET
    @Path("{id}")
    public Response retrievePlan(@PathParam("id")
    final String planId) throws RepositoryException {
        /* fetch the plan form the repository */
        final Datastream ds = this.datastreamService.findOrCreateDatastream(this.session, PLAN_FOLDER + planId + "/plato-xml");
        return Response.ok(ds.getBinary().getContent(), ds.getBinary().getMimeType()).build();
    }

    /**
     * Delete a plan from Fedora
     * @param planId the plan's id
     * @return a {@link javax.ws.rs.core.Response} which maps to a corresponding HTTP response
     * @throws javax.jcr.RepositoryException if an error occurred while deleting the plan
     */
    @DELETE
    @Path("{id}")
    public Response deletePlan(@PathParam("id")
    final String planId) throws RepositoryException {
        final String path = "/" + PLAN_FOLDER + planId;
        this.nodeService.getObject(this.session, path).delete();
        this.session.save();
        return Response.ok().build();
    }
}
