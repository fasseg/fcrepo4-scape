/* 
* Copyright 2014 Frank Asseg
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License. 
*/
package integration.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.xml.bind.JAXBElement;

import eu.scape_project.model.TestUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.HttpResponse;
import org.junit.Test;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.VerbType;

public class ListIdentifiersIT extends ReportIT {
    @Test
    public void testListOAIDCIdentifiers() throws Exception {
        createFedoraObjectWithOaiRecord(RandomStringUtils.randomAlphabetic(16),RandomStringUtils.randomAlphabetic(16), null, this
                .getClass().getClassLoader().getResourceAsStream("oaidc.xml"));
        HttpResponse resp = getOAIPMHResponse(VerbType.LIST_IDENTIFIERS.value(), null, "oai_dc", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oaipmh =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oaipmh.getError().size());
        assertNotNull(oaipmh.getRequest());
        assertEquals(VerbType.LIST_IDENTIFIERS.value(), oaipmh.getRequest().getVerb().value());
        assertTrue(oaipmh.getListIdentifiers().getHeader().size() > 0);
    }

    @Test
    public void testListPremisEventIdentifiers() throws Exception {
        createEntity(TestUtil.createTestEntity(RandomStringUtils.randomAlphabetic(16)));
        HttpResponse resp = getOAIPMHResponse(VerbType.LIST_IDENTIFIERS.value(), null, "premis-event-v2", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oaipmh =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oaipmh.getError().size());
        assertNotNull(oaipmh.getRequest());
        assertEquals(VerbType.LIST_IDENTIFIERS.value(), oaipmh.getRequest().getVerb().value());
        assertTrue(oaipmh.getListIdentifiers().getHeader().size() > 0);
    }

    @Test
    public void testListPremisFullIdentifiers() throws Exception {
        createEntity(TestUtil.createTestEntity(RandomStringUtils.randomAlphabetic(16)));
        HttpResponse resp = getOAIPMHResponse(VerbType.LIST_IDENTIFIERS.value(), null, "premis-full-v2", null, null, null);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        OAIPMHtype oaipmh =
                ((JAXBElement<OAIPMHtype>) this.unmarshaller.unmarshal(resp.getEntity().getContent())).getValue();
        assertEquals(0, oaipmh.getError().size());
        assertNotNull(oaipmh.getRequest());
        assertEquals(VerbType.LIST_IDENTIFIERS.value(), oaipmh.getRequest().getVerb().value());
        assertTrue(oaipmh.getListIdentifiers().getHeader().size() > 0);
    }
}
