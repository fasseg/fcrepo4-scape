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
package integration;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.scape_project.model.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"/integration-tests/referenced-content/test-container.xml"})
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
public class ReferencedContentIntellectualEntitiesIT extends AbstractIT {

    private static final Logger LOG = LoggerFactory
            .getLogger(ReferencedContentIntellectualEntitiesIT.class);

    @Test
    public void testIngestIntellectualEntityAndCheckRedirectForBinary()
            throws Exception {
        HttpPost post = new HttpPost(SCAPE_URL + "/entity");
        post.setEntity(new InputStreamEntity(this.getClass().getClassLoader()
                .getResourceAsStream("ONB_mets_small.xml"), -1,
                ContentType.TEXT_XML));
        HttpResponse resp = this.client.execute(post);
        String id = EntityUtils.toString(resp.getEntity());
        post.releaseConnection();

        HttpGet get = new HttpGet(SCAPE_URL + "/entity/" + id);
        resp = client.execute(get);
        assertEquals(200, resp.getStatusLine().getStatusCode());
        IntellectualEntity e =
                this.marshaller.deserialize(IntellectualEntity.class, resp
                        .getEntity().getContent());
        get.releaseConnection();

        for (Representation r : e.getRepresentations()) {
            for (File f : r.getFiles()) {
                get =
                        new HttpGet(SCAPE_URL + "/file/" +
                                e.getIdentifier().getValue() + "/" +
                                r.getIdentifier().getValue() + "/" +
                                f.getIdentifier().getValue());
                this.client.getParams().setBooleanParameter(
                        "http.protocol.handle-redirects", false);
                resp = this.client.execute(get);
                assertEquals(307, resp.getStatusLine().getStatusCode());
                assertEquals(f.getUri().toASCIIString(), resp.getFirstHeader(
                        "Location").getValue());
                get.releaseConnection();
            }
        }
    }

    @Test
    public void testIngestIntellectualEntitiesAsync() throws Exception {
        List<String> entityIds = new ArrayList(Arrays.asList("ref-async-1", "ref-async-2", "ref-async-3", "ref-async-4", "ref-async-5"));
        List<String> queueId = new ArrayList<>();
        for (String id : entityIds) {
            IntellectualEntity ie = TestUtil.createTestEntityWithMultipleRepresentations(id);
            HttpPost post = new HttpPost(SCAPE_URL + "/entity-async");
            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            this.marshaller.serialize(ie, sink);
            post.setEntity(new InputStreamEntity(new ByteArrayInputStream(sink.toByteArray()), sink.size()));
            HttpResponse resp = this.client.execute(post);
            assertEquals(200, resp.getStatusLine().getStatusCode());
            post.releaseConnection();
        }

        Iterator<String> ids = entityIds.iterator();
        while (ids.hasNext()) {
            String id = ids.next();
            HttpGet get = new HttpGet(SCAPE_URL + "/lifecycle/" + id);
            HttpResponse resp = this.client.execute(get);
            assertEquals(200, resp.getStatusLine().getStatusCode());
            get.releaseConnection();
            LifecycleState state =
                    (LifecycleState) this.marshaller.deserialize(resp
                            .getEntity().getContent());
            if (state.getState().equals(LifecycleState.State.INGESTED)) {
//                System.out.println(id + " was ingested");
                ids.remove();
            } else if (state.getState().equals(LifecycleState.State.INGESTING)) {
//                System.out.println(id + " is currently ingesting");
            }else {
                throw new Exception("this should not happen!");
            }
            if (!ids.hasNext()) {
                ids = entityIds.iterator();
            }
        }
    }

    @Test
    public void testIngestIntellectualEntitiesAsyncWithSameIDs() throws Exception {
        List<String> entityIds = new ArrayList(Arrays.asList("ref-async-6", "ref-async-6"));
        List<String> queueId = new ArrayList<>();
        int count = 0;
        for (String id : entityIds) {
            IntellectualEntity ie = TestUtil.createTestEntityWithMultipleRepresentations(id);
            HttpPost post = new HttpPost(SCAPE_URL + "/entity-async");
            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            this.marshaller.serialize(ie, sink);
            post.setEntity(new InputStreamEntity(new ByteArrayInputStream(sink.toByteArray()), sink.size()));
            HttpResponse resp = this.client.execute(post);
            if (count++ == 0) {
                assertEquals(200, resp.getStatusLine().getStatusCode());
            }else {
                assertEquals(500, resp.getStatusLine().getStatusCode());
            }
            post.releaseConnection();
        }

    }
}
