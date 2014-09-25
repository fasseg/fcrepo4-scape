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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;

import javax.xml.bind.JAXBException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.BeforeClass;

import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.util.ScapeMarshaller;

public class AbstractIT {

    protected static final String SCAPE_URL =
            "http://localhost:8080/scape";

    protected static final String FEDORA_URL = "http://localhost:8080/";

    protected final DefaultHttpClient client = new DefaultHttpClient();

    protected ScapeMarshaller marshaller;

    @BeforeClass
    public static void init() throws Exception {
        /* wait for an existing ContainerWrapper to finish */
       try{
           HttpGet get = new HttpGet("http://localhost:8080");
           DefaultHttpClient client = new DefaultHttpClient();
           while (client.execute(get).getStatusLine().getStatusCode() > 0){
               Thread.sleep(1000);
               System.out.println("Waiting for existing application server to stop...");
           }
        }catch(ConnectException e){
            // it's good, we can't connect to 8080
            // don't do exec flow by exception handling though ;)
        }
    }

    @Before
    public void setup() throws Exception {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    protected void postEntity(IntellectualEntity ie) throws IOException {
        HttpPost post = new HttpPost(SCAPE_URL + "/entity");
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try {
            this.marshaller.serialize(ie, sink);
        } catch (JAXBException e) {
            throw new IOException(e);
        }
        post.setEntity(new InputStreamEntity(new ByteArrayInputStream(sink
                .toByteArray()), sink.size(), ContentType.TEXT_XML));
        HttpResponse resp = this.client.execute(post);
        assertEquals(201, resp.getStatusLine().getStatusCode());
        String id = EntityUtils.toString(resp.getEntity());
        assertTrue(id.length() > 0);
        post.releaseConnection();
    }
}
