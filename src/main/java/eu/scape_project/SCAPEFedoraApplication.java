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
package eu.scape_project;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jersey2.InstrumentedResourceMethodApplicationListener;
import org.fcrepo.http.commons.FedoraApplication;
import org.fcrepo.http.commons.session.SessionProvider;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.process.internal.RequestScoped;

import javax.jcr.Session;

public class SCAPEFedoraApplication extends FedoraApplication {
    public SCAPEFedoraApplication() {
        packages("org.fcrepo", "eu.scape_project.resource");
        register(new FactoryBinder());
        register(MultiPartFeature.class);
        register(JacksonFeature.class);
        register(new InstrumentedResourceMethodApplicationListener(new MetricRegistry()));
    }

    static class FactoryBinder extends AbstractBinder {

        @Override
        protected void configure() {
            bindFactory(SessionProvider.class)
                    .to(Session.class)
                    .in(RequestScoped.class);
        }
    }
}
