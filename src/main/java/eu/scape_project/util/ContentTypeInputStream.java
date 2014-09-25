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
package eu.scape_project.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * A typed {@link java.io.InputStream} which can hold a Content-Type
 *
 * @author frank asseg
 *
 */
public class ContentTypeInputStream extends InputStream {

    private final InputStream src;

    private final String contentType;

    public ContentTypeInputStream(String contentType, InputStream src) {
        this.src = src;
        this.contentType = contentType;

    }

    @Override
    public int read() throws IOException {
        return src.read();
    }

    /**
     * Get the Content-Type associated with this {@link java.io.InputStream}
     * 
     * @return the Content-Type
     */
    public String getContentType() {
        return contentType;
    }

}
