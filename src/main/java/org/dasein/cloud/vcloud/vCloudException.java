/**
 * Copyright (C) 2009-2013 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.vcloud;

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;

/**
 * [Class Documentation]
 * <p>Created by George Reese: 2/5/13 3:10 PM</p>
 *
 * @author George Reese
 */
public class vCloudException extends CloudException {
    static public class Data {
        public int code;
        public String title;
        public String description;
        public CloudErrorType type;
    }

    static public Data parseException(@Nonnegative int code, @Nonnull String xml) {
        Data data = new Data();

        data.code = code;
        if( code == HttpServletResponse.SC_FORBIDDEN || code == HttpServletResponse.SC_UNAUTHORIZED ) {
            data.type = CloudErrorType.AUTHENTICATION;
        }
        else {
            data.type = CloudErrorType.GENERAL;
        }
        data.title = "Error";   // TODO: fix me
        data.description = xml;
        return data;
    }

    public vCloudException(@Nonnull Data data) {
        super(data.type, data.code, data.title, data.description);
    }

    public vCloudException(@Nonnull Throwable cause) {
        super(cause);
    }

    public vCloudException(@Nonnull CloudErrorType type, @Nonnegative int code, @Nonnull String title, @Nonnull String description) {
        super(type, code, title, description);
    }
}
