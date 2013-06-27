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

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.vcloud.compute.vCloudComputeServices;
import org.dasein.cloud.vcloud.network.vCloudNetworkServices;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Bootstrapping class for interacting with vCloud Director through Dasein Cloud. This implementation is a complete
 * re-architecture from scratch with options for backwards compatibility with the old jclouds-based implementation of
 * Dasein Cloud for vCloud.
 * @author George Reese
 * @since 2013.04
 * @version 2013.04 initial version
 */
public class vCloud extends AbstractCloud {
    static private final Logger logger = getLogger(vCloud.class);
    public final static String ISO8601_PATTERN       = "yyy-MM-dd'T'HH:mm:ss.SSSZ";
    
    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');

        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }

    static public Logger getLogger(Class<?> cls) {
        String pkg = getLastItem(cls.getPackage().getName());

        if( pkg.equals("aws") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.vcloud.std." + pkg + getLastItem(cls.getName()));
    }

    static public Logger getWireLogger(Class<?> cls) {
        return Logger.getLogger("dasein.cloud.vcloud.wire." + getLastItem(cls.getPackage().getName()) + "." + getLastItem(cls.getName()));
    }

    static public String escapeXml(String nonxml) {
        StringBuilder str = new StringBuilder();

        for( int i=0; i<nonxml.length(); i++ ) {
            char c = nonxml.charAt(i);

            switch( c ) {
                case '&': str.append("&amp;"); break;
                case '>': str.append("&gt;"); break;
                case '<': str.append("&lt;"); break;
                case '"': str.append("&quot;"); break;
                case '[': str.append("&#091;"); break;
                case ']': str.append("&#093;"); break;
                case '!': str.append("&#033;"); break;
                default: str.append(c);
            }
        }
        return str.toString();
    }

    public vCloud() { }

    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getCloudName());

        return (name == null ? "Private vCloud Cloud" : name);
    }

    @Override
    public @Nonnull vCloudComputeServices getComputeServices() {
        return new vCloudComputeServices(this);
    }

    @Override
    public @Nonnull VDCServices getDataCenterServices() {
        return new VDCServices(this);
    }

    @Override
    public @Nonnull vCloudNetworkServices getNetworkServices() {
        return new vCloudNetworkServices(this);
    }

    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getProviderName());

        return (name == null ? "VMware" : name);
    }

    public @Nullable String[] getVersionPreference() {
        ProviderContext ctx = getContext();
        String value;

        if( ctx == null ) {
            value = null;
        }
        else {
            Properties p = ctx.getCustomProperties();

            if( p == null ) {
                value = null;
            }
            else {
                value = p.getProperty("versionPreference");
            }
        }
        if( value == null ) {
            value = System.getProperty("vCloudVersionPreference");
        }
        if( value == null ) {
            return null;
        }
        else if( value.contains(",") ) {
            return value.trim().split(",");
        }
        else {
            return new String[] { value };
        }
    }


    public @Nonnull String getVMProductsResource() {
        ProviderContext ctx = getContext();
        String value;

        if( ctx == null ) {
            value = null;
        }
        else {
            Properties p = ctx.getCustomProperties();

            if( p == null ) {
                value = null;
            }
            else {
                value = p.getProperty("vmproducts");
            }
        }
        if( value == null ) {
            value = System.getProperty("vcloud.vmproducts");
        }
        if( value == null ) {
            value = "/org/dasein/cloud/vcloud/vmproducts.json";
        }
        return value;
    }

    public boolean isCompat() {
        ProviderContext ctx = getContext();
        String value;

        if( ctx == null ) {
            value = null;
        }
        else {
            Properties p = ctx.getCustomProperties();

            if( p == null ) {
                value = null;
            }
            else {
                value = p.getProperty("compat");
            }
        }
        if( value == null ) {
            value = System.getProperty("vCloudCompat");
        }
        return (value != null && value.equalsIgnoreCase("true"));
    }

    public boolean isInsecure() {
        ProviderContext ctx = getContext();
        String value;

        if( ctx == null ) {
            value = null;
        }
        else {
            Properties p = ctx.getCustomProperties();

            if( p == null ) {
                value = null;
            }
            else {
                value = p.getProperty("insecure");
            }
        }
        if( value == null ) {
            value = System.getProperty("insecure");
        }
        return (value != null && value.equalsIgnoreCase("true"));
    }

    public @Nonnegative long parseTime(@Nullable String time) throws CloudException {
        if( time == null || time.length() < 1 ) {
            return 0L;
        }
        //2013-02-02T22:16:45.917-05:00
        if( time.endsWith("-05:00") || time.endsWith("+00:00") ) {
            String tz;

            if( time.endsWith("-05:00") ) {
                int idx = time.lastIndexOf('-');

                time = time.substring(0,idx);
                tz = "America/New York";
            }
            else {
                int idx = time.lastIndexOf('-');

                time = time.substring(0,idx);
                tz = "GMT";
            }
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            fmt.setTimeZone(TimeZone.getTimeZone(tz));
            if( time.length() > 0 ) {
                try {
                    return fmt.parse(time).getTime();
                }
                catch( ParseException e ) {
                    throw new CloudException("Could not parse date: " + time);
                }
            }
        }
        else {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

            if( time.length() > 0 ) {
                try {
                    return fmt.parse(time).getTime();
                }
                catch( ParseException e ) {
                    fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    try {
                        return fmt.parse(time).getTime();
                    }
                    catch( ParseException encore ) {
                        throw new CloudException("Could not parse date: " + time);
                    }
                }
            }
        }
        return 0L;
    }

    @Override
    public String testContext() {
        APITrace.begin(this, "testContext");
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                logger.warn("No context exists for testing");
                return null;
            }
            try {
                vCloudMethod method = new vCloudMethod(this);

                method.authenticate(true);
                return ctx.getAccountNumber();
            }
            catch( Throwable t ) {
                logger.warn("Unable to connect to " + getCloudName() + " for " + ctx.getAccountNumber() + ": " + t.getMessage());
                return null;
            }
        }
        finally {
            APITrace.end();
        }
    }

    public @Nonnull String toID(@Nonnull String url) {
        String[] parts = url.split("/");

        if( parts.length > 2 ) {
            if( isCompat() ) {
                return "/" + parts[parts.length-2] + "/" + parts[parts.length-1];
            }
            else {
                return parts[parts.length-1];
            }
        }
        return url;
    }

    @Override
    public @Nonnull String toString() {
        ProviderContext ctx = getContext();

        return (getProviderName() + " - " + getCloudName() + (ctx == null ? "" : " [" + ctx.getAccountNumber() + "]"));
    }
    
    public static Date parseIsoDate(String isoDateString) {
		SimpleDateFormat df = new SimpleDateFormat( ISO8601_PATTERN );
        
		//handle TimeZone info
        
		 if ( isoDateString.endsWith( "Z" ) ) {
			 isoDateString = isoDateString.substring( 0, isoDateString.length() - 1) + "GMT-00:00";
	     } 
		 else {
			int exclude = 6;
	        
	        String first = isoDateString.substring( 0, isoDateString.length() - exclude );
	        String second = isoDateString.substring( isoDateString.length() - exclude, isoDateString.length() );
	
	        isoDateString = first + "GMT" + second;
	    }
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date result = null;
        
        try {
			result = df.parse( isoDateString );
		} catch (ParseException e) {
			logger.error("Could not parse date : " + isoDateString + " as a ISO6801 pattern : " + e.getMessage());
			return null;
		}
        return result;
	}
}