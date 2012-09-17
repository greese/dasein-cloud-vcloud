package org.dasein.cloud.vcloud;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.ProviderContext;

import javax.annotation.Nonnull;

public class vCloud extends AbstractCloud {
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

    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls, @Nonnull String type) {
        String pkg = getLastItem(cls.getPackage().getName());

        if( pkg.equals("vcloud") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.vcloud." + type + "." + pkg + getLastItem(cls.getName()));
    }

    public vCloud() { }

    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getCloudName());

        return (name == null ? "Private vCloud Cloud" : name);
    }

    @Override
    public @Nonnull VDCServices getDataCenterServices() {
        return new VDCServices(this);
    }

    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getProviderName());

        return (name == null ? "VMware" : name);
    }

    public String toString() {
        ProviderContext ctx = getContext();

        return (getProviderName() + " - " + getCloudName() + (ctx == null ? "" : " [" + ctx.getAccountNumber() + "]"));
    }
}