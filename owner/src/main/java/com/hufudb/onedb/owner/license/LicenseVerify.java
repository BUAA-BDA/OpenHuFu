package com.hufudb.onedb.owner.license;

import de.schlichtherle.license.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.prefs.Preferences;

public class LicenseVerify {
    private static Logger LOG = LogManager.getLogger(LicenseVerify.class);

    public synchronized LicenseContent install(LicenseVerifyParam param) throws Exception {
        LicenseContent result = null;
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        LicenseManager licenseManager = LicenseManagerHolder.getInstance(initLicenseParam(param));
        licenseManager.uninstall();
        result = licenseManager.install(new File(param.getLicensePath()));
        LOG.info(MessageFormat.format("Certificate installation succeed, Validity of certificate:{0} - {1}",format.format(result.getNotBefore()),format.format(result.getNotAfter())));
        return result;
    }

    public boolean verify(){
        LicenseManager licenseManager = LicenseManagerHolder.getInstance(null);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            LicenseContent licenseContent = licenseManager.verify();
            LOG.info(MessageFormat.format("The certificate verification succeed, Validity of certificate:{0} - {1}",format.format(licenseContent.getNotBefore()),format.format(licenseContent.getNotAfter())));
            return true;
        }catch (Exception e){
            LOG.error("Certificate verification failed!",e);
            return false;
        }
    }

    private LicenseParam initLicenseParam(LicenseVerifyParam param){
        Preferences preferences = Preferences.userNodeForPackage(LicenseVerify.class);

        CipherParam cipherParam = new DefaultCipherParam(param.getStorePass());

        KeyStoreParam publicStoreParam = new CustomKeyStoreParam(LicenseVerify.class
                ,param.getPublicKeysStorePath()
                ,param.getPublicAlias()
                ,param.getStorePass()
                ,null);

        return new DefaultLicenseParam(param.getSubject()
                ,preferences
                ,publicStoreParam
                ,cipherParam);
    }

}
