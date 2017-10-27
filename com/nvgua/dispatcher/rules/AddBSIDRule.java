package com.nvgua.dispatcher.rules;

import com.hp.siu.collector.rules.Rule;
import com.hp.siu.collector.rules.RuleException;
import com.hp.siu.logging.Level;
import com.hp.siu.logging.Logger;
import com.hp.siu.utils.*;

import java.util.Arrays;

/**
 * Created by Ermolenko V.
 * Date: 17.06.2014
 */
public class AddBSIDRule extends Rule {

    protected static Logger log = Logger.getLogger("com.hp.siu.collector.rules.AddBSIDRule", "com.hp.siu.collector.rules.message_catalog");

    private String BSIDConstant = "3E230001";
    private int iLocationInfo;
    private int iBSID;


    @Override
    public void applyRule(NormalizedMeteredEvent nme, NormalizedMeteredEvent nme2) throws RuleException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("AddBSIDRule.configure()");

        BinaryAttribute locationInfo = (BinaryAttribute) nme.getAttribute(iLocationInfo);
        byte[] bytes = locationInfo != null ? locationInfo.getByteArray() : null;
        if (bytes != null && bytes.length >= 8) {
            StringAttribute BSID = new StringAttribute();
            BSID.setValue(BSIDConstant + ConvertUtils.convertBytesToHexString(Arrays.copyOfRange(bytes, 6, 8)));
            nme.setAttribute(BSID, iBSID);
        } else {
            log.warning("Can not get location info: " + Arrays.toString(bytes));
        }

        if (next_ != null)
            next_.applyRule(nme, nme2);
    }

    @Override
    public void configure(Config config) throws ConfigurationException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("AddBSIDRule.configure()");
        this.BSIDConstant = config.getAttribute("BSIDConstant", this.BSIDConstant);
        this.iLocationInfo = NMESchema.getInstance().getAttributeIndex(config, "LocationInfoAttribute", true);
        this.iBSID = NMESchema.getInstance().getAttributeIndex(config, "BSIDAttribute", true);
    }
}
