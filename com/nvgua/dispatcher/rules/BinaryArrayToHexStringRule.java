package com.nvgua.dispatcher.rules;

import com.hp.siu.collector.rules.Rule;
import com.hp.siu.collector.rules.RuleException;
import com.hp.siu.logging.Level;
import com.hp.siu.logging.Logger;
import com.hp.siu.utils.*;

/**
 * Created by Ermolenko V.
 * Date: 16.06.2014
 */
public class BinaryArrayToHexStringRule extends Rule {

    protected static Logger log = Logger.getLogger("com.hp.siu.collector.rules.BinaryArrayToHexStringRule", "com.hp.siu.collector.rules.message_catalog");

    private int iBinary;
    private int iString;

    @Override
    public void applyRule(NormalizedMeteredEvent nme, NormalizedMeteredEvent nme2) throws RuleException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("BinaryArrayToHexStringRule.configure()");
        BinaryAttribute binaryAttribute = (BinaryAttribute) nme.getAttribute(iBinary);
        byte[] bytes = binaryAttribute != null ? binaryAttribute.getValue() : null;
        if (bytes != null) {
            nme.setAttribute(new StringAttribute(ConvertUtils.convertBytesToHexString(bytes)), iString);
        }


        if (next_ != null)
            next_.applyRule(nme, nme2);
    }

    @Override
    public void configure(Config config) throws ConfigurationException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("BinaryArrayToHexStringRule.configure()");
        this.iString = NMESchema.getInstance().getAttributeIndex(config, "StringAttribute", true);
        this.iBinary = NMESchema.getInstance().getAttributeIndex(config, "BinaryAttribute", true);
    }
}
