package com.nvgua.dispatcher.rules;

import com.hp.siu.collector.rules.Rule;
import com.hp.siu.collector.rules.RuleException;
import com.hp.siu.logging.Level;
import com.hp.siu.logging.Logger;
import com.hp.siu.utils.*;

import java.util.Arrays;

import static com.nvgua.dispatcher.rules.ConvertUtils.*;

/**
 * Created by Ermolenko V.
 * Date: 17.06.2014
 */
public class FixUserLocationInfoRule extends Rule {

    protected static Logger log = Logger.getLogger("com.hp.siu.collector.rules.FixUserLocationInfoRule", "com.hp.siu.collector.rules.message_catalog");

    private int iLocationInfo;

    @Override
    public void applyRule(NormalizedMeteredEvent nme, NormalizedMeteredEvent nme2) throws RuleException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("FixUserLocationInfoRule.configure()");
        BinaryAttribute locationInfo = (BinaryAttribute) nme.getAttribute(iLocationInfo);
        byte[] bytes = locationInfo != null ? locationInfo.getByteArray() : null;
        if (bytes != null && bytes.length >= 8) {
            try {
                byte[] correct = new byte[8];
                byte[] last = convertHexStringToBytes(("0" + convertBytesToHexString(Arrays.copyOfRange(bytes, 6, 8))).substring(0, 4));
                System.arraycopy(bytes, 0, correct, 0, 6);
                System.arraycopy(last, 0, correct, 6, 2);
                locationInfo.setValue(correct);
            } catch (IllegalArgumentException e) {
                log.logException(Level.WARNING, "Illegal length of hex string", e);
            }
        } else {
            log.warning("Can not fix location info: " + Arrays.toString(bytes));
        }

        if (next_ != null)
            next_.applyRule(nme, nme2);
    }

    @Override
    public void configure(Config config) throws ConfigurationException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("FixUserLocationInfoRule.configure()");
        this.iLocationInfo = NMESchema.getInstance().getAttributeIndex(config, "LocationInfoAttribute", true);
    }
}



