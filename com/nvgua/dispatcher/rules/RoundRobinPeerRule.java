package com.nvgua.dispatcher.rules;

import com.hp.siu.collector.rules.Rule;
import com.hp.siu.collector.rules.RuleException;
import com.hp.siu.logging.Level;
import com.hp.siu.logging.Logger;
import com.hp.siu.utils.*;

/**
 * Created by Ermolenko V.
 * Date: 23.10.2014
 */
public class RoundRobinPeerRule extends Rule {

    private static Logger log = Logger.getLogger("com.hp.siu.collector.rules.RoundRobinPeerRule", "com.hp.siu.collector.rules.message_catalog");

    private static volatile int index = 0;

    private String[] peerList;
    private int iPrimaryAttribute = -1;
    private int iSecondaryAttribute = -1;


    public static synchronized int getIndex(int max) {
        index = getNextIndex(index, max);
        return index;
    }

    public static int getNextIndex(int index, int max) {
        index++;
        if (index > max) index = 0;
        return index;
    }

    @Override
    public void applyRule(NormalizedMeteredEvent nme, NormalizedMeteredEvent nme2) throws RuleException {
        log.logDebug2("RoundRobinPeerRule.applyRule()");

        int primaryIndex = getIndex(peerList.length - 1);
        StringAttribute primaryAttribute = (StringAttribute) nme.getAttribute(iPrimaryAttribute);
        if (primaryAttribute == null) {
            primaryAttribute = new StringAttribute();
            nme.setAttribute(primaryAttribute, iPrimaryAttribute);
        }
        primaryAttribute.setValue(peerList[primaryIndex]);
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("Primary = " + primaryAttribute.getValue());

        if (iSecondaryAttribute != -1) {
            int secondaryIndex = getNextIndex(primaryIndex, peerList.length - 1);
            StringAttribute secondaryAttribute = (StringAttribute) nme.getAttribute(iSecondaryAttribute);
            if (secondaryAttribute == null) {
                secondaryAttribute = new StringAttribute();
                nme.setAttribute(secondaryAttribute, iSecondaryAttribute);
            }

            secondaryAttribute.setValue(peerList[secondaryIndex]);

            if (log.isLoggable(Level.DEBUG2))
                log.logDebug2("Secondary = " + secondaryAttribute.getValue());
        }

        if (this.next_ != null)
            this.next_.applyRule(nme, nme2);

    }

    @Override
    public void configure(Config config) throws ConfigurationException {
        log.logDebug2("RoundRobinPeerRule.configure()");
        this.peerList = config.getAttributes("PeerList");
        this.iPrimaryAttribute = NMESchema.getInstance().getAttributeIndex(config, "PrimaryPeerAttribute", true);
        this.iSecondaryAttribute = NMESchema.getInstance().getAttributeIndex(config, "SecondaryPeerAttribute", false);
    }
}
