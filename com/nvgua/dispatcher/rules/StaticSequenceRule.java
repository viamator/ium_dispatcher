package com.nvgua.dispatcher.rules;

import com.hp.siu.collector.rules.Rule;
import com.hp.siu.collector.rules.RuleException;
import com.hp.siu.logging.Logger;
import com.hp.siu.utils.*;

import java.util.HashMap;

/**
 * Created by Ermolenko V.
 * Date: 23.10.2014
 */
public class StaticSequenceRule extends Rule {

    private static Logger log = Logger.getLogger("com.hp.siu.collector.rules.StaticSequenceRule", "com.hp.siu.collector.rules.message_catalog");

    private int counterIndex;
    private static final HashMap<String, Long> counters = new HashMap<String, Long>();
    private boolean useInt = false;
    private long maxValue;
    private String path;

    private static synchronized long nextValue(String path, long maxValue) {
        Long counterValue = counters.get(path);
        if (counterValue == null) counterValue = 0L;
        if (counterValue >= maxValue) counterValue = 0L;
        counterValue += 1L;
        counters.put(path, counterValue);
        return counterValue;
    }

    @Override
    public void applyRule(NormalizedMeteredEvent nme, NormalizedMeteredEvent nme2) throws RuleException {
        Attribute localAttribute = nme.getAttribute(this.counterIndex);
        if (localAttribute == null) {
            localAttribute = NMESchema.getInstance().newAttributeInstance(this.counterIndex);
            nme.setAttribute(localAttribute, this.counterIndex);
        }
        long counter = nextValue(this.path, this.maxValue);
        if (this.useInt)
            ((IntegerAttributeIF)localAttribute).setValue((int)counter);
        else
            ((LongAttributeIF)localAttribute).setValue(counter);
        if (this.next_ != null)
            this.next_.applyRule(nme, nme2);
    }

    @Override
    public void configure(Config config) throws ConfigurationException {
        log.logDebug2("StaticSequenceRule.configure()");
        this.path = config.getFullName();
        String sequenceAttribute = config.getAttributeAsString("SequenceAttribute");
        AttributeInfo localAttributeInfo = NMESchema.getInstance().getAttribute(sequenceAttribute);

        if (localAttributeInfo == null)
            throw new ConfigurationException("StaticSequenceRule: the attribute SequenceAttribute with the value " + sequenceAttribute + " is not specified in the NMESchema");
        this.counterIndex = localAttributeInfo.getIndex();

        if (localAttributeInfo.getPrimitiveType() == 1)
            this.useInt = true;
        this.maxValue = config.getAttributeAsLong("MaxValue", this.useInt ? 2147483647L : 9223372036854775807L);

        if (this.maxValue < 1L)
            throw new ConfigurationException("StaticSequenceRule: MaxValue must be greater than zero");
        if ((this.useInt) && (this.maxValue > 2147483647L))
            throw new ConfigurationException("StaticSequenceRule: MaxValue must not be greater than 2147483647");
    }
}
