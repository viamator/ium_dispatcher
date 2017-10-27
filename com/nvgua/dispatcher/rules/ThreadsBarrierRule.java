package com.nvgua.dispatcher.rules;

import com.hp.siu.collector.rules.Rule;
import com.hp.siu.collector.rules.RuleChain;
import com.hp.siu.collector.rules.RuleException;
import com.hp.siu.logging.Level;
import com.hp.siu.logging.Logger;
import com.hp.siu.utils.*;

import java.util.Comparator;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by Ermolenko V.
 * Date: 17.06.2014
 */
public class ThreadsBarrierRule extends Rule implements Configurable, Reconfigurable {

    protected static Logger log = Logger.getLogger("com.hp.siu.collector.rules.ThreadsBarrierRule", "com.hp.siu.collector.rules.message_catalog");

    private static volatile int totalThreadCount = 0;
    private static final HashMap<String, Integer> threads = new HashMap<String, Integer>();
    private static final HashMap<String, Integer> maximums = new HashMap<String, Integer>();
    private static final HashMap<String, Long> times = new HashMap<String, Long>();

    private RuleChain successRuleChain = new RuleChain(false);
    private RuleChain failRuleChain = new RuleChain(false);
    private RuleChain timeoutRuleChain = new RuleChain(false);

    private int maxProcessingThreads = -1;
    private int maxPercent = -1;
    private int minGuaranteedTreads = 2;
    private int barrierTimeOut = 5000;
    private int iBarrierEntity = -1;

    private static synchronized int getTotalThreadCount() {
        return totalThreadCount;
    }

    private static synchronized int getThreadCount(String barrierEntity) {
        Integer value = threads.get(barrierEntity);
        return value == null ? 0 : value;
    }

    private static synchronized void incrementThreadCount(String barrierEntity) {
        totalThreadCount++;
        int i = getThreadCount(barrierEntity);
        i++;
        threads.put(barrierEntity, i);
        setTime(barrierEntity, 0);
        setMax(barrierEntity, i);
    }

    private static synchronized int getMax(String barrierEntity) {
        Integer value = maximums.get(barrierEntity);
        return value == null ? 0 : value;
    }

    private static synchronized void setMax(String barrierEntity, int max) {
        if (max > getMax(barrierEntity))
            maximums.put(barrierEntity, max);
    }

    private static synchronized long getTime(String barrierEntity) {
        Long value = times.get(barrierEntity);
        return value == null ? 0L : value;
    }

    private static synchronized void setTime(String barrierEntity, long time) {
        times.put(barrierEntity, time);
    }

    /**
     * Check and increment thread count
     *
     * @param barrierEntity        Barrier entity
     * @param minGuaranteedTreads  Guaranteed number of threads
     * @param maxProcessingThreads Maximum available number of threads
     * @param maxPercent           Max percent
     * @param barrierTimeOut       Barrier timeout
     * @return 0 - successfully incremented; 1 - barrier is reached; 2 - timeout is happened.
     */
    public static synchronized int incrementThreadCount(String barrierEntity, int minGuaranteedTreads, int maxProcessingThreads,
                                                        int maxPercent, long barrierTimeOut) {
        int current = getThreadCount(barrierEntity);
        int maxThreads = minGuaranteedTreads;
        if (maxProcessingThreads != -1 && maxPercent != -1) {
            int available = maxProcessingThreads - getTotalThreadCount();
            maxThreads = (current + available) * maxPercent / 100;
            if (maxThreads < minGuaranteedTreads) maxThreads = minGuaranteedTreads;
        }
        if (current + 1 <= maxThreads) {
            incrementThreadCount(barrierEntity);
            return 0;
        } else {
            long time = getTime(barrierEntity);
            if (time == 0) {
                time = System.currentTimeMillis();
                setTime(barrierEntity, time);
            }
            long diff = System.currentTimeMillis() - time;
            if (diff >= barrierTimeOut) {
                return 2;
            } else {
                return 1;
            }
        }
    }

    public static synchronized void decrementThreadCount(String barrierEntity) {
        int i = threads.get(barrierEntity);
        if (i > 0) i--;
        threads.put(barrierEntity, i);
        if (totalThreadCount > 0) totalThreadCount--;
    }

    public static synchronized String getStatistics() {
        if (threads.size() > 0) {
            // Sorting
            SortedSet<String> sorted = new TreeSet<String>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return new StringBuilder(o1).reverse().toString().compareTo(new StringBuilder(o2).reverse().toString());
                }
            });
            sorted.addAll(threads.keySet());
            // Forming string
            boolean first = true;
            StringBuilder builder = new StringBuilder("{");
            for (String barrierEntity : sorted) {
                int current = getThreadCount(barrierEntity);
                int max = getMax(barrierEntity);
                if (first) first = false; else builder.append(", ");
                builder.append(barrierEntity).append("=").append(max > current ? max : current);
            }
            builder.append("}");
            maximums.clear();
            return builder.toString();
        } else {
            return "No Data";
        }
    }


    @Override
    public void applyRule(NormalizedMeteredEvent nme, NormalizedMeteredEvent nme2) throws RuleException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("ThreadsBarrierRule.applyRule()");
        StringAttribute barrierEntityAttribute = (StringAttribute) nme.getAttribute(iBarrierEntity);
        String barrierEntity = barrierEntityAttribute != null ? barrierEntityAttribute.getValue() : null;
        if (barrierEntity != null) {
            int status = 0;
            try {
                status = incrementThreadCount(barrierEntity, minGuaranteedTreads, maxProcessingThreads, maxPercent, barrierTimeOut);
                if (log.isLoggable(Level.DEBUG2))
                    log.logDebug2("ThreadsBarrierRule.incrementThreadCount: status=" + status + ", barrierEntity=" + barrierEntity);
                switch (status) {
                    case 1:
                        log.warning("Barrier for " + barrierEntity + " is reached");
                        this.failRuleChain.applyRule(nme, nme2);
                        break;
                    case 2:
                        log.logError("Timeout for " + barrierEntity);
                        this.timeoutRuleChain.applyRule(nme, nme2);
                        break;
                    default:
                        if (log.isLoggable(Level.DEBUG2))
                            log.logDebug2("ThreadsBarrierRule.successRuleChain.applyRule()");
                        this.successRuleChain.applyRule(nme, nme2);
                }
            } finally {
                if (status == 0) {
                    decrementThreadCount(barrierEntity);
                    if (log.isLoggable(Level.DEBUG2))
                        log.logDebug2("ThreadsBarrierRule.decrementThreadCount: barrierEntity=" + barrierEntity);
                }
            }
        } else {
            log.warning("Cannot get barrier entity");
        }

        if (next_ != null)
            next_.applyRule(nme, nme2);

    }

    @Override
    public void configure(Config config) throws ConfigurationException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("ThreadsBarrierRule.configure()");
        this.iBarrierEntity = NMESchema.getInstance().getAttributeIndex(config, "BarrierEntityAttribute", true);
        this.maxProcessingThreads = config.getAttributeAsInt("MaxProcessingThreads", this.maxProcessingThreads);
        this.maxPercent = config.getAttributeAsInt("MaxPercent", this.maxPercent);
        this.minGuaranteedTreads = config.getAttributeAsInt("MinGuaranteedThreads", this.minGuaranteedTreads);
        this.barrierTimeOut = config.getAttributeAsInt("BarrierTimeOut", this.barrierTimeOut);
        this.successRuleChain.configureChain(config, "OnSuccessRuleChain");
        this.failRuleChain.configureChain(config, "OnOverloadRuleChain");
        this.timeoutRuleChain.configureChain(config, "OnOverloadTimeoutRuleChain");
    }

    @Override
    public void reconfigure(Config config) throws ConfigurationException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("ThreadsBarrierRule.reconfigure()");
        this.successRuleChain.reconfigure(config);
        this.failRuleChain.reconfigure(config);
        this.timeoutRuleChain.reconfigure(config);
    }

    @Override
    public void validate(Config config, Config config2) throws ConfigurationException {
        if (log.isLoggable(Level.DEBUG2))
            log.logDebug2("ThreadsBarrierRule.validate()");
        this.successRuleChain.validate(config, config2);
        this.failRuleChain.validate(config, config2);
        this.timeoutRuleChain.validate(config, config2);
    }

}
