package com.hp.siu.collector.rules;

import com.hp.siu.logging.Level;
import com.hp.siu.logging.Logger;
import com.hp.siu.utils.Config;
import com.hp.siu.utils.ConfigurationException;

import java.util.ArrayList;
import java.util.HashMap;

// Referenced classes of package com.hp.siu.collector.rules:
//            LookupCache

public class RoundRobinCache
    implements LookupCache
{

    private static class RRElement {
    	public RRElement(){
    		size_=0;
    		last_=-1;
    		objects_=new ArrayList();
    	}
    	
    	public void Add(Object obj){
    		objects_.add(obj);
    		size_++;
    	}
    	
    	public Object Get(){
    		last_++;
    		if (last_==size_) last_=0;
    		return objects_.get(last_);
    	}
    	
    	
    	private int size_;
    	private int last_;
    	private ArrayList objects_;
    }
    
	public RoundRobinCache()
    {
        hashMap_ = null;
        cacheSize_ = 0;
        ignoreSize_ = false;
    }

    public void configure(Config config)
        throws ConfigurationException
    {
        log_.logDebug4("RoundRobinCache.configure()");
        cacheSize_ = config.getAttributeAsInt("CacheSize", 100);
        if(cacheSize_ < 0)
            throw new ConfigurationException("\"CacheSize\" can't be a negative value");
        setSize(cacheSize_);
        if(config.getAttributeAsBoolean("_IgnoreCacheSize", false))
        {
            log_.logWarning("HashTableCacheMsg2");
            ignoreSize_ = true;
        }
    }

    private void setSize(int i)
    {
        log_.logDebug4("RoundRobinCache.setSize()");
        hashMap_ = new HashMap(i);
        if(log_.isLoggable(Level.DEBUG))
            log_.logDebug("Cache Size set to " + i);
    }

    public Object put(Object obj, Object obj1)
    {
        if(!ignoreSize_ && hashMap_.size() >= cacheSize_)
        {
            log_.logWarning("HashTableCacheMsg1");
            return null;
        } else
        {
            RRElement rre = (RRElement)hashMap_.get(obj);
            if (rre==null)
            	rre = new RRElement();
        	rre.Add(obj1);
        	return hashMap_.put(obj, rre);
        }
    }

    public Object get(Object obj)
    {
        RRElement rre =(RRElement)(hashMap_.get(obj));
        if (rre==null)
        	return null;
        else return rre.Get();
    }

    public void clear()
    {
        log_.logDebug4("RoundRobinCache.clear()");
        hashMap_.clear();
    }

    private static Logger log_ = Logger.getLogger("com.hp.siu.collector.rules.HashTableCache", "com.hp.siu.collector.rules.message_catalog");
    private HashMap hashMap_;
    private int cacheSize_;
    private boolean ignoreSize_;

}
