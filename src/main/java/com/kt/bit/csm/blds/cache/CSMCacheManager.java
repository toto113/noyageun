package com.kt.bit.csm.blds.cache;


import com.github.jedis.lock.JedisLock;
import com.kt.bit.csm.blds.utility.CSMResultSet;
import com.kt.bit.csm.blds.utility.CachedResultSet;
import com.kt.bit.csm.blds.utility.CacheColumn;
import com.kt.bit.csm.blds.utility.DataFormmater;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;


public class CSMCacheManager {
    private String  host;
    private int     port;
    public HashMap cacheTargetList = null;
    public static CSMCacheManager instance = null;


    /**
     *
     * @return
     */
    public static CSMCacheManager getInstance(){
        if(instance == null){
            instance = new CSMCacheManager( "127.0.0.1", 6379);
        }

        return instance;
    }


    /**
     * Added Date : 2014.05.20
     */
    private JedisPool pool;

    /**
     * Added Date : 2014.05.20
     */
    public CSMCacheManager() {
        this.host = "127.0.0.1";
        this.port = 6379;

        pool = new JedisPool(new JedisPoolConfig(), this.host, this.port);

        cacheTargetList = new HashMap();

        /**
         * @TO-DO : 설정 파일을 참조하여 구성하기
         */
        cacheTargetList.put("pr_personal_annual", new CachePolicy());
        cacheTargetList.put("pr_personal_annual2", new CachePolicy());
    }


    /**
     * Added Date : 2014.05.23
     * ---------------------------
     *
     * @param host : Redis Master Node IP
     * @param port : Redis Master Node Port
     */
    public CSMCacheManager(String host, int port) {
        this.host = host;
        this.port = port;

        pool = new JedisPool(new JedisPoolConfig(), this.host, this.port);
        cacheTargetList = new HashMap();

        /**
         * @TO-DO : 설정 파일을 참조하여 구성하기
         */
        cacheTargetList.put("pr_personal_annual", new CachePolicy());
        cacheTargetList.put("pr_personal_annual2", new CachePolicy());
    }

    /**
     * Added Date : 2014.06.20
     * ---------------------------
     * get info by key
     *
     * @param key
     * @return
     */
    public byte[] getByteData(String key) {

        Jedis jedis = borrow();
        try {
            return jedis.get(key.getBytes());
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.08.20
     * ---------------------------
     * set info by key (insert, update)
     *
     * @param key
     * @param value
     * @return
     */
    public String setByteData(String key, byte[] value) {

        Jedis jedis = borrow();
        try {
            return jedis.set(key.getBytes(), value);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.17
     * ---------------------------
     * Redis Ping command for health check.
     *
     * @return
     */
    public String ping() {

        Jedis jedis = borrow();
        try {
            return jedis.ping();
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.16
     * ---------------------------
     * Write changed configurations to local file.
     * This is executed Lua script at redis server side.
     *
     * @return : Lua Script SHA1 value
     */
    public String configRewrite() {

        Jedis jedis = borrow();
        try {
            return jedis.scriptLoad("return redis.call('config', 'rewrite')");
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.16
     * ---------------------------
     * Check the configuration rewrite lua script execution result.
     *
     * @param sha1 : Script SHA1 value
     * @return : result message. if success, "OK" returned.
     */
    public Object configRewriteResult(String sha1) {

        Jedis jedis = borrow();
        try {
            return jedis.evalsha(sha1, 0);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.16
     * ---------------------------
     * Change the replication setting.
     * Set current slave to master.
     *
     * @return
     */
    public String slaveOfNoOne() {

        Jedis jedis = borrow();
        try {
            return jedis.slaveofNoOne();
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.16
     * ---------------------------
     * Change the replication setting.
     * This is the same as SLAVEOF command.
     *
     * @param host : Master Node IP Address
     * @param port : Master Node Port
     * @return
     */
    public String slaveOf(String host, int port) {

        Jedis jedis = borrow();


        try {
            return jedis.slaveof(host, port);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.12
     * ---------------------------
     *
     * @return : all key count
     */
    public long dbSize() {

        Jedis jedis = borrow();
        try {
            return jedis.dbSize();
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.09
     * ---------------------------
     * Redis Configuration Setter
     *
     * @param parameter : config parameter
     * @param value : config value
     * @return
     */
    public String configSet(String parameter, String value) {

        Jedis jedis = borrow();
        try {
            return jedis.configSet(parameter, value);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.06.09
     * ---------------------------
     * Get all configuration
     *
     * @param pattern
     * @return
     */
    public List<String> configGet(String pattern) {

        Jedis jedis = borrow();
        try {

            if (pattern == null || pattern.isEmpty()) {
                return jedis.configGet("*");
            }
            else {
                return jedis.configGet(pattern);
            }

        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.05.27
     * ---------------------------
     * Get all keys by pattern
     *
     * @param pattern
     * @return
     */
    public Set<String> keys(String pattern) {

        Jedis jedis = borrow();
        try {
            return jedis.keys(pattern);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * auto increment by key
     *
     * @param key
     * @return
     */
    public long incr(String key) {

        Jedis jedis = borrow();
        long result = 0L;
        JedisLock lock = new JedisLock(jedis,  "incr_lock:" + key, 10000, 60000);

        try {
            lock.acquire();
            result = jedis.incr(key);
        } catch (InterruptedException e) {
            //logger.debug(e.getMessage());
            e.printStackTrace();
        } finally {
            lock.release();
            revert(jedis);
        }

        return result;

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * auto decrement by key
     *
     * @param key
     * @return
     */
    public long decr(String key) {

        Jedis jedis = borrow();
        long result = 0L;
        JedisLock lock = new JedisLock(jedis, "decr_lock:" + key, 10000, 60000);

        try {
            lock.acquire();
            result = jedis.decr(key);
        } catch (InterruptedException e) {
            //logger.debug(e.getMessage());
            e.printStackTrace();
        } finally {
            lock.release();
            revert(jedis);
        }

        return result;

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * delete info by key
     *
     * @param key
     * @return
     */
    public long del(String key) {

        Jedis jedis = borrow();
        long result = 0L;
        JedisLock lock = new JedisLock(jedis, "del_lock:" + key, 10000, 60000);

        try {
            lock.acquire();
            result = jedis.del(key);
        } catch (InterruptedException e) {
//            logger.debug(e.getMessage());
            e.printStackTrace();
        } finally {
            revert(jedis);
        }

        return result;

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * get info by key
     *
     * @param key
     * @return
     */
    public String get(String key) {

        Jedis jedis = borrow();
        try {
            return jedis.get(key);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * set info by key (insert, update)
     *
     * @param key
     * @param value
     * @return
     */
    public String set(String key, String value) {

        Jedis jedis = borrow();
        try {
            return jedis.set(key, value);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * remove all data in db.
     * If use this method, critical.
     * Be careful.
     *
     * @return
     */
    public String clear() {

        Jedis jedis = borrow();
        try {
            return jedis.flushDB();
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * Check the key exists or not.
     *
     * @param key
     * @return
     */
    public boolean exists(String key) {

        Jedis jedis = borrow();
        try {
            return jedis.exists(key);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * get all redis server info for monitoring
     *
     * @return
     */
    public String info() {

        Jedis jedis = borrow();
        try {
            return jedis.info();
        } finally {
            revert(jedis);
        }

    }

    /**
     * Added Date : 2014.05.20
     * ---------------------------
     * get redis server info for monitoring of given section
     *
     * @param section
     * @return
     */
    public String info(String section) {

        Jedis jedis = borrow();
        try {
            return jedis.info(section);
        } finally {
            revert(jedis);
        }

    }



    /**
     * @TO-DO
     * @param spName
     * @return
     */
    public boolean isCacheTarget(String spName){
        CachePolicy policy = (CachePolicy)cacheTargetList.get(spName);
        if(policy != null && policy.isCacheTarget() )
            return true;
        else
            return false;
    }

    /**
     * @TO-DO
     * @param key
     * @return
     */
    public CSMResultSet getResultSet(String key){
        CSMResultSet resultSet = null;
        if( exists(key) ){
            CachedResultSet cachedResultSet = DataFormmater.fromJson(this.get(key));
            resultSet = new CSMResultSet(cachedResultSet);
        }

        return resultSet;
    }

    /**
     * @TO-DO
     * @param result
     */
    public void putResultSet(String key, CSMResultSet result){
          if(result.getDataSourceType() == CSMResultSet.RESULT_MODE_ORACLE){
              try {
                  ResultSetMetaData meta = result.getMetaData();
                  CachedResultSet cachedResultSet = new CachedResultSet();
                  int colCount = meta.getColumnCount();
                  int rowCount = 0;

                  while( result.next() ){
                      CacheColumn[] cacheColumns = new CacheColumn[colCount];
                      for(int i=0; i < colCount;i++){
                          cacheColumns[i] = new CacheColumn(meta.getColumnName(i), meta.getColumnType(i), result.getObject(i));
                      }
                      cachedResultSet.addRow(cacheColumns, rowCount);
                      rowCount++;
                  }

                  this.set(key, DataFormmater.toJson(cachedResultSet));
                  result.setCachedResultSet(cachedResultSet);
                  result.setDataSourceType(CSMResultSet.RESULT_MODE_CACHE);

              } catch (SQLException e) {
                  e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
              } catch (Exception e){
                  e.printStackTrace();
              } finally
              {
                  try {
                      result.close();
                  } catch (SQLException e) {
                      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                  }
              }
          }

    }

    /**
     * Added Date : 2014.06.09
     * ---------------------------
     * migrate given key to target redis server
     *
     * @param host
     * @param port
     * @param key
     * @return
     */
    public String migrate(String host, int port, String key) {

        Jedis jedis = borrow();
        try {
            return jedis.migrate(host, port, key, 0, 100000);
        } finally {
            revert(jedis);
        }

    }

    /**
     * Modified Date : 2014.06.12
     * ---------------------------
     * Added Date : 2014.06.09
     * ---------------------------
     * migration all keys in given slot to target redis server.
     * redis cluster is working, dev mode.
     *
     * @param host
     * @param port
     * @param slot
     * @return
     */
    @Deprecated
    public String migrate(String host, int port, int slot) {

        Jedis jedis = borrow();
        String result = "";
        try {

            List<String> keys = jedis.clusterGetKeysInSlot(slot, 1000);

            for (String key : keys) {
                result = migrate(host, port, key);
            }

        } finally {
            revert(jedis);
        }

        return result;

    }

    /**
     *
     */
    public void destory() {

        pool.destroy();

    }

    /**
     *
     * @return
     */
    public Jedis borrow() {

        return pool.getResource();

    }

    /**
     *
     * @param jedis
     */
    public void revert(Jedis jedis) {

        pool.returnResource(jedis);

    }

    @Deprecated
    public byte[] bGetJ(String key) {

        Jedis jedis = borrow();
        try {
            byte[] value = jedis.get(key.getBytes());
            if (value != null) {
                return value;
            }
            return null;
        } finally {
            revert(jedis);
        }

    }

    @Deprecated
    public String bSetJ(String key, byte[] value) {

        Jedis jedis = borrow();
        try {
            return jedis.set(key.getBytes(), value);
        } finally {
            revert(jedis);
        }

    }

    @Deprecated
    public List<String> mget(String[] keys) {

        Jedis jedis = borrow();

        try {
            return jedis.mget(keys);
        } finally {
            revert(jedis);
        }

    }

    @Deprecated
    public Set<String> sCopy(String key, String new_key) {

        Jedis jedis = borrow();
        try {
            Set<String> oldSets = jedis.smembers(key);
            for (String str : oldSets) {
                jedis.sadd(new_key, str);
            }
            return oldSets;
        } finally {
            revert(jedis);
        }

    }

    @Deprecated
    public void sClear(String key, String oldKey) {

        Jedis jedis = borrow();
        try {
            Set<String> oldSets = jedis.smembers(key);
            for (String str : oldSets) {
                jedis.del(oldKey + ":" + str);
            }
            jedis.del(key);
        } finally {
            revert(jedis);
        }

    }

    @Deprecated
    public Set<String> sMove(String key, String new_key) {

        Jedis jedis = borrow();
        try {
            Set<String> oldSets = jedis.smembers(key);
            for (String str : oldSets) {
                jedis.smove(key, new_key, str);
            }
            return oldSets;
        } finally {
            revert(jedis);
        }

    }

}
