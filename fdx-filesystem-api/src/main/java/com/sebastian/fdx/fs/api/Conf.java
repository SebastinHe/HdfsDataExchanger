package com.sebastian.fdx.fs.api;

import java.util.HashMap;
import java.util.Map;

public class Conf {

    //kerberos  simple
    public static final String HADOOP_SERCURITY_AUTHENTCATION = "hadoop.security.authentication";

    //true false
    public static final String HADOOP_SERCURITY_AUTHORIZATION ="hadoop.security.authorization";

    public static final String NAMENODE_KERBEROS_PRINCIPAL = "dfs.namenode.kerberos.principal";

    public static final String DFS_NAMENOE_KEYTAB_FILE = "dfs.namenode.keytab.file";

    public static final String USER_KERBEROS_PRINCIPAL = "user.kerberos.principal";

    public static final String USER_KEYTAB_FILE = "user.keytab.file";

    public static final String FS_DEFAULT_NAME = "fs.default.name";

    public static final String HDFS_CONF_PATH="hdfs.conf.path";

    private Map<String, String> properties = new HashMap<String, String>();

    public void addProperty(String key, String value){
        properties.put(key, value);
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public String toString() {
        return "Conf [properties=" + properties + "]";
    }

}
