package com.sebastian.fdx.fs.h1;

import com.sebastian.fdx.fs.api.BaseFileSystem;
import com.sebastian.fdx.fs.api.Conf;
import com.sebastian.fdx.fs.api.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * hadoop1
 */
public class FileSystem implements BaseFileSystem {

    org.apache.hadoop.fs.FileSystem fs;

    @Override
    public FileStatus getFileStatus(String path) throws Exception {
        org.apache.hadoop.fs.FileStatus tmp =fs.getFileStatus(new Path(path));
        return new FileStatus(tmp.getPath().toString(), tmp.getLen(), tmp.isDir());
    }

    @Override
    public void initFileSystem(Conf conf) throws Exception {
        Configuration configuration = new Configuration();
        List<FileInputStream> fisList = new ArrayList<FileInputStream>();
        try{
            String[] pathArr = conf.getProperty(Conf.HDFS_CONF_PATH).split(";");
            for(String path : pathArr) {
                FileInputStream fis = new FileInputStream(path);
                configuration.addResource(fis);
                fisList.add(fis);
            }

            Configuration newConf = new Configuration();
            newConf.set(Conf.FS_DEFAULT_NAME, configuration.get(Conf.FS_DEFAULT_NAME));

            if(configuration.getBoolean(Conf.HADOOP_SERCURITY_AUTHORIZATION,false) && "kerberos".equals(configuration.get(Conf.HADOOP_SERCURITY_AUTHENTCATION))) {
                newConf.set(Conf.HADOOP_SERCURITY_AUTHORIZATION, configuration.get(Conf.HADOOP_SERCURITY_AUTHORIZATION));
                newConf.set(Conf.HADOOP_SERCURITY_AUTHENTCATION, configuration.get(Conf.HADOOP_SERCURITY_AUTHENTCATION));
                newConf.set(Conf.USER_KERBEROS_PRINCIPAL, configuration.get(Conf.USER_KERBEROS_PRINCIPAL));
                newConf.set(Conf.USER_KEYTAB_FILE, configuration.get(Conf.USER_KEYTAB_FILE));
                newConf.set(Conf.NAMENODE_KERBEROS_PRINCIPAL, configuration.get(Conf.NAMENODE_KERBEROS_PRINCIPAL));

                UserGroupInformation.setConfiguration(newConf);
                UserGroupInformation.loginUserFromKeytab(newConf.get(Conf.USER_KERBEROS_PRINCIPAL), newConf.get(Conf.USER_KEYTAB_FILE));
            }

            fs = org.apache.hadoop.fs.FileSystem.get(newConf);
        }catch (Exception exp){
            throw exp;
        } finally {
            for(FileInputStream f : fisList) {
                IOUtils.closeStream(f);
            }
        }
    }


    @Override
    public FileStatus[] listStatus(String path) throws Exception {
        org.apache.hadoop.fs.FileStatus[] tmp = fs.listStatus(new Path(path));
        FileStatus[] result = new FileStatus[tmp.length];
        for(int i=0; i<tmp.length; ++i) {
            result[i] = new FileStatus(tmp[i].getPath().toString(), tmp[i].getLen(), tmp[i].isDir());
        }
        return result;
    }

    @Override
    public boolean exists(String path) throws Exception {
        return fs.exists(new Path(path));
    }

    @Override
    public boolean mkdirs(String path) throws Exception {
        return fs.mkdirs(new Path(path));
    }

    @Override
    public boolean createNewFile(String path) throws Exception {
        return fs.createNewFile(new Path(path));
    }

    @Override
    public String getParent(String path) throws Exception {
        return fs.getFileStatus(new Path(path)).getPath().getParent().toString();
    }

    @Override
    public DataOutputStream create(String path) throws Exception {
        return fs.create(new Path(path));
    }

    @Override
    public DataInputStream open(String path) throws Exception {
        return fs.open(new Path(path));
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeStream(fs);
    }

    @Override
    public boolean delete(String path, boolean recursive) throws Exception {
        return fs.delete(new Path(path), recursive);
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" + fs + "}";
    }
}
