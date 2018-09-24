package com.sebastian.fdx.fs.api;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface BaseFileSystem {

      void initFileSystem(Conf conf) throws Exception;

      FileStatus getFileStatus(String path) throws Exception;

      DataOutputStream create(String path) throws Exception;

      DataInputStream open(String path) throws Exception;

      FileStatus[] listStatus(String path) throws Exception;

      boolean exists(String path) throws Exception;

      boolean mkdirs(String path) throws Exception;

      boolean createNewFile(String path) throws Exception;

      String getParent(String path) throws Exception;

      boolean delete(String path, boolean recursive) throws Exception;

      void close() throws Exception;

}
