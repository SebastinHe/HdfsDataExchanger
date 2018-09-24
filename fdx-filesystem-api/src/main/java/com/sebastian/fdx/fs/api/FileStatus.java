package com.sebastian.fdx.fs.api;

public class FileStatus {

    private String path;

    private long length;

    private boolean isdir;

    public FileStatus(String path, long length, boolean isdir) {
        super();
        this.path = path;
        this.length = length;
        this.isdir = isdir;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public boolean isIsdir() {
        return isdir;
    }

    public void setIsdir(boolean isdir) {
        this.isdir = isdir;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        return String.format("{path=%s, length=%s, isdir=%s}", path, length, isdir);
    }

}
