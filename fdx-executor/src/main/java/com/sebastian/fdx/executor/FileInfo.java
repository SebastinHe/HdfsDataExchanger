package com.sebastian.fdx.executor;

public class FileInfo {
    //源文件路径
    String input;

    //迁移到目标文件系统后的路径
    String output;

    //源文件大小
    long fileLength;

    FileInfo() { }

    FileInfo(String input, long fileLength) {
        this.input = input;
        this.fileLength = fileLength;
    }

    FileInfo(String input, String output, long fileLength) {
        this(input, fileLength);
        this.output = output;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public long getFileLength() {
        return fileLength;
    }

    public void setFileLength(long fileLength) {
        this.fileLength = fileLength;
    }

    public String toString() {
        return input + " : " + output + " : " + fileLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileInfo fileInfo = (FileInfo) o;

        if (fileLength != fileInfo.fileLength) return false;
        if (input != null ? !input.equals(fileInfo.input) : fileInfo.input != null) return false;
        return output != null ? output.equals(fileInfo.output) : fileInfo.output == null;
    }

    @Override
    public int hashCode() {
        int result = input != null ? input.hashCode() : 0;
        result = 31 * result + (output != null ? output.hashCode() : 0);
        result = 31 * result + (int) (fileLength ^ (fileLength >>> 32));
        return result;
    }
}