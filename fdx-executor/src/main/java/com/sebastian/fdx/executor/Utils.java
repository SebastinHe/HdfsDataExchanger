package com.sebastian.fdx.executor;

import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class Utils {

    public static Map<String, String> parseXml(String xmlFIle) throws Exception {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setIgnoringComments(true);
        DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
        Map<String, String> parameters = new HashMap<String, String>();
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(xmlFIle));
            Document doc = builder.parse(in);
            Element root = doc.getDocumentElement();
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element))
                    continue;
                Element prop = (Element) propNode;

                if (!"property".equals(prop.getTagName()))
                    throw new Exception("bad conf file: element not <property>");

                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element))
                        continue;
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes())
                        attr = ((Text) field.getFirstChild()).getData().trim();

                    if ("value".equals(field.getTagName()) && field.hasChildNodes())
                        value = ((Text) field.getFirstChild()).getData();

                    parameters.put(attr, value);
                }
            }
        } finally {
            in.close();
        }

        return parameters;
    }


    public static String makeRelative(String root, String absPath) {
        StringTokenizer pathTokens = new StringTokenizer(absPath, File.separator);
        for (StringTokenizer rootTokens = new StringTokenizer(root, File.separator); rootTokens.hasMoreTokens(); ) {
            if (!rootTokens.nextToken().equals(pathTokens.nextToken())) {
                return null;
            }
        }
        StringBuilder sb = new StringBuilder();
        for (; pathTokens.hasMoreTokens(); ) {
            sb.append(pathTokens.nextToken());
            if (pathTokens.hasMoreTokens()) {
                sb.append(File.separator);
            }
        }
        return sb.length() == 0 ? "." : sb.toString();
    }


    public static void close(Closeable closeable) {
        if(closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) { }
        }
    }

    public static  int parseInt(String s, int defaultValue) {
        int result = defaultValue;
        try {
            result= Integer.parseInt(s);
        } catch (Exception exp) {}
        return result;
    }

    /**
     * 遍历出给定目录basePath下的所有文件
     * @param basePath 遍历目录
     * @param result   遍历结果
     */
    public static void getAllResouceFile(File basePath, Set<String> result) {
        if (basePath.isDirectory()) {
            File[] children = basePath.listFiles();

            for (File child : children) {
                if (child.isDirectory()) {
                    getAllResouceFile(child, result);
                } else {
                    result.add(child.getAbsolutePath());
                }
            }
        } else {
            result.add(basePath.getAbsolutePath());
        }
    }

    public static void log(String content) {
        log(content, null);
    }

    public static void log(String content, Exception exp) {
        System.out.println(content);
        if (exp != null)
            exp.printStackTrace();
    }


}
