package com.broadcast.utils;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Predicate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JsonpathUtil
{
    private JsonpathUtil() {

    }
    public static List<Map<String, Object>> getList(Object document, String path) {
        try {
            return JsonPath.read(document, path, new Predicate[0]);
        } catch (Exception var3) {
            return Collections.emptyList();
        }
    }
    public static String getValue(Object document, String path) {
        try {
            return JsonPath.read(document, path, new Predicate[0]).toString();
        } catch (Exception var3) {
            return "";
        }
    }
    public static String getValueIfString(Object document, String path) {
        try {
            return JsonPath.read(document, path, new Predicate[0]);
        } catch (Exception var3) {
            return "";
        }
    }
    public static List<String> getStringList(Object document, String path) {
        try {
            return JsonPath.read(document, path, new Predicate[0]);
        } catch (Exception var3) {
            return Collections.emptyList();
        }
    }
    public static Map<String, Object> getObject(Object document, String path) {
        try {
            return JsonPath.read(document, path, new Predicate[0]);
        } catch (Exception var3) {
            return Collections.emptyMap();
        }
    }
}
