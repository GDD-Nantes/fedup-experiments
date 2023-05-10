package fr.gdd.sage;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;



public class ReflectionUtils {
    
    public static Field _getField(Class<?> clazz, String name) {
        try {
            Field f = clazz.getDeclaredField(name);
            f.setAccessible(true);
            return f;
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    public static Method _getMethod(Class<?> clazz, String name, Class<?>... clazzes) {
        try {
            Method m = clazz.getDeclaredMethod(name, clazzes);
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    public static Object _callField(Field f, Class<?> clazz, Object o) {
        try {
            return f.get(clazz.cast(o));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }
    
    public static Object _callMethod(Method m, Class<?> clazz, Object o, Object... args) {
        try {
            return m.invoke(o, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    public static Class<?> _getClass(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }
}
