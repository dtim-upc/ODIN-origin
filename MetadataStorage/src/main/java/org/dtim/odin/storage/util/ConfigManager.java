package org.dtim.odin.storage.util;

import org.dtim.odin.storage.ApacheMain;
import org.dtim.odin.storage.BdiMain;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by snadal on 16/06/17.
 */
public class ConfigManager {

    public static String getProperty(String property) {
        java.util.Properties prop = new java.util.Properties();
        try {
            if (ApacheMain.configPath != null) {
                prop.load(new FileInputStream(ApacheMain.configPath));
            } else{
                prop.load(new FileInputStream(BdiMain.configPath));
            }
            return prop.getProperty(property);
        } catch (IOException ex) {
            return null;
        }
    }

}
