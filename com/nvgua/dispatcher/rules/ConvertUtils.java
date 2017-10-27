package com.nvgua.dispatcher.rules;

import com.hp.siu.utils.NMEAdapter;
import com.hp.siu.utils.NormalizedMeteredEvent;
import com.hp.usage.array.ByteMutableArray;
import com.hp.usage.nme.ArrayAllocator;
import com.hp.usage.nme.AttributeRef;
import com.hp.usage.nme.AttributeTypeMismatchException;
import com.hp.usage.nme.NME;

import static com.hp.usage.nme.NMEManager.getArrayAllocator;

/**
 * Created by Ermolenko V.
 * Date: 17.06.2014
 */
public class ConvertUtils {

    final private static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String convertBytesToHexString(byte[] bytes) {
        if (bytes != null) {
            char[] hexChars = new char[bytes.length * 2];
            for (int j = 0; j < bytes.length; j++) {
                int v = bytes[j] & 0xFF;
                hexChars[j * 2] = hexArray[v >>> 4];
                hexChars[j * 2 + 1] = hexArray[v & 0x0F];
            }
            return new String(hexChars);
        }
        return null;
    }

    public static byte[] convertHexStringToBytes(String s) throws IllegalArgumentException {
        int len = s.length();
        if (len % 2 != 0)
            throw new IllegalArgumentException("Wrong hex string: " + s);
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

}
