package org.cc.Tool;

public class tool {
    public static int ByteArrayToIntH(byte[] b,int startIndex)
    {
        return (int) ((b[startIndex] & 0xff) << 24
                | (b[startIndex+1] & 0xff) << 16
                | (b[startIndex+2] & 0xff) << 8
                | (b[startIndex+3] & 0xff));
    }
}
