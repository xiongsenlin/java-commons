package org.xsl.common.util;

import org.apache.hadoop.hbase.util.Bytes;

public class BytesUtil {

	public static String getStringFromBytes(byte[] bytes) {
		return Bytes.toString(bytes);
	}

	public static int getIntegerFromBytes(byte[] bytes) {
		String targetStr = Bytes.toString(bytes);
		if (targetStr == null || targetStr.isEmpty() ) {
			return 0;
		}
		int target;
		try {
			target = Integer.valueOf(targetStr);
		} catch (NumberFormatException e) {
			target = 0;
		}
		return target;
	}

	public static long getLongFromBytes(byte[] bytes) {
		String targetStr = Bytes.toString(bytes);
		if (targetStr == null || targetStr.isEmpty() ) {
			return 0;
		}
		long target;
		try {
			target = Long.valueOf(targetStr);
		} catch (NumberFormatException e) {
			target = 0;
		}
		return target;
	}
	
	public static float getFloatFromBytes(byte[] bytes) {
		String targetStr = Bytes.toString(bytes);
		if (targetStr == null || targetStr.isEmpty() ) {
			return 0;
		}
		float target;
		try {
			target = Float.valueOf(targetStr);
		} catch (NumberFormatException e) {
			target = 0;
		}
		return target;
	}
	
	public static double getDoubleFromBytes(byte[] bytes) {
		String targetStr = Bytes.toString(bytes);
		if (targetStr == null || targetStr.isEmpty() ) {
			return 0;
		}
		double target;
		try {
			target = Double.valueOf(targetStr);
		} catch (NumberFormatException e) {
			target = 0;
		}
		return target;
	}
}
