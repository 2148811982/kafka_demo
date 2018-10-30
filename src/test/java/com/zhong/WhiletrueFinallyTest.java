package com.zhong;

public class WhiletrueFinallyTest {

	public static void main(String[] args) throws InterruptedException {
		try {
			while (true) {
				System.out.println("print");
				Thread.sleep(2000);
			} 
		} finally {
			System.out.println("finally");
		}
	}
}
