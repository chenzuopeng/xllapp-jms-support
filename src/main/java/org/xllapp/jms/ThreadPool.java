package org.xllapp.jms;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 公用线程池.
 *
 * @author dylan.chen Dec 31, 2013
 * 
 */
public abstract class ThreadPool {

	private static ExecutorService threadPool = Executors.newCachedThreadPool();
	
	public static void execute(Runnable runnable){
		threadPool.execute(runnable);
	}

}
