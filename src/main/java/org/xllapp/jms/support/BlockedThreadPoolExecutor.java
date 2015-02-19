package org.xllapp.jms.support;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 支持阻塞的线程池.
 * 
 * @author dylan.chen Aug 18, 2013
 * 
 */
public class BlockedThreadPoolExecutor extends ThreadPoolExecutor implements RejectedExecutionHandler {

	private final static Logger logger = LoggerFactory.getLogger(BlockedThreadPoolExecutor.class);

	private ReentrantLock lock = new ReentrantLock();

	private Condition condition = this.lock.newCondition();

	public BlockedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
		setRejectedExecutionHandler(this);
	}

	public BlockedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
		setRejectedExecutionHandler(this);
	}

	public BlockedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		setRejectedExecutionHandler(this);
	}

	public BlockedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		setRejectedExecutionHandler(this);
	}

	public BlockedThreadPoolExecutor(int poolSize) {
		super(poolSize, poolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
		setRejectedExecutionHandler(this);
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {

		if (logger.isInfoEnabled()) {
			logger.info("{}[{}] refuse to execute command[{}]", Thread.currentThread().getName(), getCaller(), r);
		}

		if (!executor.isShutdown()) {
			this.lock.lock();
			try {
				if (logger.isInfoEnabled()) {
					logger.info("{}[{}] waiting for the completion of any thread execution,because the thread pool is full", Thread.currentThread().getName(), getCaller());
				}
				this.condition.await();
			} catch (InterruptedException ie) {
				logger.warn(Thread.currentThread().getName() + "[" + getCaller() + "] waiting to be interrupted", ie);
			} finally {
				this.lock.unlock();
			}

			try {
				Thread.sleep(3000); //被唤醒后,等待一段时间后,重新执行拒绝的任务
			} catch (Exception e) {
				// 忽略此异常
			}

			if (logger.isInfoEnabled()) {
				logger.info("{}[{}] re-execute {}", Thread.currentThread().getName(), getCaller(), r);
			}
			executor.execute(r);
		}
	}

	private String getCaller() {
		StackTraceElement ste = Thread.currentThread().getStackTrace()[5];
		return ste.getClassName() + "." + ste.getMethodName();
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		try {
			this.lock.lock();
			if (this.lock.hasWaiters(this.condition)) {
				this.condition.signal();
				logger.info("wake up waiting threads");
			}
		} finally {
			this.lock.unlock();
		}
	}

}