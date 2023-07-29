/**
 * Copyright (C) 2009-2011 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package net.spy.memcached.spring;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.FailureMode;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.transcoders.Transcoder;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

@SuppressWarnings("rawtypes")
public class MemcachedClientFactoryBean implements FactoryBean<MemcachedClient>,
        InitializingBean, DisposableBean {

  private final ConnectionFactoryBuilder connectionFactoryBuilder =
          new ConnectionFactoryBuilder();

  private String servers;
  private long shutdownTimeoutSeconds = 0;
  private MemcachedClient client;

  // Setters for configuring ConnectionFactoryBuilder

  public void setServers(String newServers) {
    this.servers = newServers;
  }

  public void setAuthDescriptor(AuthDescriptor to) {
    connectionFactoryBuilder.setAuthDescriptor(to);
  }

  public void setDaemon(boolean d) {
    connectionFactoryBuilder.setDaemon(d);
  }

  public void setFailureMode(FailureMode fm) {
    connectionFactoryBuilder.setFailureMode(fm);
  }

  public void setHashAlg(HashAlgorithm to) {
    connectionFactoryBuilder.setHashAlg(to);
  }

  public void setInitialObservers(Collection<ConnectionObserver> obs) {
    connectionFactoryBuilder.setInitialObservers(obs);
  }

  public void setLocatorType(Locator l) {
    connectionFactoryBuilder.setLocatorType(l);
  }

  public void setMaxReconnectDelay(long to) {
    connectionFactoryBuilder.setMaxReconnectDelay(to);
  }

  public void setOpFact(OperationFactory f) {
    connectionFactoryBuilder.setOpFact(f);
  }

  public void setOpQueueFactory(OperationQueueFactory q) {
    connectionFactoryBuilder.setOpQueueFactory(q);
  }

  public void setOpQueueMaxBlockTime(long t) {
    connectionFactoryBuilder.setOpQueueMaxBlockTime(t);
  }

  public void setOpTimeout(long t) {
    connectionFactoryBuilder.setOpTimeout(t);
  }

  public void setProtocol(Protocol prot) {
    connectionFactoryBuilder.setProtocol(prot);
  }

  public void setReadBufferSize(int to) {
    connectionFactoryBuilder.setReadBufferSize(to);
  }

  public void setReadOpQueueFactory(OperationQueueFactory q) {
    connectionFactoryBuilder.setReadOpQueueFactory(q);
  }

  public void setShouldOptimize(boolean o) {
    connectionFactoryBuilder.setShouldOptimize(o);
  }

  public void setTimeoutExceptionThreshold(int to) {
    connectionFactoryBuilder.setTimeoutExceptionThreshold(to);
  }

  public void setTranscoder(Transcoder<Object> t) {
    connectionFactoryBuilder.setTranscoder(t);
  }

  public void setUseNagleAlgorithm(boolean to) {
    connectionFactoryBuilder.setUseNagleAlgorithm(to);
  }

  public void setWriteOpQueueFactory(OperationQueueFactory q) {
    connectionFactoryBuilder.setWriteOpQueueFactory(q);
  }

  // Other methods

  @Override
  public MemcachedClient getObject() throws Exception {
    return client;
  }

  @Override
  public Class<?> getObjectType() {
    return MemcachedClient.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    client = new MemcachedClient(connectionFactoryBuilder.build(),
            AddrUtil.getAddresses(servers));
  }

  @Override
  public void destroy() throws Exception {
    if (shutdownTimeoutSeconds > 0) {
      client.shutdown(shutdownTimeoutSeconds, TimeUnit.SECONDS);
    } else {
      client.shutdown();
    }
  }

  /**
   * The number of seconds to wait for connections to finish before shutting
   * down the client.
   */
  public void setShutdownTimeoutSeconds(long shutdownTimeoutSeconds) {
    this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
  }
}
