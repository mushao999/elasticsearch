/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
todo:
(1)
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.Constants;
import org.elasticsearch.core.internal.io.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.PidFile;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.InternalSettingsPreparer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * Internal startup code.
 * ES启动模块：单例模式
 */
final class Bootstrap {

    private static volatile Bootstrap INSTANCE;//单例对象
    private volatile Node node;//节点对象
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);//keepAlive线程的栅栏
    private final Thread keepAliveThread;//keepAlive线程
    private final Spawner spawner = new Spawner();//todo://michelmu

    /**
     * //构造器只做了一件事，创建了一个keepalive线程
     * todo：high keepalive线程的工作原理
     */
    Bootstrap() {
        keepAliveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    keepAliveLatch.await();
                } catch (InterruptedException e) {
                    // bail out
                }
            }
        }, "elasticsearch[keepAlive/" + Version.CURRENT + "]");
        keepAliveThread.setDaemon(false);//当都是后台进程时会退出
        // keep this thread alive (non daemon thread) until we shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                keepAliveLatch.countDown();
            }
        });
    }

    /**
     * initialize native resources
     * 初始化本地资源
     */
    public static void initializeNatives(Path tmpFile, boolean mlockAll, boolean systemCallFilter, boolean ctrlHandler) {
        final Logger logger = Loggers.getLogger(Bootstrap.class);

        //step 1 check if the user is running as root, and bail
        //小功能点1：对root权限运行进行拦截
        // 不能以root权限运行原因分析：（1）会有一些外部脚本，如果权限过高会有问题
        if (Natives.definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run elasticsearch as root");
        }

        //step 2 enable system call filter
        //系统调用过滤器，猜测应该是对系统调用做限制，对插件和脚本权限进行限制
        if (systemCallFilter) {
            Natives.tryInstallSystemCallFilter(tmpFile);
        }

        //step 3 mlockall if requested
        //如果设置了配置选项就设置内存锁
        //bootstrap.mlockall: true
        //当设置bootstrap.mlockall: true时，启动es报警告Unknown mlockall error 0，因为linux系统默认能让进程锁住的内存为45k。
        //解决方法：设置为无限制，linux命令：ulimit -l unlimited
        if (mlockAll) {
            if (Constants.WINDOWS) {
                Natives.tryVirtualLock();
            } else {
                Natives.tryMlockall();
            }
        }

        // listener for windows close event
        if (ctrlHandler) {
            Natives.addConsoleCtrlHandler(new ConsoleCtrlHandler() {
                @Override
                public boolean handle(int code) {
                    if (CTRL_CLOSE_EVENT == code) {
                        logger.info("running graceful exit on windows");
                        try {
                            Bootstrap.stop();
                        } catch (IOException e) {
                            throw new ElasticsearchException("failed to stop node", e);
                        }
                        return true;
                    }
                    return false;
                }
            });
        }

        // force remainder of JNA to be loaded (if available).
        try {
            JNAKernel32Library.getInstance();
        } catch (Exception ignored) {
            // we've already logged this.
        }

        Natives.trySetMaxNumberOfThreads();
        Natives.trySetMaxSizeVirtualMemory();
        Natives.trySetMaxFileSize();

        // init lucene random seed. it will use /dev/urandom where available:
        StringHelper.randomId();
    }

    static void initializeProbes() {
        // Force probes to be loaded
        ProcessProbe.getInstance();
        OsProbe.getInstance();
        JvmInfo.jvmInfo();
    }

    //初始化bootstrap
    private void setup(boolean addShutdownHook, Environment environment) throws BootstrapException {
        Settings settings = environment.settings();

        try {
            spawner.spawnNativeControllers(environment);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }

        initializeNatives(
            environment.tmpFile(),
            BootstrapSettings.MEMORY_LOCK_SETTING.get(settings),
            BootstrapSettings.SYSTEM_CALL_FILTER_SETTING.get(settings),
            BootstrapSettings.CTRLHANDLER_SETTING.get(settings));

        // initialize probes before the security manager is installed
        initializeProbes();

        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        IOUtils.close(node, spawner);
                        LoggerContext context = (LoggerContext) LogManager.getContext(false);
                        Configurator.shutdown(context);
                    } catch (IOException ex) {
                        throw new ElasticsearchException("failed to stop node", ex);
                    }
                }
            });
        }

        try {
            // look for jar hell
            final Logger logger = LogManager.getLogger(JarHell.class);
            JarHell.checkJarHell(logger::debug);
        } catch (IOException | URISyntaxException e) {
            throw new BootstrapException(e);
        }

        // Log ifconfig output before SecurityManager is installed
        IfConfig.logIfNecessary();

        // install SM after natives, shutdown hooks, etc.
        try {
            Security.configure(environment, BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(settings));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new BootstrapException(e);
        }

        node = new Node(environment) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(
                final BootstrapContext context,
                final BoundTransportAddress boundTransportAddress, List<BootstrapCheck> checks) throws NodeValidationException {
                BootstrapChecks.check(context, boundTransportAddress, checks);
            }

            @Override
            protected void registerDerivedNodeNameWithLogger(String nodeName) {
                LogConfigurator.setNodeName(nodeName);
            }
        };
    }

    /**
     * 获取加密配置 todo:low 后续了解
     *
     * @param initialEnv
     * @return
     * @throws BootstrapException
     */
    static SecureSettings loadSecureSettings(Environment initialEnv) throws BootstrapException {
        final KeyStoreWrapper keystore;
        try {
            keystore = KeyStoreWrapper.load(initialEnv.configFile());
        } catch (IOException e) {
            throw new BootstrapException(e);
        }

        try {
            if (keystore == null) {
                final KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create();
                keyStoreWrapper.save(initialEnv.configFile(), new char[0]);
                return keyStoreWrapper;
            } else {
                keystore.decrypt(new char[0] /* TODO: read password from stdin */);
                KeyStoreWrapper.upgrade(keystore, initialEnv.configFile(), new char[0]);
            }
        } catch (Exception e) {
            throw new BootstrapException(e);
        }
        return keystore;
    }

    /**
     * 生成运行环境 todo:low 后续了解
     * @param foreground
     * @param pidFile
     * @param secureSettings
     * @param initialSettings
     * @param configPath
     * @return
     */
    private static Environment createEnvironment(
        final boolean foreground,
        final Path pidFile,
        final SecureSettings secureSettings,
        final Settings initialSettings,
        final Path configPath) {
        Terminal terminal = foreground ? Terminal.DEFAULT : null;
        Settings.Builder builder = Settings.builder();
        if (pidFile != null) {
            builder.put(Environment.PIDFILE_SETTING.getKey(), pidFile);
        }
        builder.put(initialSettings);
        if (secureSettings != null) {
            builder.setSecureSettings(secureSettings);
        }
        return InternalSettingsPreparer.prepareEnvironment(builder.build(), terminal, Collections.emptyMap(), configPath);
    }

    /**
     * 启动操作：启动节点，启动keepalive线程
     * @throws NodeValidationException
     */
    private void start() throws NodeValidationException {
        node.start();
        keepAliveThread.start();
    }

    /**
     * 关闭节点，停止keepalive线程
     * @throws IOException
     */
    static void stop() throws IOException {
        try {
            IOUtils.close(INSTANCE.node, INSTANCE.spawner);
        } finally {
            INSTANCE.keepAliveLatch.countDown();
        }
    }

    /**
     * This method is invoked by {@link Elasticsearch#main(String[])} to startup elasticsearch.
     * 启动程序主要执行函数，由Elasticsearch的main方法直接调用
     */
    static void init(
        final boolean foreground,
        final Path pidFile,
        final boolean quiet,
        final Environment initialEnv) throws BootstrapException, NodeValidationException, UserException {
        //step 1 force the class initializer for BootstrapInfo to run before
        // the security manager is installed todo:这一句话的意思是什么，结合security manager了解
        //获取一些系统环境变量(static代码块完成)，提供一些Natvive参数的查询函数（虽然这些参数还没有被设置）
        BootstrapInfo.init();

        //step 2 调用构造函数，只干了一件事情，启动keepalive线程
        INSTANCE = new Bootstrap();

        //step 3 获取加密配置，并生成环境
        final SecureSettings keystore = loadSecureSettings(initialEnv);
        final Environment environment = createEnvironment(foreground, pidFile, keystore, initialEnv.settings(), initialEnv.configFile());

        //step 4 为日志服务配置节点名称和环境
        if (Node.NODE_NAME_SETTING.exists(environment.settings())) {
            LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(environment.settings()));
        }
        try {
            LogConfigurator.configure(environment);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        //step 5 创建pid文件
        if (environment.pidFile() != null) {
            try {
                PidFile.create(environment.pidFile(), true);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }
        }

        //step 6 启动实例
        final boolean closeStandardStreams = (foreground == false) || quiet;
        try {
            if (closeStandardStreams) {
                final Logger rootLogger = LogManager.getRootLogger();
                final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
                if (maybeConsoleAppender != null) {
                    Loggers.removeAppender(rootLogger, maybeConsoleAppender);
                }
                closeSystOut();
            }

            // fail if somebody replaced the lucene jars
            checkLucene();

            // install the default uncaught exception handler; must be done before security is
            // initialized as we do not want to grant the runtime permission
            // setDefaultUncaughtExceptionHandler
            Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler());

            INSTANCE.setup(true, environment);

            try {
                // any secure settings must be read during node construction
                IOUtils.close(keystore);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }

            INSTANCE.start();

            if (closeStandardStreams) {
                closeSysError();
            }
        } catch (NodeValidationException | RuntimeException e) {
            // disable console logging, so user does not see the exception twice (jvm will show it already)
            final Logger rootLogger = LogManager.getRootLogger();
            final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
            if (foreground && maybeConsoleAppender != null) {
                Loggers.removeAppender(rootLogger, maybeConsoleAppender);
            }
            Logger logger = LogManager.getLogger(Bootstrap.class);
            // HACK, it sucks to do this, but we will run users out of disk space otherwise
            if (e instanceof CreationException) {
                // guice: log the shortened exc to the log file
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = null;
                try {
                    ps = new PrintStream(os, false, "UTF-8");
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
                new StartupException(e).printStackTrace(ps);
                ps.flush();
                try {
                    logger.error("Guice Exception: {}", os.toString("UTF-8"));
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
            } else if (e instanceof NodeValidationException) {
                logger.error("node validation exception\n{}", e.getMessage());
            } else {
                // full exception
                logger.error("Exception", e);
            }
            // re-enable it if appropriate, so they can see any logging during the shutdown process
            if (foreground && maybeConsoleAppender != null) {
                Loggers.addAppender(rootLogger, maybeConsoleAppender);
            }

            throw e;
        }
    }

    @SuppressForbidden(reason = "System#out")
    private static void closeSystOut() {
        System.out.close();
    }

    @SuppressForbidden(reason = "System#err")
    private static void closeSysError() {
        System.err.close();
    }

    //@mushao: done
    //通过查看包中的luncene版本和设置的是否一致来判断Lucene是否ok
    private static void checkLucene() {
        if (Version.CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) == false) {
            throw new AssertionError("Lucene version mismatch this version of Elasticsearch requires lucene version ["
                + Version.CURRENT.luceneVersion + "]  but the current lucene version is [" + org.apache.lucene.util.Version.LATEST + "]");
        }
    }

}
