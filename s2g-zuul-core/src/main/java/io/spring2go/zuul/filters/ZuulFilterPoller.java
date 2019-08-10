package io.spring2go.zuul.filters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.collect.Maps;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import io.spring2go.zuul.common.Constants;
import io.spring2go.zuul.common.FilterInfo;

public class ZuulFilterPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZuulFilterPoller.class);

    // Running filters
    private Map<String, FilterInfo> runningFilters = Maps.newHashMap();

    // key: zuul.filter.poller.enabled, default: true.
    private DynamicBooleanProperty pollerEnabled = DynamicPropertyFactory.getInstance().getBooleanProperty(Constants.ZUUL_FILTER_POLLER_ENABLED, true);

    // Poller sleep time, default 30s.
    private DynamicLongProperty pollerInterval = DynamicPropertyFactory.getInstance().getLongProperty(Constants.ZUUL_FILTER_POLLER_INTERVAL, 30000);

    // key: zuul.use.active.filters, default: true
    private DynamicBooleanProperty active = DynamicPropertyFactory.getInstance().getBooleanProperty(Constants.ZUUL_USE_ACTIVE_FILTERS, true);

    // key: zuul.use.canary.filters, default: false.
    private DynamicBooleanProperty canary = DynamicPropertyFactory.getInstance().getBooleanProperty(Constants.ZUUL_USE_CANARY_FILTERS, false);

    // Filter save path.
    private DynamicStringProperty preFiltersPath = DynamicPropertyFactory.getInstance().getStringProperty(Constants.ZUUL_FILTER_PRE_PATH, null);
    private DynamicStringProperty routeFiltersPath = DynamicPropertyFactory.getInstance().getStringProperty(Constants.ZUUL_FILTER_ROUTE_PATH, null);
    private DynamicStringProperty postFiltersPath = DynamicPropertyFactory.getInstance().getStringProperty(Constants.ZUUL_FILTER_POST_PATH, null);
    private DynamicStringProperty errorFiltersPath = DynamicPropertyFactory.getInstance().getStringProperty(Constants.ZUUL_FILTER_ERROR_PATH, null);
    private DynamicStringProperty customFiltersPath = DynamicPropertyFactory.getInstance().getStringProperty(Constants.Zuul_FILTER_CUSTOM_PATH, null);

    private static ZuulFilterPoller instance = null;

    /**
     * Default construction method.
     */
    private ZuulFilterPoller() {
        this.checkerThread.start();
    }

    /**
     * Get a static ZuulFilterPoller instance.
     *
     * @return ZuulFilterPoller instance
     */
    public static ZuulFilterPoller getInstance() {
        return instance;
    }

    /**
     * Start ZuulFilterPoller thread.
     */
    public static void start() {
        if (instance == null) {
            // 保证ZuulFilterPoller实例只有一个
            synchronized (ZuulFilterPoller.class) {
                if (instance == null) {
                    instance = new ZuulFilterPoller();
                }
            }
        }
    }

    private volatile boolean running = true;

    /**
     * 疑问：为什么这里不是该类不是直接继承Thread就好了呢？
     */
    private Thread checkerThread = new Thread("ZuulFilterPoller") {
        public void run() {
            while (running) {
                try {
                    if (!pollerEnabled.get()) {
                        continue;
                    }
                    if (canary.get()) {
                        // Cat埋点监控
                        Transaction tran = Cat.getProducer().newTransaction("FilterPoller", "canary-" + ZuulFilterDaoFactory.getCurrentType());

                        try {
                            Map<String, FilterInfo> filterSet = Maps.newHashMap();
                            List<FilterInfo> activeScripts = ZuulFilterDaoFactory.getZuulFilterDao().getAllActiveFilters();
                            if (!activeScripts.isEmpty()) {
                                for (FilterInfo filterInfo : activeScripts) {
                                    filterSet.put(filterInfo.getFilterId(), filterInfo);
                                }
                            }

                            List<FilterInfo> canaryScripts = ZuulFilterDaoFactory.getZuulFilterDao().getAllCanaryFilters();
                            if (!canaryScripts.isEmpty()) {
                                for (FilterInfo filterInfo : canaryScripts) {
                                    filterSet.put(filterInfo.getFilterId(), filterInfo);
                                }
                            }

                            for (FilterInfo filterInfo : filterSet.values()) {
                                doFilterCheck(filterInfo);
                            }
                            tran.setStatus(Transaction.SUCCESS);
                        } catch (Throwable t) {
                            tran.setStatus(t);
                            Cat.logError(t);
                        } finally {
                            tran.complete();
                        }
                    } else if (active.get()) {
                        // Cat埋点监控
                        Transaction tran = Cat.getProducer().newTransaction("FilterPoller", "active-" + ZuulFilterDaoFactory.getCurrentType());

                        try {
                            // Fetch all active filter.
                            List<FilterInfo> newFilters = ZuulFilterDaoFactory.getZuulFilterDao().getAllActiveFilters();

                            tran.setStatus(Transaction.SUCCESS);

                            if (newFilters.isEmpty()) {
                                continue;
                            }

                            // Check if this filter is running ?
                            for (FilterInfo newFilter : newFilters) {
                                doFilterCheck(newFilter);
                            }

                        } catch (Throwable t) {
                            tran.setStatus(t);
                            Cat.logError(t);
                        } finally {
                            tran.complete();
                        }
                    }
                } catch (Throwable t) {
                    LOGGER.error("ZuulFilterPoller run error!", t);
                } finally {
                    try {
                        sleep(pollerInterval.get());
                    } catch (InterruptedException e) {
                        LOGGER.error("ZuulFilterPoller sleep error!", e);
                    }
                }
            }
        }
    };

    /**
     * Check filter groovy file is exist. If not, write to disk.
     *
     * @param newFilter A new filter.
     * @throws IOException
     */
    private void doFilterCheck(FilterInfo newFilter) throws IOException {
        FilterInfo existFilter = runningFilters.get(newFilter.getFilterId());
        if (existFilter == null || !existFilter.equals(newFilter)) {
            LOGGER.info("Adding filter to disk: {}.", JSON.toJSONString(newFilter));

            writeFilterToDisk(newFilter);
            // Put this new filter to running filters map.
            runningFilters.put(newFilter.getFilterId(), newFilter);
        }
    }

    /**
     * @param newFilter
     * @throws IOException
     */
    private void writeFilterToDisk(FilterInfo newFilter) throws IOException {
        String filterType = newFilter.getFilterType();

        String path = preFiltersPath.get();
        if (filterType.equals("post")) {
            path = postFiltersPath.get();
        } else if (filterType.equals("route")) {
            path = routeFiltersPath.get();
        } else if (filterType.equals("error")) {
            path = errorFiltersPath.get();
        } else if (!filterType.equals("pre") && customFiltersPath.get() != null) {
            path = customFiltersPath.get();
        }

        File f = new File(path, newFilter.getFilterName() + ".groovy");
        FileWriter file = new FileWriter(f);
        BufferedWriter out = new BufferedWriter(file);
        // Write base64 code to file.
        out.write(newFilter.getFilterCode());
        out.close();
        file.close();

        LOGGER.info("Filter written to {}.", f.getPath());
    }

    public void stop() {
        this.running = false;
    }
}
