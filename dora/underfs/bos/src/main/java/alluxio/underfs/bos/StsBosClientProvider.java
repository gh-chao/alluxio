/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.bos;

import alluxio.conf.PropertyKey;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.ThreadFactoryUtils;
import com.baidubce.BceClientConfiguration;
import com.baidubce.auth.BceCredentials;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.auth.DefaultBceSessionCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.sts.StsClient;
import com.baidubce.services.sts.model.GetSessionTokenRequest;
import com.baidubce.services.sts.model.GetSessionTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * STS client provider for Aliyun BOS.
 */
public class StsBosClientProvider implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(StsBosClientProvider.class);

    private static final int ECS_META_GET_TIMEOUT = 10000;
    private static final int BASE_SLEEP_TIME_MS = 1000;
    private static final int MAX_SLEEP_MS = 3000;
    private static final int MAX_RETRIES = 5;
    private static final String ACCESS_KEY_ID = "AccessKeyId";
    private static final String ACCESS_KEY_SECRET = "AccessKeySecret";
    private static final String SECURITY_TOKEN = "SecurityToken";
    private static final String EXPIRATION = "Expiration";
    private final String mEcsMetadataServiceUrl;
    private final long mTokenTimeoutMs;
    private final UnderFileSystemConfiguration mBosConf;
    private final ScheduledExecutorService mRefreshBosClientScheduledThread;
    private volatile BosClient mBosClient = null;
    private long mStsTokenExpiration = 0;


    /**
     * Constructs a new instance of {@link StsBosClientProvider}.
     *
     * @param bosConfiguration {@link UnderFileSystemConfiguration} for BOS
     */
    public StsBosClientProvider(UnderFileSystemConfiguration bosConfiguration) {
        mBosConf = bosConfiguration;
        mEcsMetadataServiceUrl = bosConfiguration.getString(
                PropertyKey.UNDERFS_BOS_STS_ECS_METADATA_SERVICE_ENDPOINT);
        mTokenTimeoutMs = bosConfiguration.getMs(PropertyKey.UNDERFS_BOS_STS_TOKEN_REFRESH_INTERVAL_MS);

        mRefreshBosClientScheduledThread = Executors.newSingleThreadScheduledExecutor(
                ThreadFactoryUtils.build("refresh_bos_client-%d", false));
        mRefreshBosClientScheduledThread.scheduleAtFixedRate(() -> {
            try {
                createOrRefreshBosStsClient(mBosConf);
            } catch (Exception e) {
                //retry it
                LOG.warn("exception when refreshing BOS client access token", e);
            }
        }, 0, 60000, TimeUnit.MILLISECONDS);
    }

    /**
     * Init {@link StsBosClientProvider}.
     *
     * @throws IOException if failed to init BOS Client
     */
    public void init() throws IOException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                BASE_SLEEP_TIME_MS, MAX_SLEEP_MS, MAX_RETRIES);
        IOException lastException = null;
        while (retryPolicy.attempt()) {
            try {
                createOrRefreshBosStsClient(mBosConf);
                lastException = null;
                break;
            } catch (IOException e) {
                LOG.warn("init bos client failed! has retried {} times", retryPolicy.getAttemptCount(), e);
                lastException = e;
            }
        }
        if (lastException != null) {
            LOG.error("init bos client failed.", lastException);
            throw lastException;
        }
    }

    /**
     * Create Or Refresh the STS BOS client.
     *
     * @param bosConfiguration BOS {@link UnderFileSystemConfiguration}
     * @throws IOException if failed to create or refresh BOS client
     */
    protected void createOrRefreshBosStsClient(UnderFileSystemConfiguration bosConfiguration)
            throws IOException {
        BosClientConfiguration bosClientConf =
                BOSUnderFileSystem.initializeBOSClientConfig(bosConfiguration);

        doCreateOrRefreshStsBosClient(bosConfiguration, bosClientConf);
    }

    boolean tokenWillExpiredAfter(long after) {
        return mStsTokenExpiration - System.currentTimeMillis() <= after;
    }

    private void doCreateOrRefreshStsBosClient(UnderFileSystemConfiguration bosConfiguration, BosClientConfiguration clientConfiguration) throws IOException {

        if (tokenWillExpiredAfter(mTokenTimeoutMs)) {

            String endpoint = bosConfiguration.getString(PropertyKey.BOS_ENDPOINT_KEY);

            BceCredentials credentials = new DefaultBceCredentials(ACCESS_KEY_ID, ACCESS_KEY_SECRET);
            StsClient client = new StsClient(
                    new BceClientConfiguration().withEndpoint(endpoint).withCredentials(credentials)
            );

            GetSessionTokenResponse response = client.getSessionToken(new GetSessionTokenRequest());
            // or simply call:
            // GetSessionTokenResponse response = client.getSessionToken();
            // or you can specify limited permissions with ACL:
            // GetSessionTokenResponse response = client.getSessionToken(new GetSessionTokenRequest().withAcl("blabla"));
            // build DefaultBceSessionCredentials object from response:
            BceCredentials bosstsCredentials = new DefaultBceSessionCredentials(
                    response.getAccessKeyId(),
                    response.getSecretAccessKey(),
                    response.getSessionToken());


            BosClientConfiguration config = new BosClientConfiguration(clientConfiguration);
            config.setCredentials(bosstsCredentials);
            config.setEndpoint(endpoint);

            mBosClient = new BosClient(config);

            LOG.debug("bos sts client create success, expiration = {}", mStsTokenExpiration);
        }
    }

    /**
     * Returns the STS BOS client.
     *
     * @return bos client
     */
    public BosClient getBOSClient() {
        return mBosClient;
    }

    private Date convertStringToDate(String dateString) throws IOException {
        TimeZone zeroTimeZone = TimeZone.getTimeZone("ETC/GMT-0");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(zeroTimeZone);
        Date date = null;
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {
            throw new IOException(String.format("failed to parse date: %s", dateString), e);
        }
        return date;
    }

    @Override
    public void close() throws IOException {
        if (null != mRefreshBosClientScheduledThread) {
            mRefreshBosClientScheduledThread.shutdown();
        }
        if (null != mBosClient) {
            mBosClient.shutdown();
            mBosClient = null;
        }
    }

}
