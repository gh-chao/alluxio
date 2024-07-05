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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;
import com.baidubce.BceClientException;
import com.baidubce.Protocol;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.http.DefaultRetryPolicy;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Aliyun BOS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class BOSUnderFileSystem extends ObjectUnderFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(BOSUnderFileSystem.class);

    /**
     * Suffix for an empty file to flag it as a directory.
     */
    private static final String FOLDER_SUFFIX = "/";

    /**
     * Default owner of objects if owner cannot be determined.
     */
    private static final String DEFAULT_OWNER = "";

    /**
     * Aliyun BOS client.
     */
    private final BosClient mClient;

    /**
     * Bucket name of user's configured Alluxio bucket.
     */
    private final String mBucketName;

    /**
     * The executor service for the streaming upload.
     */
    private final Supplier<ListeningExecutorService> mStreamingUploadExecutor;

    /**
     * The executor service for the multipart upload.
     */
    private final Supplier<ListeningExecutorService> mMultipartUploadExecutor;
    /**
     * The permissions associated with the bucket. Fetched once and assumed to be immutable.
     */
    private final Supplier<ObjectPermissions> mPermissions
            = CommonUtils.memoize(this::getPermissionsInternal);
    private StsBosClientProvider mClientProvider;

    /**
     * Constructor for {@link BOSUnderFileSystem}.
     *
     * @param uri        the {@link AlluxioURI} for this UFS
     * @param bosClient  Aliyun BOS client
     * @param bucketName bucket name of user's configured Alluxio bucket
     * @param conf       configuration for this UFS
     */
    protected BOSUnderFileSystem(AlluxioURI uri, @Nullable BosClient bosClient, String bucketName,
                                 UnderFileSystemConfiguration conf) {
        super(uri, conf);

        if (conf.getBoolean(PropertyKey.UNDERFS_BOS_STS_ENABLED)) {
            try {
                mClientProvider = new StsBosClientProvider(conf);
                mClientProvider.init();
                mClient = mClientProvider.getBOSClient();
            } catch (IOException e) {
                LOG.error("init sts client provider failed!", e);
                throw new BceClientException("", e);
            }
        } else if (null != bosClient) {
            mClient = bosClient;
        } else {
            Preconditions.checkArgument(conf.isSet(PropertyKey.BOS_ACCESS_KEY),
                    "Property %s is required to connect to BOS", PropertyKey.BOS_ACCESS_KEY);
            Preconditions.checkArgument(conf.isSet(PropertyKey.BOS_SECRET_KEY),
                    "Property %s is required to connect to BOS", PropertyKey.BOS_SECRET_KEY);
            Preconditions.checkArgument(conf.isSet(PropertyKey.BOS_ENDPOINT_KEY),
                    "Property %s is required to connect to BOS", PropertyKey.BOS_ENDPOINT_KEY);
            String accessId = conf.getString(PropertyKey.BOS_ACCESS_KEY);
            String accessKey = conf.getString(PropertyKey.BOS_SECRET_KEY);
            String endPoint = conf.getString(PropertyKey.BOS_ENDPOINT_KEY);

            BosClientConfiguration bosClientConf = initializeBOSClientConfig(conf);
            bosClientConf.setCredentials(new DefaultBceCredentials(accessId, accessKey));
            bosClientConf.setEndpoint(endPoint);

            mClient = new BosClient(bosClientConf);
        }

        mBucketName = bucketName;

        // Initialize the executor service for the streaming upload.
        mStreamingUploadExecutor = Suppliers.memoize(() -> {
            int numTransferThreads =
                    conf.getInt(PropertyKey.UNDERFS_BOS_STREAMING_UPLOAD_THREADS);
            ExecutorService service = ExecutorServiceFactories
                    .fixedThreadPool("alluxio-bos-streaming-upload-worker",
                            numTransferThreads).create();
            return MoreExecutors.listeningDecorator(service);
        });

        // Initialize the executor service for the multipart upload.
        mMultipartUploadExecutor = Suppliers.memoize(() -> {
            int numTransferThreads =
                    conf.getInt(PropertyKey.UNDERFS_BOS_MULTIPART_UPLOAD_THREADS);
            ExecutorService service = ExecutorServiceFactories
                    .fixedThreadPool("alluxio-bos-multipart-upload-worker",
                            numTransferThreads).create();
            return MoreExecutors.listeningDecorator(service);
        });
    }

    /**
     * Constructs a new instance of {@link BOSUnderFileSystem}.
     *
     * @param uri  the {@link AlluxioURI} for this UFS
     * @param conf the configuration for this UFS
     * @return the created {@link BOSUnderFileSystem} instance
     */
    public static BOSUnderFileSystem createInstance(AlluxioURI uri, UnderFileSystemConfiguration conf)
            throws Exception {
        String bucketName = UnderFileSystemUtils.getBucketName(uri);
        return new BOSUnderFileSystem(uri, null, bucketName, conf);
    }

    /**
     * Creates an BOS {@code ClientConfiguration} using an Alluxio Configuration.
     *
     * @param alluxioConf the BOS Configuration
     * @return the BOS {@link BosClientConfiguration}
     */
    public static BosClientConfiguration initializeBOSClientConfig(
            AlluxioConfiguration alluxioConf) {
        BosClientConfiguration bosClientConf = new BosClientConfiguration();
        bosClientConf
                .setConnectionTimeoutInMillis((int) alluxioConf.getMs(PropertyKey.UNDERFS_BOS_CONNECT_TIMEOUT));
        bosClientConf.setSocketTimeoutInMillis((int) alluxioConf.getMs(PropertyKey.UNDERFS_BOS_SOCKET_TIMEOUT));
        bosClientConf.setMaxConnections(alluxioConf.getInt(PropertyKey.UNDERFS_BOS_CONNECT_MAX));

        bosClientConf.setRetryPolicy(new DefaultRetryPolicy(alluxioConf.getInt(PropertyKey.UNDERFS_BOS_RETRY_MAX), 20000L));

        if (isProxyEnabled(alluxioConf)) {
            String proxyHost = getProxyHost(alluxioConf);
            int proxyPort = getProxyPort(alluxioConf);
            bosClientConf.setProxyHost(proxyHost);
            bosClientConf.setProxyPort(proxyPort);
            bosClientConf.setProtocol(getProtocol());
            LOG.info("the proxy for BOS is enabled, the proxy endpoint is: {}:{}", proxyHost, proxyPort);
        }
        return bosClientConf;
    }

    private static boolean isProxyEnabled(AlluxioConfiguration alluxioConf) {
        return getProxyHost(alluxioConf) != null && getProxyPort(alluxioConf) > 0;
    }

    private static int getProxyPort(AlluxioConfiguration alluxioConf) {
        int proxyPort = alluxioConf.getInt(PropertyKey.UNDERFS_BOS_PROXY_PORT);
        if (proxyPort >= 0) {
            return proxyPort;
        } else {
            try {
                return getProxyPortFromSystemProperty();
            } catch (NumberFormatException e) {
                return proxyPort;
            }
        }
    }

    private static String getProxyHost(AlluxioConfiguration alluxioConf) {
        String proxyHost = alluxioConf.getOrDefault(PropertyKey.UNDERFS_BOS_PROXY_HOST, null);
        if (proxyHost != null) {
            return proxyHost;
        } else {
            return getProxyHostFromSystemProperty();
        }
    }

    private static Protocol getProtocol() {
        String protocol = Configuration.getString(PropertyKey.UNDERFS_BOS_PROTOCOL);
        return protocol.equals(Protocol.HTTPS.toString()) ? Protocol.HTTPS : Protocol.HTTP;
    }

    /**
     * Returns the Java system property for proxy port depending on
     * {@link #getProtocol()}: i.e. if protocol is https, returns
     * the value of the system property https.proxyPort, otherwise
     * returns value of http.proxyPort.  Defaults to {@link this.proxyPort}
     * if the system property is not set with a valid port number.
     */
    private static int getProxyPortFromSystemProperty() {
        return getProtocol() == Protocol.HTTPS
                ? Integer.parseInt(getSystemProperty("https.proxyPort"))
                : Integer.parseInt(getSystemProperty("http.proxyPort"));
    }

    /**
     * Returns the Java system property for proxy host depending on
     * {@link #getProtocol()}: i.e. if protocol is https, returns
     * the value of the system property https.proxyHost, otherwise
     * returns value of http.proxyHost.
     */
    private static String getProxyHostFromSystemProperty() {
        return getProtocol() == Protocol.HTTPS
                ? getSystemProperty("https.proxyHost")
                : getSystemProperty("http.proxyHost");
    }

    /**
     * Returns the value for the given system property.
     */
    private static String getSystemProperty(String property) {
        return System.getProperty(property);
    }

    @Override
    public void cleanup() throws IOException {
        long cleanAge = mUfsConf.getMs(PropertyKey.UNDERFS_BOS_INTERMEDIATE_UPLOAD_CLEAN_AGE);
        Date cleanBefore = new Date(new Date().getTime() - cleanAge);

        ListMultipartUploadsResponse uploadListing = mClient.listMultipartUploads(
                new ListMultipartUploadsRequest(mBucketName));
        do {
            for (MultipartUploadSummary upload : uploadListing.getMultipartUploads()) {
                if (upload.getInitiated().compareTo(cleanBefore) < 0) {
                    mClient.abortMultipartUpload(new AbortMultipartUploadRequest(
                            mBucketName, upload.getKey(), upload.getUploadId()));
                }
            }
            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(mBucketName);
            request.setKeyMarker(uploadListing.getKeyMarker());

            uploadListing = mClient.listMultipartUploads(request);
        } while (uploadListing.isTruncated());
    }

    @Override
    public String getUnderFSType() {
        return "bos";
    }

    @Override
    public PositionReader openPositionRead(String path, long fileLength) {
        return new BOSPositionReader(mClient, mBucketName, stripPrefixIfPresent(path), fileLength);
    }

    // No ACL integration currently, no-op
    @Override
    public void setOwner(String path, String user, String group) {
       LOG.error("setOwner: path: {} user:{} group: {}", path, user, group);
    }

    @Override
    public void setObjectTagging(String path, String name, String value) throws IOException {
        LOG.error("set ObjectTagging path: {} name:{} value: {}", path, name, value);
    }

    @Override
    public Map<String, String> getObjectTags(String path) throws IOException {
        LOG.error("getObjectTags path: {}", path);
        return new HashMap<>();
    }

    // No ACL integration currently, no-op
    @Override
    public void setMode(String path, short mode) throws IOException {
    }

    @Override
    protected boolean copyObject(String src, String dst) {
        LOG.debug("Copying {} to {}", src, dst);
        try {
            mClient.copyObject(mBucketName, src, mBucketName, dst);
            return true;
        } catch (BceClientException e) {
            LOG.error("Failed to rename file {} to {}", src, dst, e);
            return false;
        }
    }

    @Override
    public boolean createEmptyObject(String key) {
        try {
            ObjectMetadata objMeta = new ObjectMetadata();
            objMeta.setContentLength(0);
            mClient.putObject(mBucketName, key, new ByteArrayInputStream(new byte[0]), objMeta);
            return true;
        } catch (BceClientException e) {
            LOG.error("Failed to create object: {}", key, e);
            return false;
        }
    }

    @Override
    protected OutputStream createObject(String key) throws IOException {
        if (mUfsConf.getBoolean(PropertyKey.UNDERFS_BOS_STREAMING_UPLOAD_ENABLED)) {
            return new BOSLowLevelOutputStream(mBucketName, key, mClient,
                    mStreamingUploadExecutor.get(), mUfsConf);
        } else if (mUfsConf.getBoolean(PropertyKey.UNDERFS_BOS_MULTIPART_UPLOAD_ENABLED)) {
            return new BOSMultipartUploadOutputStream(mBucketName, key, mClient,
                    mMultipartUploadExecutor.get(), mUfsConf);
        }
        return new BOSOutputStream(mBucketName, key, mClient,
                mUfsConf.getList(PropertyKey.TMP_DIRS));
    }

    @Override
    protected boolean deleteObject(String key) {
        try {
            mClient.deleteObject(mBucketName, key);
        } catch (BceClientException e) {
            LOG.error("Failed to delete {}", key, e);
            return false;
        }
        return true;
    }

    @Override
    protected List<String> deleteObjects(List<String> keys) throws IOException {
        try {
            DeleteMultipleObjectsRequest request = new DeleteMultipleObjectsRequest();
            request.setBucketName(mBucketName);
            request.setObjectKeys(keys);

            DeleteMultipleObjectsResponse result = mClient.deleteMultipleObjects(request);

            if (!result.getErrors().isEmpty()) {
                throw new IOException(result.getErrors().toString());
            }

            return keys;
        } catch (BceClientException e) {
            throw new IOException("Failed to delete objects", e);
        }
    }

    @Override
    protected String getFolderSuffix() {
        return FOLDER_SUFFIX;
    }

    @Override
    protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
            throws IOException {
        String delimiter = recursive ? "" : PATH_SEPARATOR;
        key = PathUtils.normalizePath(key, PATH_SEPARATOR);
        // In case key is root (empty string) do not normalize prefix
        key = key.equals(PATH_SEPARATOR) ? "" : key;
        ListObjectsRequest request = new ListObjectsRequest(mBucketName);
        request.setPrefix(key);
        request.setMaxKeys(getListingChunkLength(mUfsConf));
        request.setDelimiter(delimiter);

        ListObjectsResponse result = getObjectListingChunk(request);
        if (result != null) {
            return new BOSObjectListingChunk(request, result);
        }
        return null;
    }

    // Get next chunk of listing result
    protected ListObjectsResponse getObjectListingChunk(ListObjectsRequest request) {
        ListObjectsResponse result;
        try {
            result = mClient.listObjects(request);
        } catch (BceClientException e) {
            LOG.error("Failed to list path {}", request.getPrefix(), e);
            result = null;
        }
        return result;
    }

    @Override
    protected ObjectStatus getObjectStatus(String key) {
        try {
            if (isRoot(key)) {
                // return a virtual root object
                return new ObjectStatus(key, null, 0, null);
            }
            ObjectMetadata meta = mClient.getObjectMetadata(mBucketName, key);
            if (meta == null) {
                return null;
            }
            Date lastModifiedDate = meta.getLastModified();
            Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
            return new ObjectStatus(key, meta.getETag(), meta.getContentLength(),
                    lastModifiedTime);
        } catch (BceClientException e) {
            return null;
        }
    }

    // No ACL integration currently, returns default empty value
    @Override
    protected ObjectPermissions getPermissions() {
        return mPermissions.get();
    }

    /**
     * Since there is no group in BOS, the owner is reused as the group. This method calls the
     * BOS API and requires additional permissions aside from just read only. This method is best
     * effort and will continue with default permissions (no owner, no group, 0700).
     *
     * @return the permissions associated with this under storage system
     */
    private ObjectPermissions getPermissionsInternal() {
        short bucketMode =
                ModeUtils.getUMask(mUfsConf.getString(PropertyKey.UNDERFS_BOS_DEFAULT_MODE)).toShort();
        String accountOwner = DEFAULT_OWNER;

        try {
            GetBucketAclResponse bucketInfo = mClient.getBucketAcl(mBucketName);
            Grantee owner = bucketInfo.getOwner();

            if (mUfsConf.isSet(PropertyKey.UNDERFS_BOS_OWNER_ID_TO_USERNAME_MAPPING)) {
                // Here accountOwner can be null if there is no mapping set for this owner id
                accountOwner = CommonUtils.getValueFromStaticMapping(
                        mUfsConf.getString(PropertyKey.UNDERFS_BOS_OWNER_ID_TO_USERNAME_MAPPING),
                        owner.getId());
            }
            if (accountOwner == null || accountOwner.equals(DEFAULT_OWNER)) {
                // If there is no user-defined mapping, use display name or id.
                accountOwner = owner.toString() != null ? owner.toString() : owner.getId();
            }
        } catch (BceClientException e) {
            LOG.warn("Failed to get bucket owner, proceeding with defaults. {}", e.toString());
        }
        return new ObjectPermissions(accountOwner, accountOwner, bucketMode);
    }

    @Override
    protected String getRootKey() {
        return Constants.HEADER_BOS + mBucketName;
    }

    @Override
    protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy)
            throws IOException {
        try {
            return new BOSInputStream(mBucketName, key, mClient, options.getOffset(), retryPolicy,
                    mUfsConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
        } catch (BceClientException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        mClientProvider.close();
    }

    /**
     * Wrapper over BOS {@link ObjectListingChunk}.
     */
    private final class BOSObjectListingChunk implements ObjectListingChunk {
        final ListObjectsRequest mRequest;
        final ListObjectsResponse mResult;

        BOSObjectListingChunk(ListObjectsRequest request, ListObjectsResponse result) throws IOException {
            mRequest = request;
            mResult = result;
            if (mResult == null) {
                throw new IOException("BOS listing result is null");
            }
        }

        @Override
        public ObjectStatus[] getObjectStatuses() {
            List<BosObjectSummary> objects = mResult.getContents();
            ObjectStatus[] ret = new ObjectStatus[objects.size()];
            int i = 0;
            for (BosObjectSummary obj : objects) {
                Date lastModifiedDate = obj.getLastModified();
                Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
                ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
                        lastModifiedTime);
            }
            return ret;
        }

        @Override
        public String[] getCommonPrefixes() {
            List<String> res = mResult.getCommonPrefixes();
            if (null == res) {
                return new String[0];
            }
            return res.toArray(new String[0]);
        }

        @Override
        public ObjectListingChunk getNextChunk() throws IOException {
            if (mResult.isTruncated()) {
                mRequest.setMarker(mResult.getNextMarker());
                ListObjectsResponse nextResult = mClient.listObjects(mRequest);
                if (nextResult != null) {
                    return new BOSObjectListingChunk(mRequest, nextResult);
                }
            }
            return null;
        }

        @Override
        public Boolean hasNextChunk() {
            return mResult.isTruncated();
        }
    }
}
