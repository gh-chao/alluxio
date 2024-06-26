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

import alluxio.retry.RetryPolicy;
import alluxio.underfs.MultiRangeObjectInputStream;
import com.baidubce.BceClientException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A stream for reading a file from BOS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class BOSInputStream extends MultiRangeObjectInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(BOSInputStream.class);

    /**
     * Bucket name of the Alluxio BOS bucket.
     */
    private final String mBucketName;

    /**
     * Key of the file in BOS to read.
     */
    private final String mKey;

    /**
     * The BOS client for BOS operations.
     */
    private final BosClient mBosClient;

    /**
     * The size of the object in bytes.
     */
    private final long mContentLength;

    /**
     * Policy determining the retry behavior in case the key does not exist. The key may not exist
     * because of eventual consistency.
     */
    private final RetryPolicy mRetryPolicy;

    /**
     * Creates a new instance of {@link BOSInputStream}.
     *
     * @param bucketName          the name of the bucket
     * @param key                 the key of the file
     * @param client              the client for BOS
     * @param retryPolicy         retry policy in case the key does not exist
     * @param multiRangeChunkSize the chunk size to use on this stream
     */
    BOSInputStream(String bucketName, String key, BosClient client, RetryPolicy retryPolicy,
                   long multiRangeChunkSize) throws IOException {
        this(bucketName, key, client, 0L, retryPolicy, multiRangeChunkSize);
    }

    /**
     * Creates a new instance of {@link BOSInputStream}.
     *
     * @param bucketName          the name of the bucket
     * @param key                 the key of the file
     * @param client              the client for BOS
     * @param position            the position to begin reading from
     * @param retryPolicy         retry policy in case the key does not exist
     * @param multiRangeChunkSize the chunk size to use on this stream
     */
    BOSInputStream(String bucketName, String key, BosClient client, long position,
                   RetryPolicy retryPolicy, long multiRangeChunkSize) throws IOException {
        super(multiRangeChunkSize);
        mBucketName = bucketName;
        mKey = key;
        mBosClient = client;
        mPos = position;
        ObjectMetadata meta = mBosClient.getObjectMetadata(mBucketName, key);
        mContentLength = meta == null ? 0 : meta.getContentLength();
        mRetryPolicy = retryPolicy;
    }

    @Override
    protected InputStream createStream(long startPos, long endPos) throws IOException {

        GetObjectRequest req = new GetObjectRequest(mBucketName, mKey);

        // BOS returns entire object if we read past the end
        req.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);

        BceClientException lastException = null;

        String errorMessage = String.format("Failed to open key: %s bucket: %s", mKey, mBucketName);

        while (mRetryPolicy.attempt()) {
            try {
                BosObject bosObject = mBosClient.getObject(req);
                return new BufferedInputStream(bosObject.getObjectContent());
            } catch (BceClientException e) {
                errorMessage = String
                        .format("Failed to open key: %s bucket: %s attempts: %d error: %s", mKey, mBucketName,
                                mRetryPolicy.getAttemptCount(), e.getMessage());
                lastException = e;
            }
        }
        // Failed after retrying key does not exist
        throw new IOException(errorMessage, lastException);
    }
}
