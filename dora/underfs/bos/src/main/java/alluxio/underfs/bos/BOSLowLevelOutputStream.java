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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.ObjectLowLevelOutputStream;
import com.baidubce.BceClientException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.*;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link ObjectLowLevelOutputStream} implement for BOS.
 */
public class BOSLowLevelOutputStream extends ObjectLowLevelOutputStream {
    /**
     * The BOS client to interact with BOS.
     */
    private final BosClient mClient;
    /**
     * Tags for the uploaded part, provided by BOS after uploading.
     */
    private final List<PartETag> mTags =
            Collections.synchronizedList(new ArrayList<>());

    /**
     * The upload id of this multipart upload.
     */
    protected volatile String mUploadId;

    private String mContentHash;

    /**
     * Constructs a new stream for writing a file.
     *
     * @param bucketName the name of the bucket
     * @param key        the key of the file
     * @param bos        the BOS client to upload the file with
     * @param executor   a thread pool executor
     * @param ufsConf    the object store under file system configuration
     */
    public BOSLowLevelOutputStream(
            String bucketName,
            String key,
            BosClient bos,
            ListeningExecutorService executor,
            AlluxioConfiguration ufsConf) {
        super(bucketName, key, executor,
                ufsConf.getBytes(PropertyKey.UNDERFS_BOS_STREAMING_UPLOAD_PARTITION_SIZE), ufsConf);
        mClient = bos;
    }

    @Override
    protected void abortMultiPartUploadInternal() throws IOException {
        try {
            getClient().abortMultipartUpload(new AbortMultipartUploadRequest(mBucketName,
                    mKey, mUploadId));
        } catch (BceClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void uploadPartInternal(File file, int partNumber, boolean isLastPart, String md5)
            throws IOException {
        try {
            try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
                final UploadPartRequest uploadRequest =
                        new UploadPartRequest(mBucketName, mKey, mUploadId, partNumber, file.length(), inputStream);
                if (md5 != null) {
                    uploadRequest.setMd5Digest(md5);
                }
                PartETag partETag = getClient().uploadPart(uploadRequest).getPartETag();
                mTags.add(partETag);
            }
        } catch (BceClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void initMultiPartUploadInternal() throws IOException {
        try {
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(mBucketName, mKey);
            mUploadId = getClient().initiateMultipartUpload(initRequest).getUploadId();
        } catch (BceClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void completeMultiPartUploadInternal() throws IOException {
        try {
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    mBucketName, mKey, mUploadId, mTags);
            mContentHash = getClient().completeMultipartUpload(completeRequest).getETag();
        } catch (BceClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void createEmptyObject(String key) throws IOException {
        try {
            ObjectMetadata objMeta = new ObjectMetadata();
            objMeta.setContentLength(0);
            mContentHash = getClient().putObject(mBucketName, key,
                    new ByteArrayInputStream(new byte[0]), objMeta).getETag();
        } catch (BceClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void putObject(String key, File file, String md5) throws IOException {
        try {
            ObjectMetadata objMeta = new ObjectMetadata();
            if (md5 != null) {
                objMeta.setContentMd5(md5);
            }
            PutObjectRequest request = new PutObjectRequest(mBucketName, key, file, objMeta);
            mContentHash = getClient().putObject(request).getETag();
        } catch (BceClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Optional<String> getContentHash() {
        return Optional.ofNullable(mContentHash);
    }

    protected BosClient getClient() {
        return mClient;
    }
}
