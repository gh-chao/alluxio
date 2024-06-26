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

import alluxio.underfs.ObjectPositionReader;
import com.baidubce.BceClientException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.GetObjectRequest;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of {@link ObjectPositionReader} that reads from BOS object store.
 */
@ThreadSafe
public class BOSPositionReader extends ObjectPositionReader {

    /**
     * Client for operations with Aliyun BOS.
     */
    protected final BosClient mClient;

    /**
     * @param client     the Aliyun BOS client
     * @param bucketName the bucket name
     * @param path       the file path
     * @param fileLength the file length
     */
    public BOSPositionReader(BosClient client, String bucketName, String path, long fileLength) {
        // TODO(lu) path needs to be transformed to not include bucket
        super(bucketName, path, fileLength);
        mClient = client;
    }

    @Override
    protected InputStream openObjectInputStream(
            long position, int bytesToRead) throws IOException {
        BosObject object;
        try {
            GetObjectRequest getObjectRequest = new GetObjectRequest(mBucketName, mPath);
            getObjectRequest.setRange(position, position + bytesToRead - 1);
            object = mClient.getObject(getObjectRequest);
        } catch (BceClientException e) {
            String errorMessage = String
                    .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
            throw new IOException(errorMessage, e);
        }
        return object.getObjectContent();
    }
}
