use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::StreamExt;
use risingwave_storage::object::{ObjectStore, S3ObjectStore};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::main]
async fn main() {
    let object_store = Arc::new(S3ObjectStore::new("s3-ut".to_string()).await);
    let start = SystemTime::now();
    let since = start.duration_since(UNIX_EPOCH).unwrap();
    let path = "stream-upload-test".to_string() + since.as_secs().to_string().as_str();
    println!("path is {}", path);
    let data = Bytes::from("hello world".as_bytes());
    {
        // upload with stream
        let (tx, rx) = channel(16);
        let join_handle = tokio::spawn({
            let object_store = object_store.clone();
            let path = path.clone();
            async move {
                object_store
                    .upload_stream(&path, ReceiverStream::new(rx).boxed())
                    .await
            }
        });
        tx.send(data.clone()).await.unwrap();
        drop(tx);
        join_handle.await.unwrap().unwrap();
    }
    {
        // upload the whole object
        // object_store.upload(&path, data.clone()).await.unwrap();
    }
    let read_result = object_store.read(&path, None).await.unwrap();
    assert_eq!(data, read_result);
    println!("read result: {:?}", read_result);
}
