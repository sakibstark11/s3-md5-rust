use bytes::Bytes;
use hex::encode;
use md5::{Digest, Md5};
use std::error::Error;
use std::time::Instant;

async fn get_object_from_range(
    range_string: &String,
    client: &aws_sdk_s3::Client,
) -> Result<Bytes, String> {
    println!("range: {} downloading", range_string);

    let object = client
        .get_object()
        .bucket("s3-md5-bucket")
        .key("test.jpg")
        .range(range_string)
        .send()
        .await;

    println!("range: {} downloaded", range_string);

    match object {
        Ok(object) => {
            let object_body = object.body.collect().await;
            match object_body {
                Ok(object_body) => Ok(object_body.into_bytes()),
                Err(e) => Err(e.to_string()),
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

async fn get_object_size(client: &aws_sdk_s3::Client) -> Result<i64, String> {
    let object = client
        .head_object()
        .bucket("s3-md5-bucket")
        .key("test.jpg")
        .send()
        .await;

    match object {
        Ok(object) => Ok(object.content_length()),
        Err(e) => Err(e.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let client = aws_sdk_s3::Client::new(&aws_config::load_from_env().await);

    let object_size = get_object_size(&client).await.unwrap();

    println!("object size {:?} bytes", object_size);
    let mut hasher = Md5::new();

    let chunk_size_bytes = 1000000;

    let chunk_count = object_size / chunk_size_bytes;
    println!("chunk count: {}", chunk_count);

    let mut process_store: Vec<tokio::task::JoinHandle<Result<Bytes, String>>> = Vec::new();

    for part_number in 0..chunk_count {
        let start = part_number * chunk_size_bytes;
        let end = if part_number + 1 == chunk_count {
            object_size
        } else {
            ((part_number * chunk_size_bytes) + chunk_size_bytes) - 1
        };

        let range_string = format!("bytes={}-{}", start, end);
        let cloned_client = client.clone();
        let handle: tokio::task::JoinHandle<Result<Bytes, String>> =
            tokio::spawn(async move { get_object_from_range(&range_string, &cloned_client).await });
        process_store.insert(part_number.try_into().unwrap(), handle);
    }

    for handle in process_store {
        let result = handle.await.unwrap().unwrap();
        hasher.update(result);
    }
    let hash = hasher.finalize();
    let hash_str = encode(hash);
    println!("hash: {}", hash_str);
    let duration = start.elapsed();

    println!("took: {:?} seconds", duration);
    Ok(())
}
