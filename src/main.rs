use bytes::Bytes;
use hex::encode;
use md5::{Digest, Md5};
use std::error::Error;
async fn get_object_from_range(range_string: String, client: &aws_sdk_s3::Client) -> Result<Bytes> {
    return client
        .get_object()
        .bucket("s3-md5-bucket")
        .key("test.jpg")
        .range(range_string)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // create an aws s3 client
    let client = aws_sdk_s3::Client::new(&aws_config::load_from_env().await);
    // get an object from the bucket
    let resp = client
        .head_object()
        .bucket("s3-md5-bucket")
        .key("test.jpg")
        .send()
        .await?;
    let object_size = resp.content_length();
    println!("object size {:?} bytes", object_size);
    let mut hasher = Md5::new();

    let chunk_size_bytes = 1000000;

    let chunk_count = object_size / chunk_size_bytes;
    println!("chunk count: {}", chunk_count);

    let mut store: Vec<Bytes> = Vec::new();

    for part_number in 0..chunk_count {
        let start = part_number * chunk_size_bytes;
        let end = if part_number + 1 == chunk_count {
            object_size
        } else {
            ((part_number * chunk_size_bytes) + chunk_size_bytes) - 1
        };

        println!("range: {}-{}", start, end);

        let index: usize = part_number.try_into().unwrap();

        store.insert(index, body);
    }

    for chunk in store {
        hasher.update(chunk);
    }

    let hash = hasher.finalize();
    let hash_str = encode(hash);
    println!("hash: {}", hash_str);
    Ok(())
}
