#![allow(
    clippy::cast_possible_truncation,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "Fixture helpers build small known batches and fail loudly on fixture errors."
)]
use super::*;
use futures::TryStreamExt;

pub(super) async fn put(
    fixture: &Fixture,
    input: &str,
    output: &str,
    bytes: &[u8],
) -> Result<(&'static str, Vec<u8>), Error> {
    let mut frames = frames::<wire::PutObjectsRequest>(input, bytes)?.into_iter();
    let Some(wire::PutObjectsRequest {
        frame: Some(wire::put_objects_request::Frame::Header(header)),
    }) = frames.next()
    else {
        return Err(invalid());
    };
    let mut parser = crate::request::BatchUploadFraming::new(&header)?;
    let mut bodies = vec![Vec::new(); header.objects.len()];
    for frame in frames {
        parser.accept(&frame)?;
        if let Some(wire::put_objects_request::Frame::Chunk(wire::PutObjectsChunk {
            index,
            frame: Some(wire::put_objects_chunk::Frame::Body(body)),
        })) = frame.frame
        {
            bodies[index as usize].extend(body);
        }
    }
    parser.finish()?;
    let results = fixture
        .objects
        .put_batch(
            header
                .objects
                .into_iter()
                .zip(bodies.into_iter().map(Into::into))
                .collect(),
        )
        .await;
    let result = wire::PutObjectsResponse {
        results: results
            .into_iter()
            .map(|result| wire::PutObjectResult {
                result: Some(match result {
                    Ok(object) => wire::put_object_result::Result::Object(object),
                    Err(error) => wire::put_object_result::Result::Error(wire::ErrorDetail {
                        code: error.code as i32,
                        request_id: String::new(),
                    }),
                }),
            })
            .collect(),
    };
    Ok(("application/json", json::encode(output, &result)?))
}

pub(super) async fn get(
    fixture: &Fixture,
    input: &str,
    output: &str,
    bytes: &[u8],
) -> Result<(&'static str, Vec<u8>), Error> {
    let query: wire::GetObjectsRequest = json::decode(input, bytes, 128 * 1024)?;
    crate::request::get_batch(&query)?;
    let requests = query
        .objects
        .into_iter()
        .map(|selection| (selection.request.unwrap(), selection.maximum_bytes))
        .collect();
    let mut bytes = Vec::new();
    for (index, result) in fixture
        .objects
        .get_batch(requests)
        .await
        .into_iter()
        .enumerate()
    {
        let mut emit = |frame| -> Result<(), Error> {
            bytes.extend(framed(
                output,
                &wire::GetObjectsResponse {
                    index: index as u32,
                    frame: Some(frame),
                },
            )?);
            Ok(())
        };
        use wire::get_objects_response::Frame;
        match result {
            Ok(object) => {
                emit(Frame::Header(object.header().clone()))?;
                for chunk in object.body().chunks(65_536) {
                    emit(Frame::Body(wire::Body {
                        codec: wire::Codec::None as i32,
                        data: chunk.to_vec(),
                        decoded_length: chunk.len() as u64,
                    }))?;
                }
                emit(Frame::Complete(true))?;
            }
            Err(error) => emit(Frame::Error(wire::ErrorDetail {
                code: error.code as i32,
                request_id: String::new(),
            }))?,
        }
    }
    Ok(("application/x-ndjson", bytes))
}

pub(super) async fn exercise(client: &HttpObjects) -> Result<(), Error> {
    use wire::{put_objects_chunk::Frame as Chunk, put_objects_request::Frame as Upload};
    client
        .create_bucket(wire::CreateBucketRequest {
            name: "batch-http".into(),
            mutation: None,
        })
        .await?;
    let bucket = Some(wire::BucketRef {
        name: "batch-http".into(),
    });
    let objects = ["large", "empty"]
        .map(|key| wire::PutObjectHeader {
            bucket: bucket.clone(),
            object_key: key.into(),
            ..Default::default()
        })
        .to_vec();
    let mut frames = vec![wire::PutObjectsRequest {
        frame: Some(Upload::Header(wire::PutObjectsHeader { objects })),
    }];
    for chunk in vec![42; 131_073].chunks(65_536) {
        frames.push(wire::PutObjectsRequest {
            frame: Some(Upload::Chunk(wire::PutObjectsChunk {
                index: 0,
                frame: Some(Chunk::Body(chunk.to_vec())),
            })),
        });
    }
    for index in 0..2 {
        frames.push(wire::PutObjectsRequest {
            frame: Some(Upload::Chunk(wire::PutObjectsChunk {
                index,
                frame: Some(Chunk::Complete(true)),
            })),
        });
    }
    frames.push(wire::PutObjectsRequest {
        frame: Some(Upload::Complete(true)),
    });
    let response = client.put_objects(stream::iter(frames)).await?;
    assert_eq!(response.results.len(), 2);
    assert!(response.results.iter().all(|result| matches!(
        result.result,
        Some(wire::put_object_result::Result::Object(_))
    )));
    let query = wire::GetObjectsRequest {
        objects: ["large", "missing", "empty"]
            .map(|key| wire::GetObjectSelection {
                request: Some(wire::GetObjectRequest {
                    bucket: bucket.clone(),
                    object_key: key.into(),
                    ..Default::default()
                }),
                maximum_bytes: 131_073,
            })
            .to_vec(),
    };
    let frames: Vec<_> = client.get_objects(query).await?.try_collect().await?;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(
                frame.frame,
                Some(wire::get_objects_response::Frame::Complete(true))
            ))
            .count(),
        2
    );
    assert!(frames.iter().any(|frame| frame.index == 1 && matches!(&frame.frame, Some(wire::get_objects_response::Frame::Error(error)) if error.code == wire::ErrorCode::NotFound as i32)));
    let body: Vec<_> = frames
        .into_iter()
        .filter_map(|frame| match frame.frame {
            Some(wire::get_objects_response::Frame::Body(body)) => Some(body.data),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(body, vec![42; 131_073]);
    Ok(())
}

#[tokio::test]
async fn batch_http_bindings_round_trip_fragmented_frames_and_individual_errors()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = start().await?;
    let client = HttpObjects::new(&server.endpoint, "exact-token", 2 * 1024 * 1024, None)?;
    exercise(&client).await?;
    let _ = server.shutdown.send(());
    server.task.await??;
    Ok(())
}
