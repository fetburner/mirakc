use super::*;

use std::time::Duration as StdDuration;
use std::time::SystemTime;

use axum::http::HeaderMap;
use axum::http::header::IF_NONE_MATCH;
use axum_extra::headers::CacheControl;
use axum_extra::headers::ETag;
use axum_extra::headers::HeaderMapExt;
use axum_extra::headers::IfModifiedSince;
use axum_extra::headers::IfNoneMatch;
use axum_extra::headers::IfRange;
use axum_extra::headers::LastModified;

use crate::recording::Record;
use crate::web::api::stream::StreamingHeaderParams;
use crate::web::api::stream::compute_content_length;
use crate::web::api::stream::compute_content_range;
use crate::web::api::stream::do_head_stream_with_headers;
use crate::web::api::stream::streaming_with_headers;

/// Gets a media stream of the content of a record.
///
/// It's possible to get a media stream of the record even while it's recording.  In this case, data
/// will be sent when data is appended to the content file event if the stream reaches EOF at that
/// point.  The streaming will stop within 2 seconds after the stream reaches the *true* EOF.
///
/// A request for a record without content file always returns status code 204.
///
/// A range request with filters always causes an error response with status code 400.
#[allow(clippy::too_many_arguments)]
#[utoipa::path(
    get,
    path = "/recording/records/{id}/stream",
    params(
        ("id" = String, Path, description = "Record ID"),
        ("pre-filters" = Option<[String]>, Query, description = "pre-filters"),
        ("post-filters" = Option<[String]>, Query, description = "post-filters"),
    ),
    responses(
        (status = 200, description = "OK"),
        (status = 204, description = "No Content"),
        (status = 400, description = "Bad Request"),
        (status = 404, description = "Not Found"),
        (status = 500, description = "Internal Server Error"),
    ),
    operation_id = "getRecordStream",
)]
pub(in crate::web::api) async fn get<R, W>(
    State(RecordingManagerExtractor(recording_manager)): State<RecordingManagerExtractor<R>>,
    State(ConfigExtractor(config)): State<ConfigExtractor>,
    State(SpawnerExtractor(spawner)): State<SpawnerExtractor<W>>,
    Path(id): Path<RecordId>,
    request_headers: HeaderMap,
    ranges: Option<TypedHeader<axum_extra::headers::Range>>,
    if_none_match: Option<TypedHeader<IfNoneMatch>>,
    if_modified_since: Option<TypedHeader<IfModifiedSince>>,
    if_range: Option<TypedHeader<IfRange>>,
    user: TunerUser,
    Qs(filter_setting): Qs<FilterSetting>,
) -> Result<Response, Error>
where
    R: Call<recording::OpenContent>,
    R: Call<recording::QueryRecord>,
    W: Spawn,
{
    let (record, content_length) = recording_manager
        .call(recording::QueryRecord { id: id.clone() })
        .await??;

    let content_length = match content_length {
        Some(content_length) if content_length > 0 => content_length,
        _ => return Err(Error::NoContent),
    };

    let (filters, content_type, seekable) = build_filters(&config, &filter_setting, &record)?;
    let validator = build_validator(&record, filters.is_empty());
    let last_modified = build_last_modified(&record);
    let cache_headers = build_cache_headers(&record, validator.as_ref(), last_modified.as_ref());

    if is_not_modified(
        validator.as_ref(),
        last_modified.as_ref(),
        &if_none_match,
        &if_modified_since,
        request_headers.contains_key(IF_NONE_MATCH),
    ) {
        return Ok((StatusCode::NOT_MODIFIED, cache_headers).into_response());
    }

    let incomplete = matches!(record.recording_status, RecordingStatus::Recording);
    let range = match (&if_range, &validator) {
        (Some(TypedHeader(if_range)), _)
            if if_range.is_modified(validator.as_ref(), last_modified.as_ref()) =>
        {
            None
        }
        _ => compute_content_range(&ranges, content_length, incomplete, seekable)?,
    };
    let length = compute_content_length(content_length, incomplete, range.as_ref());

    let params = StreamingHeaderParams {
        seekable,
        content_type,
        length,
        range,
        user,
    };

    let (stream, stop_trigger) = recording_manager
        .call(recording::OpenContent::new(
            id.clone(),
            params.range.clone(),
        ))
        .await??;

    streaming_with_headers(
        &config,
        &spawner,
        stream,
        filters,
        &params,
        stop_trigger,
        &cache_headers,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[utoipa::path(
    head,
    path = "/recording/records/{id}/stream",
    params(
        ("id" = String, Path, description = "Record ID"),
        ("pre-filters" = Option<[String]>, Query, description = "pre-filters"),
        ("post-filters" = Option<[String]>, Query, description = "post-filters"),
    ),
    responses(
        (status = 200, description = "OK"),
        (status = 204, description = "No Content"),
        (status = 404, description = "Not Found"),
        (status = 500, description = "Internal Server Error"),
    ),
    operation_id = "checkRecordStream",
)]
pub(in crate::web::api) async fn head<R>(
    State(RecordingManagerExtractor(recording_manager)): State<RecordingManagerExtractor<R>>,
    State(ConfigExtractor(config)): State<ConfigExtractor>,
    Path(id): Path<RecordId>,
    request_headers: HeaderMap,
    ranges: Option<TypedHeader<axum_extra::headers::Range>>,
    if_none_match: Option<TypedHeader<IfNoneMatch>>,
    if_modified_since: Option<TypedHeader<IfModifiedSince>>,
    if_range: Option<TypedHeader<IfRange>>,
    user: TunerUser,
    Qs(filter_setting): Qs<FilterSetting>,
) -> Result<Response, Error>
where
    R: Call<recording::QueryRecord>,
{
    let (record, content_length) = recording_manager
        .call(recording::QueryRecord { id: id.clone() })
        .await??;

    let content_length = match content_length {
        Some(content_length) if content_length > 0 => content_length,
        _ => return Err(Error::NoContent),
    };

    let (filters, content_type, seekable) = build_filters(&config, &filter_setting, &record)?;
    let validator = build_validator(&record, filters.is_empty());
    let last_modified = build_last_modified(&record);
    let cache_headers = build_cache_headers(&record, validator.as_ref(), last_modified.as_ref());

    if is_not_modified(
        validator.as_ref(),
        last_modified.as_ref(),
        &if_none_match,
        &if_modified_since,
        request_headers.contains_key(IF_NONE_MATCH),
    ) {
        return Ok((StatusCode::NOT_MODIFIED, cache_headers).into_response());
    }

    let incomplete = matches!(record.recording_status, RecordingStatus::Recording);
    let range = match (&if_range, &validator) {
        (Some(TypedHeader(if_range)), _)
            if if_range.is_modified(validator.as_ref(), last_modified.as_ref()) =>
        {
            None
        }
        _ => compute_content_range(&ranges, content_length, incomplete, seekable)?,
    };
    let length = compute_content_length(content_length, incomplete, range.as_ref());

    let params = StreamingHeaderParams {
        seekable,
        content_type,
        length,
        range,
        user,
    };

    do_head_stream_with_headers(&params, &cache_headers)
}

fn build_validator(record: &Record, filters_are_empty: bool) -> Option<ETag> {
    record
        .content_sha256
        .as_deref()
        .filter(|_| filters_are_empty)
        .and_then(|hash| format!("\"{hash}\"").parse().ok())
}

fn build_last_modified(record: &Record) -> Option<LastModified> {
    record
        .recording_end_time
        .map(|time| LastModified::from(SystemTime::from(time)))
}

fn build_cache_headers(
    record: &Record,
    validator: Option<&ETag>,
    last_modified: Option<&LastModified>,
) -> HeaderMap {
    let mut headers = HeaderMap::new();

    if let Some(validator) = validator {
        headers.typed_insert(validator.clone());
    }
    if let Some(last_modified) = last_modified {
        headers.typed_insert(*last_modified);
    }

    let cache_control = if matches!(record.recording_status, RecordingStatus::Recording) {
        CacheControl::new().with_no_store()
    } else {
        CacheControl::new()
            .with_private()
            .with_max_age(StdDuration::from_secs(365 * 24 * 60 * 60))
            .with_immutable()
    };
    headers.typed_insert(cache_control);

    headers
}

fn is_not_modified(
    validator: Option<&ETag>,
    last_modified: Option<&LastModified>,
    if_none_match: &Option<TypedHeader<IfNoneMatch>>,
    if_modified_since: &Option<TypedHeader<IfModifiedSince>>,
    if_none_match_present: bool,
) -> bool {
    if if_none_match_present {
        let Some(TypedHeader(if_none_match)) = if_none_match else {
            return false;
        };
        return validator
            .map(|validator| !if_none_match.precondition_passes(validator))
            .unwrap_or(false);
    }

    match (if_modified_since, last_modified) {
        (Some(TypedHeader(if_modified_since)), Some(last_modified)) => {
            !if_modified_since.is_modified(SystemTime::from(*last_modified))
        }
        _ => false,
    }
}

fn build_filters(
    config: &Config,
    filter_setting: &FilterSetting,
    record: &Record,
) -> Result<(Vec<String>, String, bool), Error> {
    let video_tags: Vec<u8> = record
        .program
        .video
        .iter()
        .map(|video| video.component_tag)
        .collect();

    let audio_tags: Vec<u8> = record
        .program
        .audios
        .values()
        .map(|audio| audio.component_tag)
        .collect();

    let data = mustache::MapBuilder::new()
        .insert_str("channel_name", &record.service.channel.name)
        .insert("channel_type", &record.service.channel.channel_type)?
        .insert_str("channel", &record.service.channel.channel)
        .insert("sid", &record.program.id.sid().value())?
        .insert("eid", &record.program.id.eid().value())?
        .insert("video_tags", &video_tags)?
        .insert("audio_tags", &audio_tags)?
        .insert("id", &record.id.value())?
        .build();

    let mut builder = FilterPipelineBuilder::new(data, true); // seekable by default
    builder.add_pre_filters(&config.pre_filters, &filter_setting.pre_filters)?;
    // The stream has already been decoded.
    builder.add_post_filters(&config.post_filters, &filter_setting.post_filters)?;
    Ok(builder.build())
}
