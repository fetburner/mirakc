use std::task::Context;
use std::task::Poll;

use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::Request;
use axum::http::header::CACHE_CONTROL;
use axum::response::Response;
use futures::future::BoxFuture;
use tower::Layer;
use tower::Service;

#[derive(Clone)]
pub(super) struct DefaultHeadersLayer(HeaderMap);

impl DefaultHeadersLayer {
    pub(super) fn new(headers: HeaderMap) -> Self {
        DefaultHeadersLayer(headers)
    }
}

impl<S> Layer<S> for DefaultHeadersLayer {
    type Service = DefaultHeadersService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DefaultHeadersService {
            inner,
            headers: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct DefaultHeadersService<S> {
    inner: S,
    headers: HeaderMap,
}

impl<S> Service<Request<Body>> for DefaultHeadersService<S>
where
    S: Service<Request<Body>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    // `BoxFuture` is a type alias for `Pin<Box<dyn Future + Send + 'a>>`
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let headers = self.headers.clone();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let mut res = fut.await?;
            for (name, value) in headers.iter() {
                let should_insert = if name == CACHE_CONTROL {
                    // Keep the default no-store policy for SSE responses, which set no-cache,
                    // while allowing endpoints to opt into a more specific cache policy.
                    res.headers()
                        .get(name)
                        .map(|value| value == "no-cache")
                        .unwrap_or(true)
                } else {
                    !res.headers().contains_key(name)
                };
                if should_insert {
                    res.headers_mut().insert(name, value.clone());
                }
            }
            Ok(res)
        })
    }
}
