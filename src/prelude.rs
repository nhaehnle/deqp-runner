use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct UnwrapOrNever<F> {
    future: Option<F>,
}
impl<F> UnwrapOrNever<F> {
    fn pin_get_future(self: Pin<&mut Self>) -> Pin<&mut Option<F>> {
        // This is the pattern for obtaining pin-of-field from pin-of-struct
        // that is described as (conditionally) safe in the documentation of
        // `std::pin`.
        unsafe { self.map_unchecked_mut(|s| &mut s.future) }
    }
}

impl<T, F: Future<Output=T>> Future for UnwrapOrNever<F> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.pin_get_future().as_pin_mut() {
        Some(f) => f.poll(cx),
        None => Poll::Pending,
        }
    }
}

pub trait OptionFutureExt<F> {
    /// Given a value of type `Option<Future<Output=T>>`, produce a
    /// `Future<Output=T>` that delegates to the future if one is given,
    /// or that never produces a result (equivalent to `pending`).
    fn unwrap_or_never(self) -> UnwrapOrNever<F>;
}

impl<T, F: Future<Output=T>> OptionFutureExt<F> for Option<F> {
    fn unwrap_or_never(self) -> UnwrapOrNever<F> {
        UnwrapOrNever {
            future: self,
        }
    }
}

// Replace by trim_ascii_{start,end} once stabilized.
pub trait ByteSliceExt {
    fn trim_whitespace_start(&self) -> &[u8];
    fn trim_whitespace_end(&self) -> &[u8];
}
impl ByteSliceExt for [u8] {
    fn trim_whitespace_start(&self) -> &[u8] {
        let count = self.iter().take_while(|b| b.is_ascii_whitespace()).count();
        &self[count..]
    }

    fn trim_whitespace_end(&self) -> &[u8] {
        let count = self.iter().rev().take_while(|b| b.is_ascii_whitespace())
                        .count();
        &self[..self.len() - count]
    }
}
