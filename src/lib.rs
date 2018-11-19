use futures::{Future, Poll};
use std::ops::Drop;

/// Inspect when a Future gets dropped
pub struct InspectDrop<F> {
    inner: F,
    id: String,
}

pub fn inspect_drop<F: Future>(inner: F, id: String) -> InspectDrop<F> {
    InspectDrop { inner, id }
}

impl<F: Future> Future for InspectDrop<F> {
    type Item = F::Item;
    type Error = F::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

impl<F> Drop for InspectDrop<F> {
    fn drop(&mut self) {
        println!("Dropping {}", self.id)
    }
}
