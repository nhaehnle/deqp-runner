use std::io::prelude::*;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::fs::File;
use std::path::Path;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Return an iterator that produces the items from `iterator`, inserting
/// clones of `item` in between each one.
pub fn intersperse<I, T>(mut iterator: I, item: T) -> impl Iterator<Item=T>
    where I: Iterator<Item=T>,
          T: Clone
{
    enum State<T> {
        Iter(T),
        Intersperse(T),
        End,
    }
    struct Iter<I, T> {
        iterator: I,
        item: T,
        state: State<T>,
    }
    impl<I, T> Iterator for Iter<I, T>
        where I: Iterator<Item=T>,
              T: Clone
    {
        type Item = T;

        fn next(&mut self) -> Option<Self::Item> {
            let result;
            (result, self.state) = match std::mem::replace(&mut self.state, State::End) {
            State::Iter(t) => (Some(t), match self.iterator.next() {
                Some(t) => State::Intersperse(t),
                None => State::End,
                }),
            State::Intersperse(t) => (Some(self.item.clone()), State::Iter(t)),
            State::End => (None, State::End),
            };
            result
        }
    }

    let state = match iterator.next() {
        Some(t) => State::Iter(t),
        None => State::End,
        };
    Iter {
        iterator,
        item,
        state,
    }
}

/// Run `f` and prefix any errors with the string returned by `prefix`.
pub fn try_forward<'a, F, R, C, S>(f: F, prefix: C) -> Result<R>
    where F: FnOnce() -> Result<R>,
          C: 'a + Fn() -> S,
          S: Into<String>,
{
    #[derive(Debug)]
    struct WrappedError {
        prefix: String,
        cause: Box<dyn std::error::Error>,
    }
    impl Display for WrappedError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}: {}", self.prefix, self.cause)
        }
    }
    impl Error for WrappedError {}

    match f() {
    Err(err) => Err(Box::new(WrappedError {
        prefix: prefix().into(),
        cause: err,
    })),
    Ok(result) => Ok(result)
    }
}

pub fn error<T: Debug + Display>(t: T) -> impl Error {
    #[derive(Debug)]
    struct WrappedError<T>(T);
    impl<T: Display> std::fmt::Display for WrappedError<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }
    impl<T: Debug + Display> Error for WrappedError<T> {}

    WrappedError(t)
}

fn read_bytes_impl(path: &Path) -> Result<Vec<u8>> {
    try_forward(|| -> Result<Vec<u8>> {
        let mut file = File::open(path)?;
        let mut buffer: Vec<u8> = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }, || path.display().to_string())
}

pub fn read_bytes<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    read_bytes_impl(path.as_ref())
}
