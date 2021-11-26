// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::result::Result;

#[macro_export]
macro_rules! make_err {
    ($code:expr, $($arg:tt)+) => {{
        $crate::Error::new(
            $code,
            format!("{}", format_args!($($arg)+)),
        )
    }};
}

#[macro_export]
macro_rules! make_input_err {
    ($($arg:tt)+) => {{
        $crate::make_err!($crate::Code::InvalidArgument, $($arg)+)
    }};
}

#[macro_export]
macro_rules! error_if {
    ($cond:expr, $($arg:tt)+) => {{
        if $cond {
            Err($crate::make_err!($crate::Code::InvalidArgument, $($arg)+))?;
        }
    }};
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Error {
    pub code: Code,
    pub messages: Vec<String>,
}

impl Error {
    pub fn new(code: Code, msg: String) -> Self {
        let mut msgs = Vec::with_capacity(1);
        if !msg.is_empty() {
            msgs.push(msg);
        }
        Error { code, messages: msgs }
    }

    pub fn merge<E: Into<Error>>(mut self, other: E) -> Self {
        let mut other: Error = other.into();
        // This will help with knowing which messages are tied to different errors.
        self.messages.push("---".to_string());
        self.messages.append(&mut other.messages);
        self
    }

    pub fn to_std_err(self) -> std::io::Error {
        std::io::Error::new(self.code.into(), self.messages.join(" : "))
    }
}

impl std::error::Error for Error {}

impl Into<proto::google::rpc::Status> for Error {
    fn into(self) -> proto::google::rpc::Status {
        (&self).into()
    }
}

impl Into<proto::google::rpc::Status> for &Error {
    fn into(self) -> proto::google::rpc::Status {
        proto::google::rpc::Status {
            code: self.code as i32,
            message: format!("Error: {:?}", self),
            details: vec![],
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A manual impl to reduce the noise of frequently empty fields.
        let mut builder = f.debug_struct("Error");

        builder.field("code", &self.code);

        if !self.messages.is_empty() {
            builder.field("messages", &self.messages);
        }

        builder.finish()
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        make_err!(Code::Internal, "{}", err.to_string())
    }
}

impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Self {
        make_err!(Code::Internal, "{}", err.to_string())
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        make_err!(Code::InvalidArgument, "{}", err.to_string())
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        make_err!(Code::Internal, "{}", err.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        make_err!(Code::InvalidArgument, "{}", err.to_string())
    }
}

impl From<hex::FromHexError> for Error {
    fn from(err: hex::FromHexError) -> Self {
        make_err!(Code::InvalidArgument, "{}", err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error {
            code: err.kind().into(),
            messages: vec![err.to_string()],
        }
    }
}

impl From<Code> for Error {
    fn from(code: Code) -> Self {
        make_err!(code, "")
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        make_err!(status.code().into(), "{}", status.to_string())
    }
}

impl Into<tonic::Status> for Error {
    fn into(self) -> tonic::Status {
        tonic::Status::new(self.code.into(), self.messages.join(" : "))
    }
}

pub trait ResultExt<T> {
    fn err_tip_with_code<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: std::string::ToString,
        F: (std::ops::FnOnce(&Error) -> (Code, S)) + Sized;

    #[inline]
    fn err_tip<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: std::string::ToString,
        F: (std::ops::FnOnce() -> S) + Sized,
    {
        self.err_tip_with_code(|e| (e.code, tip_fn()))
    }

    fn merge<U>(self, _other: Result<U, Error>) -> Result<U, Error>
    where
        Self: Sized,
    {
        unreachable!();
    }
}

impl<T, E: Into<Error>> ResultExt<T> for Result<T, E> {
    #[inline]
    fn err_tip_with_code<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: std::string::ToString,
        F: (std::ops::FnOnce(&Error) -> (Code, S)) + Sized,
    {
        self.map_err(|e| {
            let mut error: Error = e.into();
            let (code, message) = tip_fn(&error);
            error.code = code;
            error.messages.push(message.to_string());
            error
        })
    }

    fn merge<U>(self, other: Result<U, Error>) -> Result<U, Error>
    where
        Self: Sized,
    {
        if let Err(e) = self {
            let mut e: Error = e.into();
            if let Err(other_err) = other {
                let mut other_err: Error = other_err.into();
                // This will help with knowing which messages are tied to different errors.
                e.messages.push("---".to_string());
                e.messages.append(&mut other_err.messages);
            }
            return Err(e);
        }
        other
    }
}

impl<T> ResultExt<T> for Option<T> {
    #[inline]
    fn err_tip_with_code<F, S>(self, tip_fn: F) -> Result<T, Error>
    where
        Self: Sized,
        S: std::string::ToString,
        F: (std::ops::FnOnce(&Error) -> (Code, S)) + Sized,
    {
        self.ok_or_else(|| {
            let mut error = Error {
                code: Code::Internal,
                messages: vec![],
            };
            let (code, message) = tip_fn(&error);
            error.code = code;
            error.messages.push(message.to_string());
            error
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,

    // New Codes may be added in the future, so never exhaustively match!
    #[doc(hidden)]
    __NonExhaustive,
}

impl From<tonic::Code> for Code {
    fn from(code: tonic::Code) -> Self {
        match code {
            tonic::Code::Ok => Code::Ok,
            tonic::Code::Cancelled => Code::Cancelled,
            tonic::Code::Unknown => Code::Unknown,
            tonic::Code::InvalidArgument => Code::InvalidArgument,
            tonic::Code::DeadlineExceeded => Code::DeadlineExceeded,
            tonic::Code::NotFound => Code::NotFound,
            tonic::Code::AlreadyExists => Code::AlreadyExists,
            tonic::Code::PermissionDenied => Code::PermissionDenied,
            tonic::Code::ResourceExhausted => Code::ResourceExhausted,
            tonic::Code::FailedPrecondition => Code::FailedPrecondition,
            tonic::Code::Aborted => Code::Aborted,
            tonic::Code::OutOfRange => Code::OutOfRange,
            tonic::Code::Unimplemented => Code::Unimplemented,
            tonic::Code::Internal => Code::Internal,
            tonic::Code::Unavailable => Code::Unavailable,
            tonic::Code::DataLoss => Code::DataLoss,
            tonic::Code::Unauthenticated => Code::Unauthenticated,
        }
    }
}

impl Into<tonic::Code> for Code {
    fn into(self) -> tonic::Code {
        match self {
            Code::Ok => tonic::Code::Ok,
            Code::Cancelled => tonic::Code::Cancelled,
            Code::Unknown => tonic::Code::Unknown,
            Code::InvalidArgument => tonic::Code::InvalidArgument,
            Code::DeadlineExceeded => tonic::Code::DeadlineExceeded,
            Code::NotFound => tonic::Code::NotFound,
            Code::AlreadyExists => tonic::Code::AlreadyExists,
            Code::PermissionDenied => tonic::Code::PermissionDenied,
            Code::ResourceExhausted => tonic::Code::ResourceExhausted,
            Code::FailedPrecondition => tonic::Code::FailedPrecondition,
            Code::Aborted => tonic::Code::Aborted,
            Code::OutOfRange => tonic::Code::OutOfRange,
            Code::Unimplemented => tonic::Code::Unimplemented,
            Code::Internal => tonic::Code::Internal,
            Code::Unavailable => tonic::Code::Unavailable,
            Code::DataLoss => tonic::Code::DataLoss,
            Code::Unauthenticated => tonic::Code::Unauthenticated,
            _ => tonic::Code::Unknown,
        }
    }
}

impl From<std::io::ErrorKind> for Code {
    fn from(kind: std::io::ErrorKind) -> Self {
        match kind {
            std::io::ErrorKind::NotFound => Code::NotFound,
            std::io::ErrorKind::PermissionDenied => Code::PermissionDenied,
            std::io::ErrorKind::ConnectionRefused => Code::Unavailable,
            std::io::ErrorKind::ConnectionReset => Code::Unavailable,
            std::io::ErrorKind::ConnectionAborted => Code::Unavailable,
            std::io::ErrorKind::NotConnected => Code::Internal,
            std::io::ErrorKind::AddrInUse => Code::Internal,
            std::io::ErrorKind::AddrNotAvailable => Code::Internal,
            std::io::ErrorKind::BrokenPipe => Code::Internal,
            std::io::ErrorKind::AlreadyExists => Code::AlreadyExists,
            std::io::ErrorKind::WouldBlock => Code::Internal,
            std::io::ErrorKind::InvalidInput => Code::InvalidArgument,
            std::io::ErrorKind::InvalidData => Code::InvalidArgument,
            std::io::ErrorKind::TimedOut => Code::DeadlineExceeded,
            std::io::ErrorKind::WriteZero => Code::Internal,
            std::io::ErrorKind::Interrupted => Code::Aborted,
            std::io::ErrorKind::Other => Code::Internal,
            std::io::ErrorKind::UnexpectedEof => Code::Internal,
            _ => Code::Unknown,
        }
    }
}

impl From<Code> for std::io::ErrorKind {
    fn from(kind: Code) -> Self {
        match kind {
            Code::Aborted => std::io::ErrorKind::Interrupted,
            Code::AlreadyExists => std::io::ErrorKind::AlreadyExists,
            Code::DeadlineExceeded => std::io::ErrorKind::TimedOut,
            Code::Internal => std::io::ErrorKind::Other,
            Code::InvalidArgument => std::io::ErrorKind::InvalidInput,
            Code::NotFound => std::io::ErrorKind::NotFound,
            Code::PermissionDenied => std::io::ErrorKind::PermissionDenied,
            Code::Unavailable => std::io::ErrorKind::ConnectionRefused,
            _ => std::io::ErrorKind::Other,
        }
    }
}
