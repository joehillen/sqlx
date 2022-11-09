use futures_core::future::BoxFuture;
use std::marker::PhantomData;
use url::Url;

use crate::any::{Any, AnyConnectOptions, AnyKind};
use crate::connection::{ConnectOptions, Connection};
use crate::error::Error;

use crate::database::Database;
pub use backend::AnyConnectionBackend;

use crate::transaction::Transaction;

mod backend;
mod establish;
mod executor;

/// A connection to _any_ SQLx database.
///
/// The database driver used is determined by the scheme
/// of the connection url.
///
/// ```text
/// postgres://postgres@localhost/test
/// sqlite://a.sqlite
/// ```
#[derive(Debug)]
pub struct AnyConnection {
    pub(crate) backend: Box<dyn AnyConnectionBackend>,
}

impl AnyConnection {
    pub(crate) fn connect<DB: Database>(
        options: &AnyConnectOptions,
    ) -> BoxFuture<'_, crate::Result<Self>>
    where
        DB::Connection: AnyConnectionBackend,
        <DB::Connection as Connection>::Options:
            for<'a> TryFrom<&'a AnyConnectOptions, Error = Error>,
    {
        let res = TryFrom::try_from(options);

        Box::pin(async {
            let options: <DB::Connection as Connection>::Options = res?;

            Ok(AnyConnection {
                backend: Box::new(options.connect().await?),
            })
        })
    }

    #[cfg(feature = "migrate")]
    pub(crate) fn get_migrate(
        &mut self,
    ) -> crate::Result<&mut (dyn crate::migrate::Migrate + Send + 'static)> {
        self.backend.as_migrate()
    }
}

impl Connection for AnyConnection {
    type Database = Any;

    type Options = AnyConnectOptions;

    fn close(self) -> BoxFuture<'static, Result<(), Error>> {
        self.backend.close()
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), Error>> {
        self.backend.close()
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.backend.ping()
    }

    fn begin(&mut self) -> BoxFuture<'_, Result<Transaction<'_, Self::Database>, Error>>
    where
        Self: Sized,
    {
        Transaction::begin(self)
    }

    fn cached_statements_size(&self) -> usize {
        self.backend.cached_statements_size()
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, crate::Result<()>> {
        self.backend.clear_cached_statements()
    }

    #[doc(hidden)]
    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.backend.flush()
    }

    #[doc(hidden)]
    fn should_flush(&self) -> bool {
        self.backend.should_flush()
    }
}
