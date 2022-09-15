use crate::{
    Either, PgArguments, PgColumn, PgConnection, PgQueryResult, PgRow, PgTransactionManager,
    PgTypeInfo, Postgres,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{TryFutureExt, TryStreamExt};
use std::borrow::Cow;
use std::sync::Arc;

use sqlx_core::any::{
    Any, AnyArguments, AnyColumn, AnyConnectionBackend, AnyQueryResult, AnyRow, AnyStatement,
    AnyTypeInfo, AnyTypeInfoKind, AnyValue, AnyValueKind,
};

use crate::type_info::PgType;
use sqlx_core::any::driver::AnyDriver;
use sqlx_core::column::Column;
use sqlx_core::connection::Connection;
use sqlx_core::describe::Describe;
use sqlx_core::executor::Executor;
use sqlx_core::row::Row;
use sqlx_core::transaction::TransactionManager;

pub const DRIVER: AnyDriver = AnyDriver::with_migrate::<Postgres>();

impl AnyConnectionBackend for PgConnection {
    fn name(&self) -> &str {
        "PostgreSQL"
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self)
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self)
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::pin(self)
    }

    fn begin(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::begin(self)
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::commit(self)
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::rollback(self)
    }

    fn start_rollback(&mut self) {
        PgTransactionManager::start_rollback(self)
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self)
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    fn fetch_many<'q>(
        &'q mut self,
        query: &'q str,
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxStream<'q, sqlx_core::Result<Either<AnyQueryResult, AnyRow>>> {
        let persistent = arguments.is_some();
        let args = arguments.map(AnyArguments::convert_into);

        Box::pin(
            self.run(query, args, 0, persistent, None)
                .try_flatten_stream()
                .and_then(|res: Either<AnyQueryResult, AnyRow>| async move {
                    res.map_left(map_result).map_right(map_row)
                }),
        )
    }

    fn fetch_optional<'q>(
        &'q mut self,
        query: &'q str,
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxFuture<'q, sqlx_core::Result<Option<AnyRow>>> {
        let persistent = arguments.is_some();
        let args = arguments.map(AnyArguments::convert_into);

        Box::pin(async move {
            let mut stream = self.run(query, args, 1, persistent, None).await?;
            futures_util::pin_mut!(stream);

            if let Some(Either::Right(row)) = stream.try_next().await? {
                return Ok(Some(map_row(row)?));
            }

            Ok(None)
        })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: &'q str,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement<'q>>> {
        Box::pin(async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;

            let parameters = if !statement.metadata.parameters.is_empty() {
                Some(Either::Left(
                    statement
                        .metadata
                        .parameters
                        .iter()
                        .enumerate()
                        .map(|(i, type_info)| {
                            map_type(type_info).ok_or_else(|| {
                                sqlx_core::Error::AnyDriverError(
                                    format!(
                                        "Any driver does not support type {} of parameter {}",
                                        type_info, i
                                    )
                                    .into(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            } else {
                None
            };

            let columns = statement
                .metadata
                .columns
                .iter()
                .map(map_column)
                .collect::<Result<Vec<_>, _>>()?;

            Ok(AnyStatement {
                sql: Cow::Borrowed(sql),
                parameters,
                column_names: Arc::new(statement.metadata.column_names.clone()),
                columns,
            })
        })
    }

    fn describe<'q>(&'q mut self, sql: &'q str) -> BoxFuture<'q, sqlx_core::Result<Describe<Any>>> {
        todo!()
    }
}

fn map_type(pg_type: &PgTypeInfo) -> Option<AnyTypeInfo> {
    Some(AnyTypeInfo {
        kind: match &pg_type.0 {
            PgType::Void => AnyTypeInfoKind::Null,
            PgType::Int2 => AnyTypeInfoKind::SmallInt,
            PgType::Int4 => AnyTypeInfoKind::Integer,
            PgType::Int8 => AnyTypeInfoKind::BigInt,
            PgType::Float4 => AnyTypeInfoKind::Real,
            PgType::Float8 => AnyTypeInfoKind::Double,
            PgType::Bytea => AnyTypeInfoKind::Blob,
            PgType::Text => AnyTypeInfoKind::Text,
            _ => return None,
        },
    })
}

fn map_column(col: &PgColumn) -> sqlx_core::Result<AnyColumn> {
    let type_info = map_type(&col.type_info).ok_or_else(|| sqlx_core::Error::ColumnDecode {
        index: col.name.to_string(),
        source: format!("Any driver does not support column type {:?}", pg_type).into(),
    })?;

    Ok(AnyColumn {
        ordinal: col.ordinal,
        name: col.name.clone(),
        type_info,
    })
}

fn map_row(row: PgRow) -> sqlx_core::Result<AnyRow> {
    let mut row_out = AnyRow {
        column_names: row.metadata.column_names.clone(),
        columns: Vec::with_capacity(row.columns().len()),
        values: Vec::with_capacity(row.columns().len()),
    };

    for col in row.columns() {
        let i = col.ordinal;

        let any_col = map_column(col)?;

        let value_kind = match any_col.type_info().kind() {
            _ if row.data.values[i].is_none() => AnyValueKind::Null,
            AnyTypeInfoKind::Null => AnyValueKind::Null,
            AnyTypeInfoKind::SmallInt => AnyValueKind::SmallInt(row.try_get(i)?),
            AnyTypeInfoKind::Integer => AnyValueKind::Integer(row.try_get(i)?),
            AnyTypeInfoKind::BigInt => AnyValueKind::BigInt(row.try_get(i)?),
            AnyTypeInfoKind::Real => AnyValueKind::Real(row.try_get(i)?),
            AnyTypeInfoKind::Double => AnyValueKind::Double(row.try_get(i)?),
            AnyTypeInfoKind::Blob => AnyValueKind::Blob(row.try_get(i)?),
            AnyTypeInfoKind::Text => AnyValueKind::Text(row.try_get(i)?),
        };

        row_out.columns.push(any_col);
        row_out.values.push(AnyValue { kind: value_kind });
    }

    Ok(row_out)
}

fn map_result(res: PgQueryResult) -> AnyQueryResult {
    AnyQueryResult {
        rows_affected: res.rows_affected(),
        last_insert_id: None,
    }
}
