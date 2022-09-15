use crate::any::error::mismatched_types;
use crate::any::{Any, AnyColumn, AnyValue};
use crate::column::ColumnIndex;
use crate::database::HasValueRef;
use crate::decode::Decode;
use crate::error::Error;
use crate::ext::ustr::UStr;
use crate::row::Row;
use crate::type_info::TypeInfo;
use crate::types::Type;
use crate::value::{Value, ValueRef};

#[derive(Clone)]
pub struct AnyRow {
    #[doc(hidden)]
    pub column_names: crate::HashMap<UStr, usize>,
    #[doc(hidden)]
    pub columns: Vec<AnyColumn>,
    #[doc(hidden)]
    pub values: Vec<AnyValue>,
}

impl Row for AnyRow {
    type Database = Any;

    fn columns(&self) -> &[AnyColumn] {
        &self.columns
    }

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> Result<<Self::Database as HasValueRef<'_>>::ValueRef, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        Ok(self
            .columns
            .get(index)
            .ok_or_else(|| Error::ColumnIndexOutOfBounds {
                index,
                len: self.columns.len(),
            })?
            .value
            .as_ref())
    }

    fn try_get<'r, T, I>(&'r self, index: I) -> Result<T, Error>
    where
        I: ColumnIndex<Self>,
        T: Decode<'r, Self::Database> + Type<Self::Database>,
    {
        let value = self.try_get_raw(&index)?;
        let ty = value.type_info();

        if !value.is_null() && !ty.is_null() && !T::compatible(&ty) {
            Err(mismatched_types::<T>(&ty))
        } else {
            T::decode(value)
        }
        .map_err(|source| Error::ColumnDecode {
            index: format!("{:?}", index),
            source,
        })
    }
}

impl<'i> ColumnIndex<AnyRow> for &'i str {
    fn index(&self, row: &AnyRow) -> Result<usize, Error> {
        row.column_names
            .get(*self)
            .copied()
            .ok_or_else(|| Error::ColumnNotFound(self.to_string()))
    }
}
