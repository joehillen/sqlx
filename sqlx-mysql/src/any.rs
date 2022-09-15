use crate::MySql;
use sqlx_core::any::driver::AnyDriver;

pub const DRIVER: AnyDriver = AnyDriver::with_migrate::<MySql>();
