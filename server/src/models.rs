use diesel::prelude::*;

use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use serde::Serialize;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

#[derive(Clone, Queryable, Selectable, Serialize)]
#[diesel(table_name = crate::schema::applications)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite, diesel::pg::Pg))]
pub struct Application {
    pub id: i32,
    pub user: String,
    pub token: String,
    pub addr: Option<String>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::applications)]
pub struct NewApplication {
    pub user: String,
    pub token: String,
}
