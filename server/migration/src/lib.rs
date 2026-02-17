pub use sea_orm_migration::prelude::*;

mod m20220101_000001_create_table_application;
mod m20260217_000002_add_name_to_application;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20220101_000001_create_table_application::Migration),
            Box::new(m20260217_000002_add_name_to_application::Migration),
        ]
    }
}
