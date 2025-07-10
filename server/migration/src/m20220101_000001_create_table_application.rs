use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Application::Table)
                    .if_not_exists()
                    .col(pk_auto(Application::Id))
                    .col(string(Application::Username))
                    .col(string(Application::Token))
                    .col(string_null(Application::Address))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("application_token")
                    .table(Application::Table)
                    .col(Application::Token)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("application_username")
                    .table(Application::Table)
                    .col(Application::Username)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("application_username")
                    .table(Application::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("application_token")
                    .table(Application::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .if_exists()
                    .table(Application::Table)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Application {
    Table,
    Id,
    Username,
    Token,
    Address,
}
