diesel::table! {
    applications (id) {
        id -> Integer,
        user -> Text,
        token -> Text,
        addr -> Nullable<Text>,
    }
}
