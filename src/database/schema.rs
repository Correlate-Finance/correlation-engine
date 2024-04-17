// @generated automatically by Diesel CLI.

diesel::table! {
    auth_group (id) {
        id -> Int4,
        #[max_length = 150]
        name -> Varchar,
    }
}

diesel::table! {
    auth_group_permissions (id) {
        id -> Int8,
        group_id -> Int4,
        permission_id -> Int4,
    }
}

diesel::table! {
    auth_permission (id) {
        id -> Int4,
        #[max_length = 255]
        name -> Varchar,
        content_type_id -> Int4,
        #[max_length = 100]
        codename -> Varchar,
    }
}

diesel::table! {
    authtoken_token (key) {
        #[max_length = 40]
        key -> Varchar,
        created -> Timestamptz,
        user_id -> Int8,
    }
}

diesel::table! {
    datasets_dataset (id) {
        id -> Int4,
        date -> Timestamptz,
        value -> Float8,
        created_at -> Timestamptz,
        metadata_id -> Int8,
    }
}

diesel::table! {
    datasets_datasetmetadata (id) {
        id -> Int8,
        #[max_length = 255]
        internal_name -> Varchar,
        #[max_length = 255]
        external_name -> Nullable<Varchar>,
        description -> Nullable<Text>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        #[max_length = 255]
        category -> Nullable<Varchar>,
        high_level -> Bool,
        #[max_length = 255]
        source -> Nullable<Varchar>,
        group_popularity -> Nullable<Int4>,
        popularity -> Nullable<Int4>,
        hidden -> Bool,
        #[max_length = 255]
        sub_source -> Nullable<Varchar>,
        #[max_length = 255]
        units -> Nullable<Varchar>,
        #[max_length = 255]
        units_short -> Nullable<Varchar>,
        #[max_length = 255]
        release -> Nullable<Varchar>,
        #[max_length = 200]
        url -> Nullable<Varchar>,
        categories -> Nullable<Array<Nullable<Varchar>>>,
    }
}

diesel::table! {
    datasets_index (id) {
        id -> Int4,
        #[max_length = 255]
        name -> Varchar,
        #[max_length = 255]
        aggregation_period -> Nullable<Varchar>,
        #[max_length = 255]
        correlation_metric -> Nullable<Varchar>,
        created_at -> Timestamptz,
        user_id -> Int8,
    }
}

diesel::table! {
    datasets_indexdataset (id) {
        id -> Int4,
        weight -> Float8,
        dataset_id -> Int8,
        index_id -> Int4,
    }
}

diesel::table! {
    django_admin_log (id) {
        id -> Int4,
        action_time -> Timestamptz,
        object_id -> Nullable<Text>,
        #[max_length = 200]
        object_repr -> Varchar,
        action_flag -> Int2,
        change_message -> Text,
        content_type_id -> Nullable<Int4>,
        user_id -> Int8,
    }
}

diesel::table! {
    django_content_type (id) {
        id -> Int4,
        #[max_length = 100]
        app_label -> Varchar,
        #[max_length = 100]
        model -> Varchar,
    }
}

diesel::table! {
    django_migrations (id) {
        id -> Int8,
        #[max_length = 255]
        app -> Varchar,
        #[max_length = 255]
        name -> Varchar,
        applied -> Timestamptz,
    }
}

diesel::table! {
    django_session (session_key) {
        #[max_length = 40]
        session_key -> Varchar,
        session_data -> Text,
        expire_date -> Timestamptz,
    }
}

diesel::table! {
    users_allowlist (id) {
        id -> Int8,
        #[max_length = 254]
        email -> Varchar,
    }
}

diesel::table! {
    users_user (id) {
        id -> Int8,
        last_login -> Nullable<Timestamptz>,
        is_superuser -> Bool,
        #[max_length = 150]
        first_name -> Varchar,
        #[max_length = 150]
        last_name -> Varchar,
        is_staff -> Bool,
        is_active -> Bool,
        date_joined -> Timestamptz,
        #[max_length = 255]
        name -> Varchar,
        #[max_length = 255]
        email -> Varchar,
        #[max_length = 255]
        password -> Varchar,
    }
}

diesel::table! {
    users_user_groups (id) {
        id -> Int8,
        user_id -> Int8,
        group_id -> Int4,
    }
}

diesel::table! {
    users_user_user_permissions (id) {
        id -> Int8,
        user_id -> Int8,
        permission_id -> Int4,
    }
}

diesel::table! {
    users_watchlist (id) {
        id -> Int4,
        created_at -> Timestamptz,
        dataset_id -> Int8,
        user_id -> Int8,
    }
}

diesel::joinable!(auth_group_permissions -> auth_group (group_id));
diesel::joinable!(auth_group_permissions -> auth_permission (permission_id));
diesel::joinable!(auth_permission -> django_content_type (content_type_id));
diesel::joinable!(authtoken_token -> users_user (user_id));
diesel::joinable!(datasets_dataset -> datasets_datasetmetadata (metadata_id));
diesel::joinable!(django_admin_log -> django_content_type (content_type_id));
diesel::joinable!(django_admin_log -> users_user (user_id));
diesel::joinable!(users_user_groups -> auth_group (group_id));
diesel::joinable!(users_user_groups -> users_user (user_id));
diesel::joinable!(users_user_user_permissions -> auth_permission (permission_id));
diesel::joinable!(users_user_user_permissions -> users_user (user_id));
diesel::joinable!(users_watchlist -> datasets_datasetmetadata (dataset_id));
diesel::joinable!(users_watchlist -> users_user (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    auth_group,
    auth_group_permissions,
    auth_permission,
    authtoken_token,
    datasets_dataset,
    datasets_datasetmetadata,
    django_admin_log,
    django_content_type,
    django_migrations,
    django_session,
    users_allowlist,
    users_user,
    users_user_groups,
    users_user_user_permissions,
    users_watchlist,
);
