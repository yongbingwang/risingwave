use std::sync::Arc;

use diesel::pg::Pg;
use diesel::pg::types::sql_types;
use diesel::serialize::Output;
use diesel::types::ToSql;
use serde_json::Value;

use super::HummockForwardIndexer;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::hummock::{HummockOptions, HummockStorage};
use crate::object::InMemObjectStore;

#[tokio::test]
async fn basic_index_test() {
    let jdoc = r#"{
        "array": [
          1,
          2,
          3,
          {
            "a": 1,
            "b": "cool",
            "c": [
              1,
              2,
              "cold"
            ]
          }
        ],
        "boolean": true,
        "color": "gold",
        "null": null,
        "number": 123,
        "object": {
          "a": "b",
          "c": "d"
        },
        "string": "Hello World"
      }"#;

    // Create a hummock instance so as to fill the registry.
    let hummock_options = HummockOptions::default_for_test();
    let object_client = Arc::new(InMemObjectStore::new());
    let local_version_manager = Arc::new(LocalVersionManager::new(
        object_client.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let hummock_storage = HummockStorage::new(
        object_client,
        hummock_options,
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
    )
    .await
    .unwrap();

    // Primary index.
    let pk_cfname = "pk_index".as_bytes().to_vec();
    let pk_index = HummockForwardIndexer::new(hummock_storage.clone(), pk_cfname.clone());
    let k: u64 = 0;
    let v: Value = serde_json::from_str(jdoc).unwrap();

    let mut buffer = Vec::new();
    let mut bytes = Output::test(&mut buffer);
    let test_json = serde_json::Value::Bool(true);
    ToSql::<sql_types::Jsonb, Pg>::to_sql(&test_json, &mut bytes).unwrap();

    pk_index
        .index_entry(&k.to_be_bytes(), &v)
        .unwrap();
    pk_index.commit(1).await.unwrap();

    // Secondary index.
    // `CREATE INDEX nest_array_index ON table_name USING GIN (jdoc->'array'->3->'b');`
    let sk_cfname = r#"jdoc->'array'->3->'b'"#.as_bytes().to_vec();
    let sk_index = HummockForwardIndexer::new(hummock_storage.clone(), sk_cfname);
    let k = "cool".as_bytes().to_vec();
    let doc_id: u64 = 0;
    sk_index
        .index_entry(&k, &doc_id.to_be_bytes())
        .unwrap();
}

#[tokio::test]
async fn collection_test() {
    let collection = [
        r#"{"ISBN": 8647295405123, "year": 1988, "genre": "science", "title": "A Brief History of Time", "author": {"given_name": "Stephen", "family_name": "Hawking"}, "editors": ["Melisa", "Mark", "John", "Fred", "Jane"]}"#,
        r#"{"ISBN": 4582546494267, "year": 1623, "title": "Macbeth", "author": {"given_name": "William", "family_name": "Shakespeare"}}"#,
        r#"{"ISBN": 6563973589123, "year": 1989, "genre": "novel", "title": "Joy Luck Club", "author": {"given_name": "Amy", "family_name": "Tan"}, "editors": ["Ruilin", "Aiping"]}"#,
        r#"{"ISBN": 9874563896457, "year": 1950, "genre": "novel", "title": "Great Expectations", "author": {"family_name": "Dickens"}, "editors": ["Robert", "John", "Melisa", "Elizabeth"]}"#,
        r#"{"ISBN": 8760835734528, "year": 1603, "title": "Hamlet", "author": {"given_name": "William", "family_name": "Shakespeare"}, "editors": ["Lysa", "Elizabeth"]}"#,
        r#"{"ISBN": 7658956876542, "year": 1838, "genre": "novel", "title": "Oliver Twist", "author": {"given_name": "Charles", "family_name": "Dickens"}, "editors": ["Mark", "Tony", "Britney"]}"#,
    ];

    let sk_cfname = "jdoc->ISBN".as_bytes().to_vec();

    // Create a hummock instance so as to fill the registry.
    let hummock_options = HummockOptions::default_for_test();
    let object_client = Arc::new(InMemObjectStore::new());
    let local_version_manager = Arc::new(LocalVersionManager::new(
        object_client.clone(),
        &hummock_options.remote_dir,
        None,
    ));
    let hummock_storage = HummockStorage::new(
        object_client,
        hummock_options,
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
    )
    .await
    .unwrap();

    let sk_index = HummockForwardIndexer::new(hummock_storage.clone(), sk_cfname.clone());

    for jdoc in collection.iter() {
        let jdoc_bytes = serde_json::to_vec(jdoc).unwrap();
        let sk = jdoc.get("ISBN").unwrap().to_string().as_bytes().to_vec();
        sk_index.index_entry(&sk, &jdoc_bytes).await.unwrap();
    }

    sk_index.commit(1).await.unwrap();
}
