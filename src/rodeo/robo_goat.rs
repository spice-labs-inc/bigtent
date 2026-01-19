use std::{collections::BTreeSet, fmt::Debug, fs::File, io::Write, path::PathBuf, sync::Arc};

use crate::{
    item::{Item, TAG_FROM, TAG_TO},
    rodeo::goat_trait::{impl_antialias_for, impl_north_send, impl_stream_flattened_items},
    util::{MD5Hash, iso8601_now, md5hash_str, sha256_for_slice},
};
use anyhow::Result;
use log::error;
use serde_json::{Map, json};
use tokio::sync::mpsc::Receiver;
use tokio_util::either::Either;
use uuid::Uuid;

pub trait ClusterRoboMember {
    fn name(&self) -> String;
    fn offset_from_pos(&self, pos: usize) -> Option<ItemOffset>;
    fn item_from_item_offset(&self, item_offset: &ItemOffset) -> Option<Item>;
}
use super::{goat_trait::GoatRodeoTrait, index::ItemOffset};
#[derive(Debug, Clone)]
pub struct RoboticGoat {
    name: String,
    items: Vec<Item>,
    uuid: String,
    offsets: Vec<ItemOffset>,
    history: serde_json::Value,
}

impl ClusterRoboMember for RoboticGoat {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn offset_from_pos(&self, pos: usize) -> Option<ItemOffset> {
        if pos >= self.items.len() {
            error!("Offset out of bounds {} vs. {}", pos, self.items.len());
            return None;
        }

        Some(self.offsets[pos])
    }
    fn item_from_item_offset(&self, item_offset: &ItemOffset) -> Option<Item> {
        let pos = item_offset.loc.0;
        if pos >= self.items.len() {
            error!("Invalid offset {}", pos);
            return None;
        }

        Some(self.items[pos].clone())
    }
}

impl RoboticGoat {
    pub fn new(name: &str, items: Vec<Item>, history: serde_json::Value) -> Arc<RoboticGoat> {
        let uuid = Uuid::new_v4().to_string();
        let mut offsets = vec![];
        for (idx, item) in items.iter().enumerate() {
            offsets.push(ItemOffset {
                hash: md5hash_str(&item.identifier),
                loc: (idx, 0),
            });
        }
        offsets.sort_by_key(|v| v.hash);
        Arc::new(RoboticGoat {
            name: name.to_string(),
            items,
            uuid,
            offsets,
            history,
        })
    }

    /// Create a robo cluster with a base name (e.g., "tags" or "uploads")
    /// where the tag name is a particular string and the specific items are a
    /// `Vec<(String, Value)>`
    pub fn new_cluster(
        base_name: &str,
        tag_name: &str,
        items: Vec<(String, serde_json::Value)>,
    ) -> Result<Arc<RoboticGoat>> {
        let mut robo_items = vec![];
        for (name, json) in items {
            let mut map = match json {
                serde_json::Value::Object(m) => m,
                v => {
                    let mut m = Map::new();
                    m.insert("extra".to_string(), v);
                    m
                }
            };
            map.insert("tag".to_string(), tag_name.into());
            map.insert("date".to_string(), iso8601_now().into());

            // there's gotta be a better way to convert from serde_json to serde_cbor///
            let ser = serde_cbor::to_vec(&serde_json::Value::Object(map))?;

            let body: serde_cbor::Value = serde_cbor::from_reader(&ser[..])?;

            let identifier = format!("tag:sha256:{}", hex::encode(&sha256_for_slice(&ser)));

            let i = Item {
                identifier: identifier.clone(),
                connections: BTreeSet::from([
                    (TAG_FROM.to_string(), base_name.to_string()),
                    (TAG_TO.to_string(), name.clone()),
                ]),
                body_mime_type: Some("application/vnd.cc.goatrodeo.tag".to_string()),
                body: Some(body),
            };
            robo_items.push(i);
            // create the "back link"
            robo_items.push(Item {
                identifier: name,
                connections: BTreeSet::from([(TAG_FROM.to_string(), identifier)]),
                body_mime_type: None,
                body: None,
            });
        }
        let mut connections = BTreeSet::new();

        // backlinks from the tags to the root tag
        for i in &robo_items {
            // only for the actual tags, not for the synthetic back-link
            if i.body.is_some() {
                connections.insert((TAG_TO.to_string(), i.identifier.to_string()));
            }
        }
        let tags = Item {
            identifier: base_name.to_string(),
            connections,
            body_mime_type: None,
            body: None,
        };
        robo_items.push(tags);

        Ok(RoboticGoat::new(
            tag_name,
            robo_items,
            json!({"date": iso8601_now(), "operation": "synthetic tag"}),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_synthetic() {
    use super::{goat::GoatRodeoCluster, goat_herd::GoatHerd};
    use crate::item::EdgeType;
    let path = PathBuf::from("test_data/cluster_a/2025_04_19_17_10_26_012a73d9c40dc9c0.grc");
    let cluster_a = GoatRodeoCluster::new(&path, false, None, None)
        .await
        .expect("Should get first cluster");
    let mut roots = cluster_a.clone().roots().await;
    let mut root_ids = vec![];
    while let Some(item) = roots.recv().await {
        root_ids.push((item.identifier.clone(), json!({"hello": 42})));
    }

    assert!(
        root_ids.len() == 1,
        "Expected 1 root ids, got {}",
        root_ids.len()
    );

    let synth = RoboticGoat::new_cluster("tags", "test test", root_ids)
        .expect("Should get a synthetic goat");

    let herd = GoatHerd::new(vec![
        crate::rodeo::member::member_core(cluster_a),
        crate::rodeo::member::member_synth(synth),
    ]);

    let tags = herd
        .item_for_identifier("tags")
        .expect("Should get tags from option");

    let tagged: Vec<String> = tags
        .connections
        .iter()
        .filter(|conn| conn.0.is_tag_to())
        .map(|conn| conn.1.clone())
        .collect();

    assert_eq!(tagged.len(), 1, "Expecting 1 tag, got {:?}", tagged);

    for t in &tagged {
        let the_tag = herd.item_for_identifier(&t).expect("Get tag");
        let the_tag_id = &the_tag.identifier;
        for (t, v) in &the_tag.connections {
            if t.is_tag_to() {
                let tagged_item = herd
                    .item_for_identifier(v)
                    .expect(&format!("Should load {}", v));
                assert_eq!(
                    1,
                    tagged_item
                        .connections
                        .iter()
                        .filter(|c| c.0.is_tag_from() && the_tag_id == &c.1)
                        .count()
                );
            }
        }
    }
}

impl GoatRodeoTrait for RoboticGoat {
    fn node_count(&self) -> u64 {
        self.items.len() as u64
    }

    fn get_purl(&self) -> Result<PathBuf> {
        let filename = format!("/tmp/{}.txt", self.uuid);
        let ret = PathBuf::from(&filename);
        if ret.exists() && ret.is_file() {
            return Ok(ret);
        }

        let mut dest = File::create(&ret)?;

        dest.flush()?;
        Ok(ret)
    }

    fn number_of_items(&self) -> usize {
        self.items.len()
    }

    fn read_history(&self) -> Result<Vec<serde_json::Value>> {
        Ok(vec![self.history.clone()])
    }

    async fn north_send(
        self: Arc<Self>,
        gitoids: Vec<String>,
        purls_only: bool,
        start: std::time::Instant,
    ) -> Result<Receiver<Either<Item, String>>> {
        impl_north_send(self, gitoids, purls_only, start).await
    }

    async fn roots(self: Arc<Self>) -> Receiver<Item> {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            for item in &self.items {
                if item.is_root_item() {
                    let _ = tx.send(item.clone()).await;
                }
            }
        });
        rx
    }

    fn item_for_identifier(&self, data: &str) -> Option<Item> {
        self.item_for_hash(md5hash_str(data))
    }

    fn item_for_hash(&self, hash: MD5Hash) -> Option<Item> {
        let found = match self.offsets.binary_search_by_key(&hash, |v| v.hash) {
            Ok(v) => v,
            Err(_) => return None,
        };
        let res = self.item_from_item_offset(&self.offsets[found]);
        res
    }

    fn antialias_for(self: Arc<Self>, data: &str) -> Option<Item> {
        impl_antialias_for(self, data)
    }

    async fn stream_flattened_items(
        self: Arc<Self>,
        gitoids: Vec<String>,
        source: bool,
    ) -> Result<Receiver<Either<Item, String>>> {
        impl_stream_flattened_items(self, gitoids, source).await
    }

    fn has_identifier(&self, identifier: &str) -> bool {
        self.offsets
            .binary_search_by_key(&md5hash_str(identifier), |v| v.hash)
            .is_ok()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}
