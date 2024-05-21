use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, PartialOrd, Deserialize, Serialize, Clone, Copy)]
pub enum EdgeType {
    AliasTo,
    AliasFrom,
    Contains,
    ContainedBy,
    BuildsTo,
    BuiltFrom,
}

// #[derive(Debug, PartialEq, PartialOrd, Deserialize, Serialize, Clone)]
// pub struct Edge {
//     pub connection: String,
//     pub edge_type: EdgeType,
// }

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct ItemMetaData {
    pub file_names: Vec<String>,
    pub file_type: String,
    pub file_sub_type: String,
    pub extra: HashMap<String, String>,
}

impl ItemMetaData {
    pub fn merge(&self, other: ItemMetaData) -> ItemMetaData {
        let my_names: HashSet<String> = HashSet::from_iter(self.file_names.clone().into_iter()); //  = Set(self.fileNames)
        let whats_left: HashSet<String> = other
            .file_names
            .into_iter()
            .filter(|s| my_names.contains(s))
            .collect();

        let extra_left: HashMap<&String, &String> = other
            .extra
            .iter()
            .filter(|(k, v)| self.extra.get(*k) == Some(v))
            .collect();

        if whats_left.is_empty() && extra_left.is_empty() {
            self.clone()
        } else {
            ItemMetaData {
                file_names: {
                    let mut mine: Vec<String> = (my_names.union(&whats_left))
                        .into_iter()
                        .map(|v| v.clone())
                        .collect();
                    mine.sort();
                    mine
                },
                file_type: self.file_type.clone(),
                file_sub_type: self.file_sub_type.clone(),
                extra: {
                    let mut mine = self.extra.clone();
                    mine.extend(extra_left.into_iter().map(|(a, b)| (a.clone(), b.clone())));
                    mine
                },
            }
        }
    }

    //   pub fn from(
    //     file_name: String,
    //     file_type: FileType,
    //     package_identifier: Option<PackageIdentifier>
    // ) -> ItemMetaData {
    //   match package_identifier {
    //     Some(
    //           pid @ PackageIdentifier(protocol, groupId, artifactId, version)
    //         ) =>
    //       ItemMetaData{
    //         file_names: vec![fileName],
    //         file_type: "package".into(),
    //         file_sub_type: protocol.name,
    //         extra: todo!("merge maps") // fileType.toStringMap() ++ Map("purl" -> pid.purl()) ++ pid.toStringMap()
    //       },
    //     None =>
    //       ItemMetaData(
    //         fileNames = Vector(fileName),
    //         fileType = fileType.typeName().getOrElse("Unknown"),
    //         fileSubType = fileType.subType().getOrElse(""),
    //         extra = fileType.toStringMap()
    //       )
    //   }
}

pub type LocationReference = (u64, u64);
pub type Edge = (String, EdgeType);

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Item {
    pub identifier: String,
    pub reference: LocationReference,
    pub alt_identifiers: Vec<String>,
    pub connections: Vec<Edge>,
    pub previous_reference: Option<LocationReference>,
    pub metadata: Option<ItemMetaData>,
    pub merged_from: Vec<Item>,
    pub _timestamp: i64,
    pub _version: u32,
    pub _type: String,
}

impl Item {
    pub fn merge(&self, _others: Vec<Item>) -> Item {
        todo!("Write merge");

        // val finalMetaData = others.foldLeft(this.metadata) { (a, b) =>
        //   (a, b.metadata) match {
        //     case (Some(me), Some(them)) => Some(me.merge(them))
        //     case (None, Some(them))     => Some(them)
        //     case (Some(me), None)       => Some(me)
        //     case _                      => None
        //   }
        // }

        // val myAlts = Set(this.altIdentifiers: _*)
        // val finalAlts = others.foldLeft(myAlts) { (me, them) =>
        //   val theirAlts = Set(them.altIdentifiers: _*)
        //   me ++ theirAlts
        // }

        // val myConnections = Set(this.connections: _*)
        // val finalConnections = others.foldLeft(myConnections) { (me, them) =>
        //   val theirCons = Set(them.connections: _*)
        //   me ++ theirCons
        // }
        // // nothing new
        // if (
        //   myAlts == finalAlts && this.metadata == finalMetaData && myConnections == finalConnections
        // ) {
        //   this
        // } else {
        //   val notStored = this.reference == ItemReference.noop
        //   this.copy(
        //     altIdentifiers = finalAlts.toVector.sorted,
        //     connections = finalConnections.toVector.sorted,
        //     previousReference = if (notStored) None else Some(this.reference),
        //     metadata = finalMetaData,
        //     mergedFrom = if (notStored) Vector() else others,
        //     _timestamp = System.currentTimeMillis(),
        //     _version = 1
        //   )
        // }
    }
}
