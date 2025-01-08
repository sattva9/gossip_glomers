use std::collections::{HashMap, HashSet};

use serde::{
    de::{self, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub msg_type: MessageType,
}

impl MessageBody {
    pub fn with_type(msg_type: MessageType) -> Self {
        Self {
            msg_id: None,
            in_reply_to: None,
            msg_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageType {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Error {
        code: u32,
        text: String,
    },

    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },

    Generate,
    GenerateOk {
        id: String,
    },

    Broadcast {
        message: i64,
    },
    BroadcastOk,
    BroadcastMany {
        messages: HashSet<i64>,
    },
    BroadcastManyOk,
    Read {
        key: Option<String>,
    },
    ReadOk {
        #[serde(skip_serializing_if = "Option::is_none")]
        messages: Option<HashSet<i64>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,

    Add {
        delta: i64,
    },
    AddOk,

    Send {
        key: String,
        msg: i64,
    },
    SendOk {
        offset: i64,
    },
    Poll {
        offsets: HashMap<String, i64>,
    },
    PollOk {
        msgs: HashMap<String, Vec<[i64; 2]>>,
    },
    CommitOffsets {
        offsets: HashMap<String, i64>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, i64>,
    },

    Txn {
        txn: Vec<Transaction>,
    },
    TxnOk {
        txn: Vec<Transaction>,
    },

    Cas {
        key: String,
        from: Value,
        to: Value,
        create_if_not_exists: Option<bool>,
    },
    CasOk,

    Write {
        key: String,
        value: Value,
    },
    WriteOk,
}

#[derive(Debug, Clone)]
pub enum Transaction {
    Read { key: u64, val: Value },
    Write { key: u64, value: i64 },
    Append { key: u64, value: i64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    None,
    Int(i64),
    Vec(Vec<i64>),
    Map(HashMap<String, Vec<i64>>),
    String(String),
}

impl Value {
    pub fn as_int(self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_vec(self) -> Option<Vec<i64>> {
        match self {
            Self::Vec(v) => Some(v),
            _ => None,
        }
    }
}

impl Serialize for Transaction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        match &self {
            Transaction::Read { key, val: value } => {
                seq.serialize_element("r")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
            Transaction::Write { key, value } => {
                seq.serialize_element("w")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
            Transaction::Append { key, value } => {
                seq.serialize_element("append")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Transaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Value::deserialize(deserializer).and_then(|value| match value {
        //     Value::Array(data) => match &data[0] {
        //         Value::String(t) => {
        //             if t.eq("r") {
        //                 let key = data[1].as_u64().unwrap();
        //                 let value = data[2].as_i64();
        //                 Ok(Transaction::Read { key, value })
        //             } else {
        //                 let key = data[1].as_u64().unwrap();
        //                 let value = data[2].as_i64().unwrap();
        //                 Ok(Transaction::Write { key, value })
        //             }
        //         }
        //         _ => Err(serde::de::Error::custom("failed to de Transaction")),
        //     },
        //     _ => Err(serde::de::Error::custom("failed to de Transaction")),
        // })
        struct InstanceVisitor;

        impl<'de> Visitor<'de> for InstanceVisitor {
            type Value = Transaction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("expected Transaction, which is array of ['r', 4, null]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let op: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                match op.as_str() {
                    "r" => {
                        let value = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                        Ok(Transaction::Read { key, val: value })
                    }
                    "w" => {
                        let value = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                        Ok(Transaction::Write { key, value })
                    }
                    "append" => {
                        let value = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                        Ok(Transaction::Append { key, value })
                    }
                    _ => Err(de::Error::custom("Inavlid op")),
                }
            }
        }
        deserializer.deserialize_any(InstanceVisitor)
    }
}
