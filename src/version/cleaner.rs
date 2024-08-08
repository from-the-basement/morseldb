use std::{collections::BTreeMap, io, marker::PhantomData, sync::Arc};

use flume::{Receiver, Sender};

use crate::{
    fs::{FileId, FileProvider},
    timestamp::Timestamp,
    DbOption,
};
use crate::record::Record;

pub enum CleanTag {
    Add { ts: Timestamp, gens: Vec<FileId> },
    Clean { ts: Timestamp },
}

pub(crate) struct Cleaner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    tag_recv: Receiver<CleanTag>,
    gens_map: BTreeMap<Timestamp, (Vec<FileId>, bool)>,
    option: Arc<DbOption<R>>,
    _p: PhantomData<FP>,
}

impl<R, FP> Cleaner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn new(option: Arc<DbOption<R>>) -> (Self, Sender<CleanTag>) {
        let (tag_send, tag_recv) = flume::bounded(option.clean_channel_buffer);

        (
            Cleaner {
                tag_recv,
                gens_map: Default::default(),
                option,
                _p: Default::default(),
            },
            tag_send,
        )
    }

    pub(crate) async fn listen(&mut self) -> Result<(), io::Error> {
        while let Ok(tag) = self.tag_recv.recv_async().await {
            match tag {
                CleanTag::Add { ts, gens } => {
                    let _ = self.gens_map.insert(ts, (gens, false));
                }
                CleanTag::Clean { ts: version_num } => {
                    if let Some((_, dropped)) = self.gens_map.get_mut(&version_num) {
                        *dropped = true;
                    }
                    while let Some((first_version, (gens, dropped))) = self.gens_map.pop_first() {
                        if !dropped {
                            let _ = self.gens_map.insert(first_version, (gens, false));
                            break;
                        }
                        for gen in gens {
                            FP::remove(self.option.table_path(&gen)).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

// TODO: TestCase
