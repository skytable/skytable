/*
 * Created on Sun Feb 18 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use {
    crate::{
        engine::{
            core::{
                index::{DcFieldIndex, PrimaryIndexKey, Row, RowData},
                model::{
                    delta::{DataDelta, DataDeltaKind, DeltaVersion},
                    ModelData,
                },
            },
            data::{
                cell::Datacell,
                tag::{DataTag, TagUnique},
            },
            error::StorageError,
            idx::{MTIndex, STIndex, STIndexSeq},
            storage::{
                common::sdss::sdss_r1::rw::{TrackedReaderContext, TrackedWriter},
                common_encoding::r1,
                v2::raw::{
                    journal::{
                        self, BatchAdapter, BatchAdapterSpec, BatchDriver, JournalAdapterEvent,
                        JournalSettings, RawJournalAdapter,
                    },
                    spec::ModelDataBatchAofV1,
                },
            },
            RuntimeResult,
        },
        util::{compiler::TaggedEnum, EndianQW},
    },
    crossbeam_epoch::{pin, Guard},
    sky_macros::TaggedEnum,
    std::{
        cell::RefCell,
        collections::{hash_map::Entry as HMEntry, HashMap},
        rc::Rc,
    },
};

pub type ModelDriver = BatchDriver<ModelDataAdapter>;
impl ModelDriver {
    pub fn open_model_driver(
        mdl: &ModelData,
        model_data_file_path: &str,
        settings: JournalSettings,
    ) -> RuntimeResult<Self> {
        journal::open_journal(model_data_file_path, mdl, settings)
    }
    /// Create a new event log
    pub fn create_model_driver(model_data_file_path: &str) -> RuntimeResult<Self> {
        journal::create_journal(model_data_file_path)
    }
}

/// The model data adapter (abstract journal adapter impl)
#[derive(Debug)]
pub struct ModelDataAdapter;

#[derive(Debug, PartialEq, Clone, Copy, TaggedEnum)]
#[repr(u8)]
/// The kind of batch
pub enum BatchType {
    /// a standard batch (with n <= m events; n = Δdata, m = cardinality)
    Standard = 0,
}

#[derive(Debug, PartialEq, Clone, Copy, TaggedEnum)]
#[repr(u8)]
/// The type of event *inside* a batch
#[allow(unused)] // TODO(@ohsayan): somehow merge this into delta kind?
pub enum EventType {
    Delete = 0,
    Insert = 1,
    Update = 2,
    /// owing to inconsistent reads, we exited early
    EarlyExit = 3,
}

/*
    persist implementation
    ---
    this section implements persistence for a model data batch. now, there are several special
    cases to handle, for example inconsistent views of the database and such so this might look
    a little messy.
*/

struct RowWriter<'b> {
    f: &'b mut TrackedWriter<<BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec>,
}

impl<'b> RowWriter<'b> {
    /// write global row information:
    /// - pk tag
    /// - schema version
    /// - column count
    fn write_row_global_metadata(&mut self, model: &ModelData) -> RuntimeResult<()> {
        self.f
            .dtrack_write(&[model.p_tag().tag_unique().value_u8()])?;
        self.f.dtrack_write(
            &model
                .delta_state()
                .schema_current_version()
                .value_u64()
                .u64_bytes_le(),
        )?;
        e!(self
            .f
            .dtrack_write(&(model.fields().st_len() - 1).u64_bytes_le()))
    }
    /// write row metadata:
    /// - change type
    /// - txn id
    fn write_row_metadata(
        &mut self,
        change: DataDeltaKind,
        txn_id: DeltaVersion,
    ) -> RuntimeResult<()> {
        if cfg!(debug) {
            let event_kind = EventType::try_from_raw(change.value_u8()).unwrap();
            match (event_kind, change) {
                (EventType::Delete, DataDeltaKind::Delete)
                | (EventType::Insert, DataDeltaKind::Insert)
                | (EventType::Update, DataDeltaKind::Update) => {}
                (EventType::EarlyExit, _) => unreachable!(),
                _ => panic!(),
            }
        }
        let change_type = [change.value_u8()];
        self.f.dtrack_write(&change_type)?;
        let txn_id = txn_id.value_u64().u64_bytes_le();
        self.f.dtrack_write(&txn_id)?;
        Ok(())
    }
    /// encode the primary key only. this means NO TAG is encoded.
    fn write_row_pk(&mut self, pk: &PrimaryIndexKey) -> RuntimeResult<()> {
        match pk.tag() {
            TagUnique::UnsignedInt | TagUnique::SignedInt => {
                let data = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    pk.read_uint()
                }
                .u64_bytes_le();
                self.f.dtrack_write(&data)?;
            }
            TagUnique::Str | TagUnique::Bin => {
                let slice = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    pk.read_bin()
                };
                let slice_l = slice.len().u64_bytes_le();
                self.f.dtrack_write(&slice_l)?;
                self.f.dtrack_write(slice)?;
            }
            TagUnique::Illegal => unsafe {
                // UNSAFE(@ohsayan): a pk can't be constructed with illegal
                impossible!()
            },
        }
        Ok(())
    }
    /// Encode a single cell
    fn write_cell(&mut self, value: &Datacell) -> RuntimeResult<()> {
        let mut buf = vec![];
        r1::obj::cell::encode(&mut buf, value);
        self.f.dtrack_write(&buf)?;
        Ok(())
    }
    /// Encode row data
    fn write_row_data(&mut self, model: &ModelData, row_data: &RowData) -> RuntimeResult<()> {
        for field_name in model.fields().stseq_ord_key() {
            match row_data.fields().get(field_name) {
                Some(cell) => {
                    self.write_cell(cell)?;
                }
                None if field_name.as_str() == model.p_key() => {}
                None => self.f.dtrack_write(&[0])?,
            }
        }
        Ok(())
    }
}

struct BatchWriter<'a, 'b> {
    model: &'a ModelData,
    row_writer: RowWriter<'b>,
    g: &'a Guard,
    sync_count: usize,
}

impl<'a, 'b> BatchWriter<'a, 'b> {
    fn write_batch(
        model: &'a ModelData,
        g: &'a Guard,
        expected: usize,
        f: &'b mut TrackedWriter<<BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec>,
        batch_stat: &mut BatchStats,
    ) -> RuntimeResult<usize> {
        /*
            go over each delta, check if inconsistent and apply if not. if any delta sync fails, we enqueue the delta again.
            Since the diffing algorithm will ensure that a stale delta is never written, we don't have to worry about checking
            if the delta is stale or not manually. It will be picked up by another collection sequence.

            There are several scopes of improvement, but one that is evident here is to try and use a sequential memory block
            rather than remote allocations for the deltas which should theoretically improve performance. But as always, we're bound by
            disk I/O so it might not be a major factor.

            -- @ohsayan
        */
        let mut me = Self::new(model, g, f)?;
        let mut i = 0;
        while i < expected {
            let delta = me.model.delta_state().__data_delta_dequeue(me.g).unwrap();
            match me.step(&delta) {
                Ok(()) => i += 1,
                Err(e) => {
                    // errored, so push this back in; we have written and flushed all prior deltas
                    me.model.delta_state().append_new_data_delta(delta, me.g);
                    batch_stat.set_actual(i);
                    return Err(e);
                }
            }
        }
        Ok(me.sync_count)
    }
    fn new(
        model: &'a ModelData,
        g: &'a Guard,
        f: &'b mut TrackedWriter<<BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec>,
    ) -> RuntimeResult<Self> {
        let mut row_writer = RowWriter { f };
        row_writer.write_row_global_metadata(model)?;
        Ok(Self {
            model,
            row_writer,
            g,
            sync_count: 0,
        })
    }
    fn step(&mut self, delta: &DataDelta) -> RuntimeResult<()> {
        match delta.change() {
            DataDeltaKind::Delete => {
                self.row_writer
                    .write_row_metadata(delta.change(), delta.data_version())?;
                self.row_writer.write_row_pk(delta.row().d_key())?;
            }
            DataDeltaKind::Insert | DataDeltaKind::Update => {
                // resolve deltas (this is yet another opportunity for us to reclaim memory from deleted items)
                let row_data = delta
                    .row()
                    .resolve_schema_deltas_and_freeze_if(self.model.delta_state(), |row| {
                        row.get_txn_revised() <= delta.data_version()
                    });
                if row_data.get_txn_revised() > delta.data_version() {
                    // inconsistent read. there should already be another revised delta somewhere
                    return Ok(());
                }
                self.row_writer
                    .write_row_metadata(delta.change(), delta.data_version())?;
                // encode data
                self.row_writer.write_row_pk(delta.row().d_key())?;
                self.row_writer.write_row_data(self.model, &row_data)?;
            }
        }
        self.row_writer.f.flush_buf()?;
        self.sync_count += 1;
        Ok(())
    }
}

/// A standard model batch where atmost the given number of keys are flushed
pub struct StdModelBatch<'a>(&'a ModelData, usize);

impl<'a> StdModelBatch<'a> {
    pub fn new(model: &'a ModelData, observed_len: usize) -> Self {
        Self(model, observed_len)
    }
}

impl<'a> JournalAdapterEvent<BatchAdapter<ModelDataAdapter>> for StdModelBatch<'a> {
    fn md(&self) -> u64 {
        BatchType::Standard.dscr_u64()
    }
    fn write_direct(
        self,
        writer: &mut TrackedWriter<<BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec>,
        ctx: Rc<RefCell<BatchStats>>,
    ) -> RuntimeResult<()> {
        // [expected commit]
        writer.dtrack_write(&self.1.u64_bytes_le())?;
        let g = pin();
        let actual_commit =
            BatchWriter::write_batch(self.0, &g, self.1, writer, &mut ctx.borrow_mut())?;
        if actual_commit != self.1 {
            // early exit
            writer.dtrack_write(&[EventType::EarlyExit.dscr()])?;
        }
        e!(writer.dtrack_write(&actual_commit.u64_bytes_le()))
    }
}

pub struct FullModel<'a>(&'a ModelData);

impl<'a> FullModel<'a> {
    pub fn new(model: &'a ModelData) -> Self {
        Self(model)
    }
}

impl<'a> JournalAdapterEvent<BatchAdapter<ModelDataAdapter>> for FullModel<'a> {
    fn md(&self) -> u64 {
        BatchType::Standard.dscr_u64()
    }
    fn write_direct(
        self,
        f: &mut TrackedWriter<<BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec>,
        _: Rc<RefCell<BatchStats>>,
    ) -> RuntimeResult<()> {
        let g = pin();
        let mut row_writer: RowWriter<'_> = RowWriter { f };
        let index = self.0.primary_index().__raw_index();
        let current_row_count = index.mt_len();
        // expect commit == current row count
        row_writer
            .f
            .dtrack_write(&current_row_count.u64_bytes_le())?;
        // [pk tag][schema version][column cnt]
        row_writer.write_row_global_metadata(self.0)?;
        for (key, row_data) in index.mt_iter_kv(&g) {
            let row_data = row_data.read();
            row_writer.write_row_metadata(DataDeltaKind::Insert, row_data.get_txn_revised())?;
            row_writer.write_row_pk(key)?;
            row_writer.write_row_data(self.0, &row_data)?;
        }
        // actual commit == current row count
        row_writer
            .f
            .dtrack_write(&current_row_count.u64_bytes_le())?;
        Ok(())
    }
}

/*
    restore implementation
    ---
    the section below implements data restore from a single batch. like the persist impl,
    this is also a fairly complex implementation because some changes, for example deletes
    may need to be applied later due to out-of-order persistence; it is important to postpone
    operations that we're unsure about since a change can appear out of order and we want to
    restore the database to its exact state
*/

/// Per-batch metadata
pub struct BatchMetadata {
    pk_tag: TagUnique,
    schema_version: u64,
    column_count: u64,
}

enum DecodedBatchEventKind {
    Delete,
    Insert(Vec<Datacell>),
    Update(Vec<Datacell>),
}

/// State handling for any pending queries
pub struct BatchRestoreState {
    events: Vec<DecodedBatchEvent>,
}

struct DecodedBatchEvent {
    txn_id: DeltaVersion,
    pk: PrimaryIndexKey,
    kind: DecodedBatchEventKind,
}

impl DecodedBatchEvent {
    fn new(txn_id: u64, pk: PrimaryIndexKey, kind: DecodedBatchEventKind) -> Self {
        Self {
            txn_id: DeltaVersion::__new(txn_id),
            pk,
            kind,
        }
    }
}

pub struct BatchStats {
    actual_commit: usize,
}

impl BatchStats {
    pub fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self { actual_commit: 0 }))
    }
    pub fn into_inner(me: Rc<RefCell<Self>>) -> Self {
        RefCell::into_inner(Rc::into_inner(me).unwrap())
    }
    fn set_actual(&mut self, new: usize) {
        self.actual_commit = new;
    }
    pub fn get_actual(&self) -> usize {
        self.actual_commit
    }
}

impl BatchAdapterSpec for ModelDataAdapter {
    type Spec = ModelDataBatchAofV1;
    type GlobalState = ModelData;
    type BatchType = BatchType;
    type EventType = EventType;
    type BatchMetadata = BatchMetadata;
    type BatchState = BatchRestoreState;
    type CommitContext = Rc<RefCell<BatchStats>>;
    fn is_early_exit(event_type: &Self::EventType) -> bool {
        EventType::EarlyExit.eq(event_type)
    }
    fn initialize_batch_state(_: &Self::GlobalState) -> Self::BatchState {
        BatchRestoreState { events: Vec::new() }
    }
    fn decode_batch_metadata(
        _: &Self::GlobalState,
        f: &mut TrackedReaderContext<Self::Spec>,
        batch_type: Self::BatchType,
    ) -> RuntimeResult<Self::BatchMetadata> {
        // [pk tag][schema version][column cnt]
        match batch_type {
            BatchType::Standard => {}
        }
        let pk_tag = TagUnique::try_from_raw(f.read_block().map(|[b]| b)?)
            .ok_or(StorageError::InternalDecodeStructureIllegalData)?;
        let schema_version = u64::from_le_bytes(f.read_block()?);
        let column_count = u64::from_le_bytes(f.read_block()?);
        Ok(BatchMetadata {
            pk_tag,
            schema_version,
            column_count,
        })
    }
    fn update_state_for_new_event(
        _: &Self::GlobalState,
        bs: &mut Self::BatchState,
        f: &mut TrackedReaderContext<Self::Spec>,
        batch_info: &Self::BatchMetadata,
        event_type: Self::EventType,
    ) -> RuntimeResult<()> {
        // get txn id
        let txn_id = u64::from_le_bytes(f.read_block()?);
        // get pk
        let pk = restore_impls::decode_primary_key::<Self::Spec>(f, batch_info.pk_tag)?;
        match event_type {
            EventType::Delete => {
                bs.events.push(DecodedBatchEvent::new(
                    txn_id,
                    pk,
                    DecodedBatchEventKind::Delete,
                ));
            }
            EventType::Insert | EventType::Update => {
                // insert or update
                // prepare row
                let row = restore_impls::decode_row_data(batch_info, f)?;
                if event_type == EventType::Insert {
                    bs.events.push(DecodedBatchEvent::new(
                        txn_id,
                        pk,
                        DecodedBatchEventKind::Insert(row),
                    ));
                } else {
                    bs.events.push(DecodedBatchEvent::new(
                        txn_id,
                        pk,
                        DecodedBatchEventKind::Update(row),
                    ));
                }
            }
            EventType::EarlyExit => unreachable!(),
        }
        Ok(())
    }
    fn finish(
        batch_state: Self::BatchState,
        batch_md: Self::BatchMetadata,
        gs: &Self::GlobalState,
    ) -> RuntimeResult<()> {
        /*
            go over each change in this batch, resolve conflicts and then apply to global state
        */
        let g = unsafe { crossbeam_epoch::unprotected() };
        let mut pending_delete = HashMap::new();
        let p_index = gs.primary_index().__raw_index();
        let m = gs;
        let mut real_last_txn_id = DeltaVersion::genesis();
        for DecodedBatchEvent { txn_id, pk, kind } in batch_state.events {
            match kind {
                DecodedBatchEventKind::Insert(new_row) | DecodedBatchEventKind::Update(new_row) => {
                    let popped_row = p_index.mt_delete_return(&pk, &g);
                    if let Some(row) = popped_row {
                        /*
                            if a newer version of the row is received first and the older version is pending to be synced, the older
                            version is never synced. this is how the diffing algorithm works to ensure consistency.
                            the delta diff algorithm statically guarantees this.
                        */
                        let row_txn_revised = row.read().get_txn_revised();
                        assert!(
                            row_txn_revised.value_u64() == 0 || row_txn_revised < txn_id,
                            "revised ID is {} but our row has version {}",
                            row.read().get_txn_revised().value_u64(),
                            txn_id.value_u64()
                        );
                    }
                    if txn_id > real_last_txn_id {
                        real_last_txn_id = txn_id;
                    }
                    let mut data = DcFieldIndex::default();
                    for (field_name, new_data) in m
                        .fields()
                        .stseq_ord_key()
                        .filter(|key| key.as_str() != m.p_key())
                        .zip(new_row)
                    {
                        data.st_insert(
                            unsafe {
                                // UNSAFE(@ohsayan): model in scope, we're good
                                field_name.clone()
                            },
                            new_data,
                        );
                    }
                    let row = Row::new_restored(
                        pk,
                        data,
                        DeltaVersion::__new(batch_md.schema_version),
                        txn_id,
                    );
                    // resolve any deltas
                    let _ = row.resolve_schema_deltas_and_freeze(m.delta_state());
                    // put it back in (lol); blame @ohsayan for this joke
                    p_index.mt_insert(row, &g);
                }
                DecodedBatchEventKind::Delete => {
                    /*
                        due to the concurrent nature of the engine, deletes can "appear before" an insert or update and since
                        we don't store deleted txn ids, we just put this in a pending list.
                    */
                    match pending_delete.entry(pk) {
                        HMEntry::Occupied(mut existing_delete) => {
                            if *existing_delete.get() > txn_id {
                                /*
                                    this is a "newer delete" and it takes precedence. basically the same key was
                                    deleted by two txns but they were only synced much later, and out of order.
                                */
                                continue;
                            }
                            // the existing delete happened before our delete, so our delete takes precedence
                            // we have a newer delete for the same key
                            *existing_delete.get_mut() = txn_id;
                        }
                        HMEntry::Vacant(new) => {
                            // we never deleted this
                            new.insert(txn_id);
                        }
                    }
                }
            }
        }
        // apply pending deletes; all our conflicts would have been resolved by now
        for (pk, txn_id) in pending_delete {
            if txn_id > real_last_txn_id {
                real_last_txn_id = txn_id;
            }
            match p_index.mt_get(&pk, &g) {
                Some(row) => {
                    if row.read().get_txn_revised() > txn_id {
                        // our delete "happened before" this row was inserted
                        continue;
                    }
                    // yup, go ahead and chuck it
                    let _ = p_index.mt_delete(&pk, &g);
                }
                None => {
                    // if we reach here it basically means that both an (insert and/or update) and a delete
                    // were present as part of the same diff and the delta algorithm ignored the insert/update
                    // in this case, we do nothing
                }
            }
        }
        // +1 since it is a fetch add!
        m.delta_state()
            .__set_delta_version(DeltaVersion::__new(real_last_txn_id.value_u64() + 1));
        Ok(())
    }
}

mod restore_impls {
    use {
        super::BatchMetadata,
        crate::{
            engine::{
                core::index::PrimaryIndexKey,
                data::{cell::Datacell, tag::TagUnique},
                error::StorageError,
                storage::{
                    common::sdss::sdss_r1::{rw::TrackedReaderContext, FileSpecV1},
                    common_encoding::r1::{
                        obj::cell::{self, StorageCellTypeID},
                        DataSource,
                    },
                    v2::raw::spec::ModelDataBatchAofV1,
                },
                RuntimeResult,
            },
            util::compiler::TaggedEnum,
        },
        std::mem::ManuallyDrop,
    };
    /// Primary key decode impl
    ///
    /// NB: We really need to make this generic, but for now we can settle for this
    pub fn decode_primary_key<S: FileSpecV1>(
        f: &mut TrackedReaderContext<S>,
        pk_type: TagUnique,
    ) -> RuntimeResult<PrimaryIndexKey> {
        Ok(match pk_type {
            TagUnique::SignedInt | TagUnique::UnsignedInt => {
                let qw = u64::from_le_bytes(f.read_block()?);
                unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    PrimaryIndexKey::new_from_qw(pk_type, qw)
                }
            }
            TagUnique::Str | TagUnique::Bin => {
                let len = u64::from_le_bytes(f.read_block()?);
                let mut data = vec![0; len as usize];
                f.read(&mut data)?;
                if pk_type == TagUnique::Str {
                    if core::str::from_utf8(&data).is_err() {
                        return Err(StorageError::InternalDecodeStructureCorruptedPayload.into());
                    }
                }
                unsafe {
                    // UNSAFE(@ohsayan): +tagck +verityck
                    let mut md = ManuallyDrop::new(data);
                    PrimaryIndexKey::new_from_dual(pk_type, len, md.as_mut_ptr() as usize)
                }
            }
            TagUnique::Illegal => unsafe {
                // UNSAFE(@ohsayan): TagUnique::try_from_raw rejects an construction with Invalid as the dscr
                impossible!()
            },
        })
    }
    pub fn decode_row_data(
        batch_info: &BatchMetadata,
        f: &mut TrackedReaderContext<ModelDataBatchAofV1>,
    ) -> Result<Vec<Datacell>, crate::engine::fractal::error::Error> {
        let mut row = vec![];
        let mut this_col_cnt = batch_info.column_count;
        while this_col_cnt != 0 {
            let Some(dscr) = StorageCellTypeID::try_from_raw(f.read_block().map(|[b]| b)?) else {
                return Err(StorageError::InternalDecodeStructureIllegalData.into());
            };
            let cell = unsafe { cell::decode_element::<Datacell, _>(f, dscr) }.map_err(|e| e.0)?;
            row.push(cell);
            this_col_cnt -= 1;
        }
        Ok(row)
    }

    /*
        this is some silly ridiculous hackery because of some of our legacy code. basically an attempt is made to directly coerce error types.
        we'll make this super generic so that no more of this madness is needed
    */
    pub struct ErrorHack(crate::engine::fractal::error::Error);
    impl From<crate::engine::fractal::error::Error> for ErrorHack {
        fn from(value: crate::engine::fractal::error::Error) -> Self {
            Self(value)
        }
    }
    impl From<std::io::Error> for ErrorHack {
        fn from(value: std::io::Error) -> Self {
            Self(value.into())
        }
    }
    impl From<()> for ErrorHack {
        fn from(_: ()) -> Self {
            Self(StorageError::InternalDecodeStructureCorrupted.into())
        }
    }
    impl<'a> DataSource for TrackedReaderContext<'a, ModelDataBatchAofV1> {
        const RELIABLE_SOURCE: bool = false;
        type Error = ErrorHack;
        fn has_remaining(&self, cnt: usize) -> bool {
            self.remaining() >= cnt as u64
        }
        unsafe fn read_next_byte(&mut self) -> Result<u8, Self::Error> {
            Ok(self.read_next_block::<1>()?[0])
        }
        unsafe fn read_next_block<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
            Ok(self.read_block()?)
        }
        unsafe fn read_next_u64_le(&mut self) -> Result<u64, Self::Error> {
            self.read_next_block().map(u64::from_le_bytes)
        }
        unsafe fn read_next_variable_block(&mut self, size: usize) -> Result<Vec<u8>, Self::Error> {
            let mut buf = vec![0; size];
            self.read(&mut buf)?;
            Ok(buf)
        }
    }
}
