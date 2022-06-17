// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use uuid::Uuid;

use action_messages::ActionInfo;
use error::{make_err, make_input_err, Code, Error, ResultExt};
use platform_property_manager::{PlatformProperties, PlatformPropertyValue};
use proto::com::github::allada::turbo_cache::remote_execution::{
    update_for_worker, ConnectionResult, StartExecute, UpdateForWorker,
};
use tokio::sync::mpsc::UnboundedSender;

pub type WorkerTimestamp = u64;

/// Unique id of worker.
#[derive(Eq, PartialEq, Hash, Copy, Clone)]
pub struct WorkerId(pub u128);

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = Uuid::encode_buffer();
        let worker_id_str = Uuid::from_u128(self.0).to_hyphenated().encode_lower(&mut buf);
        write!(f, "{}", worker_id_str)
    }
}

impl std::fmt::Debug for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = Uuid::encode_buffer();
        let worker_id_str = Uuid::from_u128(self.0).to_hyphenated().encode_lower(&mut buf);
        f.write_str(worker_id_str)
    }
}

impl TryFrom<String> for WorkerId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        match Uuid::parse_str(&s) {
            Err(e) => Err(make_input_err!(
                "Failed to convert string to WorkerId : {} : {:?}",
                s,
                e
            )),
            Ok(my_uuid) => Ok(WorkerId(my_uuid.as_u128())),
        }
    }
}

/// Notifications to send worker about a requested state change.
pub enum WorkerUpdate {
    /// Requests that the worker begin executing this action.
    RunAction(Arc<ActionInfo>),

    /// Request that the worker is no longer in the pool and may discard any jobs.
    Disconnect,
}

/// Represents a connection to a worker and used as the medium to
/// interact with the worker from the client/scheduler.
pub struct Worker {
    /// Unique identifier of the worker.
    pub id: WorkerId,

    /// Properties that describe the capabilities of this worker.
    pub platform_properties: PlatformProperties,

    /// Channel to send commands from scheduler to worker.
    pub tx: UnboundedSender<UpdateForWorker>,

    /// The action info of the running action if worker is assigned one.
    pub running_action_info: Option<Arc<ActionInfo>>,

    /// Timestamp of last time this worker had been communicated with.
    // Warning: Do not update this timestamp without updating the placement of the worker in
    // the LRUCache in the Workers struct.
    pub last_update_timestamp: WorkerTimestamp,
}

impl Worker {
    pub fn new(
        id: WorkerId,
        platform_properties: PlatformProperties,
        tx: UnboundedSender<UpdateForWorker>,
        timestamp: WorkerTimestamp,
    ) -> Self {
        Self {
            id,
            platform_properties,
            tx,
            running_action_info: None,
            last_update_timestamp: timestamp,
        }
    }

    /// Sends the initial connection information to the worker. This generally is just meta info.
    /// This should only be sent once and should always be the first item in the stream.
    pub fn send_initial_connection_result(&mut self) -> Result<(), Error> {
        self.send_msg_to_worker(update_for_worker::Update::ConnectionResult(ConnectionResult {
            worker_id: self.id.to_string(),
        }))
        .err_tip(|| format!("Failed to send ConnectionResult to worker : {}", self.id))
    }

    /// Notifies the worker of a requested state change.
    pub fn notify_update(&mut self, worker_update: WorkerUpdate) -> Result<(), Error> {
        match worker_update {
            WorkerUpdate::RunAction(action_info) => self.run_action(action_info),
            WorkerUpdate::Disconnect => self.send_msg_to_worker(update_for_worker::Update::Disconnect(())),
        }
    }

    pub fn keep_alive(&mut self) -> Result<(), Error> {
        self.send_msg_to_worker(update_for_worker::Update::KeepAlive(()))
            .err_tip(|| format!("Failed to send KeepAlive to worker : {}", self.id))
    }

    fn send_msg_to_worker(&mut self, msg: update_for_worker::Update) -> Result<(), Error> {
        self.tx
            .send(UpdateForWorker { update: Some(msg) })
            .map_err(|_| make_err!(Code::Internal, "Worker disconnected"))
    }

    fn run_action(&mut self, action_info: Arc<ActionInfo>) -> Result<(), Error> {
        let action_info_clone = action_info.as_ref().clone();
        self.running_action_info = Some(action_info.clone());
        self.reduce_platform_properties(&action_info.platform_properties);
        self.send_msg_to_worker(update_for_worker::Update::StartAction(StartExecute {
            execute_request: Some(action_info_clone.into()),
            salt: *action_info.salt(),
        }))
    }

    /// Reduces the platform properties available on the worker based on the platform properties provided.
    /// This is used because we allow more than 1 job to run on a worker at a time, and this is how the
    /// scheduler knows if more jobs can run on a given worker.
    fn reduce_platform_properties(&mut self, props: &PlatformProperties) {
        debug_assert!(props.is_satisfied_by(&self.platform_properties));
        for (property, prop_value) in &props.properties {
            if let PlatformPropertyValue::Minimum(value) = prop_value {
                let worker_props = &mut self.platform_properties.properties;
                if let &mut PlatformPropertyValue::Minimum(worker_value) = &mut worker_props.get_mut(property).unwrap()
                {
                    *worker_value -= value;
                }
            }
        }
    }

    pub fn restore_platform_properties(&mut self, props: &PlatformProperties) {
        for (property, prop_value) in &props.properties {
            if let PlatformPropertyValue::Minimum(value) = prop_value {
                let worker_props = &mut self.platform_properties.properties;
                if let &mut PlatformPropertyValue::Minimum(worker_value) = &mut worker_props.get_mut(property).unwrap()
                {
                    *worker_value += value;
                }
            }
        }
    }
}

impl PartialEq for Worker {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Worker {}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
