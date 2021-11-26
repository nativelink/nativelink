// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use action_messages::ActionInfo;
use error::{make_err, Code, Error};
use platform_property_manager::{PlatformProperties, PlatformPropertyValue};
use proto::com::github::allada::rust_cas::remote_execution::{update_for_worker, StartExecute, UpdateForWorker};
use tokio::sync::mpsc::UnboundedSender;

/// Unique id of worker.
pub type WorkerId = String;

/// Notifications to send worker about a requested state change.
pub enum WorkerUpdate {
    /// Requests that the worker begin executing this action.
    RunAction(Arc<ActionInfo>),
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
}

impl Worker {
    pub fn new(id: WorkerId, platform_properties: PlatformProperties, tx: UnboundedSender<UpdateForWorker>) -> Self {
        Self {
            id,
            platform_properties,
            tx,
        }
    }

    /// Notifies the worker of a requested state change.
    pub fn notify_update(&mut self, worker_update: WorkerUpdate) -> Result<(), Error> {
        match worker_update {
            WorkerUpdate::RunAction(action_info) => self.run_action(action_info),
        }
    }

    fn send_msg_to_worker(&mut self, msg: update_for_worker::Update) -> Result<(), Error> {
        self.tx
            .send(UpdateForWorker { update: Some(msg) })
            .map_err(|_| make_err!(Code::Internal, "Worker disconnected"))
    }

    fn run_action(&mut self, action_info: Arc<ActionInfo>) -> Result<(), Error> {
        self.reduce_platform_properties(&action_info.platform_properties);
        self.send_msg_to_worker(update_for_worker::Update::StartAction(StartExecute {
            execute_request: Some(action_info.as_ref().into()),
        }))
    }

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
