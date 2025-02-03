use std::collections::HashMap;
use bitvec::{bitvec, prelude::Lsb0, view::BitView};
use module_system::{Handler, ModuleRef, System};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use bitvec::vec::BitVec;
use uuid::Uuid;

/// A source of randomness.
pub(crate) trait RandomnessSource {
    /// Generates a next pseudo-random u32 value selected
    /// from a uniform distribution.
    fn next_u32(&mut self) -> u32;
}

/// A conflict-free state-based replicated counter.
pub(crate) trait ConflictFreeReplicatedCounter<T> {
    /// Sets a given counter so that it counts
    /// no elements.
    fn set_to_zero(&mut self);

    /// Sets a given counter so that it counts
    /// an infinite number of elements (all possible).
    fn set_to_infinity(&mut self);

    /// Adds one more element to a given counter
    /// (increments the counter by one) by one using
    /// a given source of randomness.
    /// If the counter counts an infinite number of elements,
    /// an `Err` is returned and the given counter remains
    /// intact; otherwise, `Ok` is returned.
    fn try_count_one_more_element(&mut self, rs: &mut dyn RandomnessSource) -> Result<(), String>;

    /// Merges another counter with a given counter,
    /// so that, as a result, the given counter counts
    /// elements counted originally by both itself
    /// and the other counter. If the two counters are
    /// incompatible, `Err` is returned and the given
    /// counter remains intact; otherwise, `Ok` is returned.
    fn try_merge_with(&mut self, other: &Self) -> Result<(), String>;

    /// Returns the number of elements counted
    /// by a given counter.
    fn evaluate(&self) -> T;
}

/// An implementation of a probabilistic counting sketch.
#[derive(Clone, Debug)]
pub(crate) struct ProbabilisticCounter {
    sketches: Vec<BitVec<u8, Lsb0>>,
}

impl ProbabilisticCounter {
    /// The scaling factor used in probabilistic counting.
    const SCALING_FACTOR: f64 = 1.29281;

    /// Creates a new probabilistic counter
    /// with a given number of sketch instances and
    /// bits per instance. The counter
    /// counts no elements.
    pub(crate) fn new_zero(bits_per_instance: usize, num_instances: usize) -> Self {
        assert!(num_instances > 0);
        assert!(bits_per_instance > 0 && bits_per_instance <= u32::BITS as usize);
        assert_eq!(bits_per_instance % 8, 0);

        let sketches = vec![bitvec![u8, Lsb0; 0; bits_per_instance]; num_instances];
        Self { sketches }
    }

    /// Creates a new probabilistic counter
    /// with the same configuration as a given one.
    /// The new counter counts no elements.
    pub(crate) fn new_zero_with_same_config(other: &ProbabilisticCounter) -> Self {
        ProbabilisticCounter::new_zero(other.get_num_bits_per_instance(), other.get_num_instances())
    }

    /// Returns the number of sketch instances utilized
    /// by a given probabilistic counter.
    pub(crate) fn get_num_instances(&self) -> usize {
        self.sketches.len()
    }

    /// Returns the number of bits per sketch instance
    /// utilized by a given probabilistic counter.
    pub(crate) fn get_num_bits_per_instance(&self) -> usize {
        self.sketches[0].len()
    }

    /// Given an u32 bit number drawn at random from a
    /// uniform distribution produces a number from
    /// a geometric distribution with probability 1/2.
    /// The second parameter denotes the number of bits
    /// of the number that should be used.
    /// This function shall be used for selecting bits for
    /// incrementation of the sketches.
    pub(crate) fn uniform_u32_to_geometric(rand_no: u32, num_bits: usize) -> u32 {
        let rand_val = (rand_no as u64) & ((1_u64 << num_bits) - 1);
        let first_one = rand_val.view_bits::<Lsb0>().first_one();
        match first_one {
            None => 1,
            Some(idx) => idx as u32,
        }
    }

    /// Returns a given bit in a given instance of a given sketch.
    #[cfg(test)]
    pub(crate) fn get_bit(&self, instance_idx: usize, in_instance_bit_idx: usize) -> bool {
        self.sketches[instance_idx][in_instance_bit_idx]
    }

    /// Sets a given bit in a given instance of a given sketch
    /// to the value provided as a parameter.
    #[cfg(test)]
    pub(crate) fn set_bit(&mut self, instance_idx: usize, in_instance_bit_idx: usize, val: bool) {
        self.sketches[instance_idx].set(in_instance_bit_idx, val);
    }

    /// Returns a uniform random value that leads to
    /// setting a specific bit in the counter. In principle,
    /// this is used to partially revert function
    /// `uniform_u32_to_geometric` for testing.
    #[cfg(test)]
    pub(crate) fn geometric_to_sample_u32(geom_no: u32) -> u32 {
        assert!(geom_no < u32::BITS);
        1_u32 << geom_no
    }
}

impl ConflictFreeReplicatedCounter<u64> for ProbabilisticCounter {
    fn set_to_zero(&mut self) {
        self.sketches = vec![bitvec![u8, Lsb0; 0; self.get_num_bits_per_instance()]; self.get_num_instances()];
    }

    fn set_to_infinity(&mut self) {
        self.sketches = vec![bitvec![u8, Lsb0; 1; self.get_num_bits_per_instance()]; self.get_num_instances()];
    }

    fn try_count_one_more_element(&mut self, rs: &mut dyn RandomnessSource) -> Result<(), String> {
        if self.evaluate() == u64::MAX {
            return Err("Counter is at infinity.".to_string());
        }

        let num_bits = self.get_num_bits_per_instance();
        for sketch in self.sketches.iter_mut() {
            let random_value = rs.next_u32();
            let random_index = Self::uniform_u32_to_geometric(random_value, num_bits) as usize;
            sketch.set(random_index,true);
        }

        Ok(())
    }

    fn try_merge_with(&mut self, other: &Self) -> Result<(), String> {
        if self.get_num_instances() != other.get_num_instances() {
            return Err("Different number of instances.".to_string())
        }
        if self.get_num_bits_per_instance() != other.get_num_bits_per_instance() {
            return Err("Different number of bits per instance.".to_string())
        }

        for (i, sketch) in self.sketches.iter_mut().enumerate() {
            *sketch |= &other.sketches[i];
        }

        Ok(())
    }

    fn evaluate(&self) -> u64 {
        if self.sketches.iter().all(|sketch| sketch.not_any()) {
            return 0;
        }

        let mut fz = vec![0u32; self.get_num_instances()];

        for (i, sketch) in self.sketches.iter().enumerate() {
            for (j, bit) in sketch.iter().enumerate() {
                if !bit {
                    fz[i] = j as u32;
                    break;
                }
                if j == self.get_num_bits_per_instance() - 1 {
                    return u64::MAX;
                }
            }
        }

        let sum = fz.iter().sum::<u32>();
        let avg = f64::from(sum) / self.get_num_instances() as f64;
        let result = (Self::SCALING_FACTOR * 2f64.powf(avg)).round() as u64;
        result
    }
}

/// A service allowing for sampling random nodes
/// from the system for gossiping.
pub(crate) trait PeerSamplingService {
    /// Returns a reference to a random Node
    /// in the system.
    fn get_random_peer(&mut self) -> ModuleRef<Node>;
}

/// A node (process) in the system.
pub(crate) struct Node {
    uuid: Uuid,
    rs: Box<dyn RandomnessSource + Send>,
    pss: Box<dyn PeerSamplingService + Send>,
    counters: HashMap<Uuid, ProbabilisticCounter>,
    predicates: HashMap<Uuid, (SystemTime, Arc<dyn Fn(&Uuid) -> bool + Send + Sync>)>,
}

/// A message used by a client to install
/// a query on a node.
pub(crate) struct QueryInstallMsg {
    pub(crate) bits_per_instance: usize,
    pub(crate) num_instances: usize,
    pub(crate) predicate: Arc<dyn Fn(&Uuid) -> bool + Send + Sync>,
}

/// A message used by a client to poll a node
/// to provide its current estimate of the query value.
pub(crate) struct QueryResultPollMsg {
    pub(crate) initiator: Uuid,
    pub(crate) callback: QueryResultPollCallback,
}

pub(crate) type QueryResultPollCallback =
    Box<dyn FnOnce(Option<u64>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// A message that triggers a node to initiate
/// gossiping.
pub(crate) struct SyncTriggerMsg {}

/// A gossip message sent between two nodes.
pub(crate) struct SyncGossipMsg {
    counters: HashMap<Uuid, ProbabilisticCounter>,
    predicates: HashMap<Uuid, (SystemTime, Arc<dyn Fn(&Uuid) -> bool + Send + Sync>)>,
}

impl Node {
    pub(crate) async fn new(
        system: &mut System,
        uuid: Uuid,
        rs: Box<dyn RandomnessSource + Send>,
        pss: Box<dyn PeerSamplingService + Send>,
    ) -> ModuleRef<Self> {
        let self_ref = system
            .register_module(Self {
                uuid,
                rs,
                pss,
                counters: HashMap::new(),
                predicates: HashMap::new(),
            })
            .await;
        self_ref
    }
}

#[async_trait::async_trait]
impl Handler<QueryInstallMsg> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: QueryInstallMsg) {
        if msg.bits_per_instance == 0
            || msg.bits_per_instance > u32::BITS as usize
            || msg.bits_per_instance % 8 != 0
            || msg.num_instances == 0
        {
            return;
        }
        let timestamp = SystemTime::now();

        let mut counter = ProbabilisticCounter::new_zero(msg.bits_per_instance, msg.num_instances);
        if (msg.predicate)(&self.uuid) {
            counter.try_count_one_more_element(&mut *self.rs).unwrap();
        }

        self.counters.insert(self.uuid, counter);
        self.predicates.insert(self.uuid, (timestamp, msg.predicate));
    }
}

#[async_trait::async_trait]
impl Handler<QueryResultPollMsg> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: QueryResultPollMsg) {
        let result = self.counters.get(&msg.initiator).map(|counter| counter.evaluate());
        (msg.callback)(result).await;
    }
}

#[async_trait::async_trait]
impl Handler<SyncTriggerMsg> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: SyncTriggerMsg) {
        let peer = self.pss.get_random_peer();

        let gossip_msg = SyncGossipMsg {
            counters: self.counters.clone(),
            predicates: self.predicates.clone(),
        };

        peer.send(gossip_msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<SyncGossipMsg> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: SyncGossipMsg) {
        for (query_id, mut received_counter) in msg.counters {
            let (received_timestamp, received_predicate) = msg.predicates.get(&query_id).unwrap();

            match self.predicates.get_mut(&query_id) {
                None => {
                    if (*received_predicate)(&self.uuid) {
                        received_counter.try_count_one_more_element(&mut *self.rs).unwrap();
                    }
                    self.counters.insert(query_id, received_counter);
                    self.predicates.insert(query_id, (*received_timestamp, received_predicate.clone()));
                },
                Some((local_timestamp, _)) => {
                    if received_timestamp > local_timestamp {
                        if (*received_predicate)(&self.uuid) {
                            received_counter.try_count_one_more_element(&mut *self.rs).unwrap();
                        }
                        self.counters.insert(query_id, received_counter);
                        self.predicates.insert(query_id, (*received_timestamp, received_predicate.clone()));
                    } else {
                        self.counters.get_mut(&query_id).unwrap().try_merge_with(&received_counter).unwrap();
                    }
                }
            }
        }
    }
}
