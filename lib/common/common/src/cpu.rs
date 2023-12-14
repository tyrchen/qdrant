use std::cmp::Ordering;
use std::num::NonZeroIsize;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Global CPU budget in number of cores for all optimization tasks.
/// Assigns CPU permits to tasks to limit overall resource utilization.
pub static OPTIMIZER_CPU_BUDGET: OnceLock<CpuBudget> = OnceLock::new();

/// Try to read number of CPUs from environment variable `QDRANT_NUM_CPUS`.
/// If it is not set, use `num_cpus::get()`.
pub fn get_num_cpus() -> usize {
    match std::env::var("QDRANT_NUM_CPUS") {
        Ok(val) => {
            let num_cpus = val.parse::<usize>().unwrap_or(0);
            if num_cpus > 0 {
                num_cpus
            } else {
                num_cpus::get()
            }
        }
        Err(_) => num_cpus::get(),
    }
}

/// Default value of CPU budget.
///
/// Dynamic based on CPU size.
///
/// On low CPU systems, we want to reserve the minimal amount of CPUs for other tasks to allow
/// efficient optimization. On high CPU systems we want to reserve more CPUs.
#[inline(always)]
fn default_cpu_budget(num_cpu: usize) -> NonZeroIsize {
    let cpu_budget = if num_cpu <= 32 {
        -1
    } else if num_cpu <= 48 {
        -2
    } else if num_cpu <= 64 {
        -3
    } else if num_cpu <= 96 {
        -4
    } else if num_cpu <= 128 {
        -6
    } else {
        -(num_cpu as isize / 16)
    };
    NonZeroIsize::new(cpu_budget).unwrap()
}

/// Get available CPU budget to use for optimizations as number of CPUs (threads).
///
/// This is user configurable via `cpu_budget` parameter in settings:
/// If 0 - auto selection, keep at least one CPU free when possible.
/// If negative - subtract this number of CPUs from the available CPUs.
/// If positive - use this exact number of CPUs.
///
/// The returned value will always be at least 1.
pub fn get_cpu_budget(cpu_budget_param: isize) -> usize {
    match cpu_budget_param.cmp(&0) {
        // If less than zero, subtract from available CPUs
        Ordering::Less => get_num_cpus()
            .saturating_sub(-cpu_budget_param as usize)
            .max(1),
        // If zero, use automatic selection
        Ordering::Equal => get_cpu_budget(default_cpu_budget(get_num_cpus()).get()),
        // If greater than zero, use exact number
        Ordering::Greater => cpu_budget_param as usize,
    }
}

/// Structure managing global CPU budget for optimization tasks.
///
/// Assigns CPU permits to tasks to limit overall resource utilization, making optimization
/// workloads more predictable and efficient.
#[derive(Debug)]
pub struct CpuBudget {
    semaphore: Arc<Semaphore>,
}

impl CpuBudget {
    pub fn new(cpu_budget: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(cpu_budget)),
        }
    }

    /// Try to acquire CPU permit for optimization task from global CPU budget.
    pub fn try_acquire(&self, desired_cpus: usize) -> Option<CpuPermit> {
        // Determine what number of CPUs to acquire based on available budget
        let num_cpus = self.semaphore.available_permits().min(desired_cpus) as u32;
        if num_cpus == 0 {
            return None;
        }

        // Try to acquire selected number of CPUs
        let result = Semaphore::try_acquire_many_owned(self.semaphore.clone(), num_cpus);
        let permit = match result {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return None,
            Err(TryAcquireError::Closed) => unreachable!("Cannot acquire CPU permit because CPU budget semaphore is closed, this should never happen"),
        };

        Some(CpuPermit::new(num_cpus, permit))
    }

    /// Check if there is any available CPU in this budget.
    pub fn has_budget(&self) -> bool {
        self.semaphore.available_permits() > 0
    }

    /// Block until we have any CPU budget available.
    ///
    /// Uses an exponential backoff strategy to avoid busy waiting.
    pub fn block_until_budget(&self) {
        if self.has_budget() {
            return;
        }

        // Wait for CPU budget to be available with exponential backoff
        // TODO: find better way, don't busy wait
        let mut delay = Duration::from_micros(100);
        while !self.has_budget() {
            thread::sleep(delay);
            delay = (delay * 2).min(Duration::from_secs(10));
        }
    }
}

impl Default for CpuBudget {
    fn default() -> Self {
        Self::new(get_cpu_budget(0))
    }
}

/// CPU permit, used to limit number of concurrent CPU-intensive operations
///
/// This permit represents the number of CPUs allocated for an operation, so that the operation can
/// respect other parallel workloads. When dropped or `release()`-ed, the CPUs are given back for
/// other tasks to acquire.
///
/// These CPU permits are used to better balance and saturate resource utilization.
pub struct CpuPermit {
    /// Number of CPUs acquired in this permit.
    pub num_cpus: u32,
    /// Semaphore permit.
    permit: Option<OwnedSemaphorePermit>,
}

impl CpuPermit {
    /// New CPU permit with given CPU count and permit semaphore.
    pub fn new(count: u32, permit: OwnedSemaphorePermit) -> Self {
        Self {
            num_cpus: count,
            permit: Some(permit),
        }
    }

    /// New CPU permit with given CPU count without a backing semaphore for a shared pool.
    pub fn dummy(count: u32) -> Self {
        Self {
            num_cpus: count,
            permit: None,
        }
    }

    /// Release CPU permit, giving them back to the semaphore.
    pub fn release(&mut self) {
        self.permit.take();
    }
}

impl Drop for CpuPermit {
    fn drop(&mut self) {
        self.release();
    }
}
