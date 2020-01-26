extern crate failure;
extern crate rayon;
#[macro_use]
extern crate lazy_static;


use std::sync::RwLock;
use std::sync::Mutex;
use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};
use failure::{Fail, Error};



lazy_static! {
    static ref POOL: RwLock<Option<ThreadPool>> = RwLock::new(ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .ok());
}


#[derive(Fail, Debug, Clone)]
pub enum AError {
    AsyncNotStarted,
    AsyncNotReady,
    AsyncRunning,
    AsyncFailed,
    AsyncStale,
    AsyncValuePulled,
    AsyncValueMoved,
    AsyncValueEmpty,
    NoValueFn,
    NoReadyFn,
    MutexPoisoned,
    NoThreadPool,
    OtherError(Arc<Error>)
}

impl From<Error> for AError {
    fn from(err: Error) -> Self {
        AError::OtherError(Arc::new(err))
    }
}


impl std::fmt::Display for AError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Async ERROR: {:?}", self)
    }
}

impl<T> From<std::sync::PoisonError<T>> for AError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        let err = AError::MutexPoisoned;
        err
    }
}


impl AError {
    pub fn async_not_ready() -> AError {
        AError::AsyncNotReady
    }
    pub fn async_running() -> AError {
        AError::AsyncRunning
    }
    pub fn async_pulled() -> AError {
        AError::AsyncValuePulled
    }
    pub fn async_moved() -> AError {
        AError::AsyncValueMoved
    }
    pub fn async_empty() -> AError {
        AError::AsyncValueEmpty
    }
    pub fn async_stale() -> AError {
        AError::AsyncStale
    }
    pub fn no_value_fn() -> AError {
        AError::NoValueFn
    }
    pub fn no_ready_fn() -> AError {
        AError::NoValueFn
    }
    pub fn mutex_poisoned() -> AError {
        AError::MutexPoisoned
    }
    pub fn no_thread_pool() -> AError {
        AError::NoThreadPool
    }
}


pub type AResult<T> = Result<T, AError>;
pub type CResult<T> = Result<T, Error>;

pub type AsyncValue<T> = Arc<Mutex<Option<AResult<T>>>>;

pub type AsyncValueFn<T> = dyn FnOnce(&Stale) -> CResult<T> + Send + 'static;
pub type AsyncReadyFn<T> = dyn FnOnce(Result<&mut T,
                                             AError>,
                                      &Stale)
                                      -> CResult<()> + Send + 'static;

use std::fmt::Debug;
impl<T: Send + Debug> Debug for Async<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(),std::fmt::Error> {
        write!(fmt, "{:?}", self.value)?;
        write!(fmt, "{:?}", self.async_value)?;
        write!(fmt, "{:?}", self.state)?;
        write!(fmt, "{:?}", self.stale)?;

        let async_closure = self.async_closure
            .try_lock()
            .map(|clos| format!("{:?}", clos.is_some()))
            .map_err(|e| format!("{:?}", e));

        let on_ready = self.on_ready
            .try_lock()
            .map(|clos| format!("On ready clsoruses: {}", clos.len()))
            .map_err(|e| format!("{:?}", e));

        write!(fmt, "{:?}", async_closure)?;
        write!(fmt, "{:?}", on_ready)?;

        Ok(())
    }
}

use std::iter::Fuse;
pub struct StoppableIterator<I: Iterator> {
    iter: Fuse<I>,
    stale: Stale,
}

impl<I: Iterator> StoppableIterator<I> {
    fn stale(&self) -> bool {
        self.stale
            .is_stale()
            .unwrap_or(true)
    }
}

impl<I: Iterator> Iterator for StoppableIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<I::Item> {
        // If stale return None, Fuse takes care of the rest
        if self.stale() {
            return None;
        }

        self.iter.next()
    }
}

pub trait StopIter {
    fn stop_stale(self, stale: Stale) -> StoppableIterator<Self>
    where
        Self: Iterator + Sized;
}

impl<I: Iterator> StopIter for I {
    fn stop_stale(self, stale: Stale) -> StoppableIterator<Self> {
        StoppableIterator {
            iter: self.fuse(),
            stale: stale
        }
    }
}

#[derive(Clone)]
pub struct Async<T: Send + 'static>
{
    pub value: AResult<T>,
    async_value: AsyncValue<T>,
    async_closure: Arc<Mutex<Option<Box<AsyncValueFn<T>>>>>,
    on_ready: Arc<Mutex<Vec<Box<AsyncReadyFn<T>>>>>,
    state: Arc<Mutex<State>>,
    stale: Stale
}

#[derive(Copy, Clone, Debug)]
pub enum State {
    Sleeping,
    Running,
    OnReady,
    Ready,
    Failed,
    Taken
}


impl<T: Send + 'static> Async<T>
{
    pub fn new(closure: impl FnOnce(&Stale) -> CResult<T> + Send + 'static)
               -> Async<T> {
        let closure: Box<AsyncValueFn<T>>
            = Box::new(closure);

        let async_value = Async {
            value: Err(AError::AsyncNotStarted),
            async_value: Arc::new(Mutex::new(None)),
            async_closure: Arc::new(Mutex::new(Some(closure))),
            on_ready: Arc::new(Mutex::new(vec![])),
            state: Arc::new(Mutex::new(State::Sleeping)),
            stale: Stale::new() };

        async_value
    }

    pub fn new_with_value(val: T) -> Async<T> {
        Async {
            value: Ok(val),
            async_value: Arc::new(Mutex::new(None)),
            async_closure: Arc::new(Mutex::new(None)),
            on_ready: Arc::new(Mutex::new(vec![])),
            state: Arc::new(Mutex::new(State::Ready)),
            stale: Stale::new()
        }
    }

    pub fn run_sync(self) -> AResult<T> {
        let value = self.async_value.clone();
        let closure = self
            .async_closure
            .lock()?
            .take()
            .ok_or_else(|| AError::no_value_fn())?;

        Async::run_closure(closure,
                           self.async_value,
                           self.on_ready,
                           self.state,
                           self.stale)?;

        let mut value = value.lock()?;
        let value = value
            .take()
            .unwrap_or_else(|| Err(AError::async_empty()));

        value
    }

    fn run_closure(async_closure: Box<AsyncValueFn<T>>,
                   async_value: AsyncValue<T>,
                   on_ready: Arc<Mutex<Vec<Box<AsyncReadyFn<T>>>>>,
                   state: Arc<Mutex<State>>,
                   stale: Stale) -> AResult<()> {
        state.lock().map(|mut state| *state = State::Running).ok();

        let value: AResult<T> = async_closure(&stale).map_err(|e| e.into());

        async_value
            .lock()
            .map_err(|_| AError::mutex_poisoned())
            .map(|mut val| {
                val.replace(value.map_err(|e| e.into()));

                state.lock().map(|mut state| *state = State::OnReady).ok();

                while let Ok(fun) = on_ready
                    .lock()
                    .map_err(|_| AError::mutex_poisoned())
                    .and_then(|mut funs| {
                        if funs.len() > 0 {
                            Ok(funs.remove(0))
                        } else { Err(AError::no_ready_fn()) }
                    }) {
                        val.as_mut().map(|val|
                                         fun(val.cloned_err(),
                                             &stale).ok());

                        // Hack to synchronize with chain_on_ready
                        state.lock().ok();
                    };


                match *val {
                    Some(Ok(_)) => {
                        state
                            .lock()
                            .map(|mut state| *state = State::Ready).ok()
                    },
                    _ => state.lock().map(|mut state| *state = State::Failed).ok()
                }
            }).ok();

        Ok(())
    }

    pub fn run(&mut self) -> AResult<()> {
        if self.is_running() {
            Err(AError::async_running())?
        }

        if let Ok(true) = self.is_stale() {
            Err(AError::async_stale())?
        }


        let closure = self
            .async_closure
            .lock()?
            .take()
            .ok_or_else(|| AError::no_value_fn())?;
        let async_value = self.async_value.clone();
        let stale = self.stale.clone();
        let on_ready = self.on_ready.clone();
        let state = self.state.clone();



        std::thread::spawn(move || {
            Async::run_closure(closure,
                               async_value,
                               on_ready,
                               state,
                               stale).ok();
        });

        self.set_running();

        Ok(())
    }

    pub fn run_pooled(&mut self, pool: Option<&ThreadPool>) -> AResult<()> {
        if self.is_running() {
            Err(AError::async_running())?
        }

        if let Ok(true) = self.is_stale() {
            Err(AError::async_stale())?
        }

        let closure = self
            .async_closure
            .lock()?
            .take()
            .ok_or_else(|| AError::no_value_fn())?;
        let async_value = self.async_value.clone();
        let stale = self.stale.clone();
        let on_ready = self.on_ready.clone();
        let state = self.state.clone();

        let pool = pool.ok_or_else(|| POOL.read())
            .map(|pool| pool)
            .map_err(|_| AError::no_thread_pool())?;

        pool.spawn(move || {
            Async::run_closure(closure,
                               async_value,
                               on_ready,
                               state,
                               stale).ok();
        });

        Ok(())
    }

    pub fn set_stale(&mut self) -> AResult<()> {
        self.stale.set_stale()?;
        Ok(())
    }

    pub fn set_fresh(&self) -> AResult<()> {
        self.stale.set_fresh()?;
        Ok(())
    }

    pub fn is_stale(&self) -> AResult<bool> {
        self.stale.is_stale()
    }

    pub fn get_stale(&self) -> Stale {
        self.stale.clone()
    }

    pub fn put_stale(&mut self, stale: Stale) {
        self.stale = stale;
    }

    pub fn is_running(&self) -> bool {
        self.state.lock()
            .map_err(|_| false)
            .map(|state| {
                if let State::Sleeping = &*state {
                    false
                } else { true }
            }).unwrap()

    }

    pub fn is_ready(&self) -> bool {
        self.state.lock()
            .map_err(|_| false)
            .map(|state| {
                if let State::Ready = &*state {
                    true
                } else { false }
            }).unwrap()

    }

    pub fn set_sleeping(&self) {
        self.state.lock()
            .map(|mut state| {
              *state = State::Sleeping
            }).ok();
    }

    pub fn set_running(&self) {
        self.state.lock()
            .map(|mut state| {
              *state = State::Running
            }).ok();
    }

    pub fn set_ready(&self) {
        self.state.lock()
            .map(|mut state| {
              *state = State::Ready
            }).ok();
    }

    pub fn set_failed(&self) {
        self.state.lock()
            .map(|mut state| {
              *state = State::Failed
            }).ok();
    }

    pub fn pull_async(&mut self) -> AResult<()> {
        if self.value.is_ok() { Err(AError::async_pulled())? }

        let mut async_value = self.async_value.lock()?;


        match async_value.as_ref() {
            Some(Ok(_)) => {
                let value = async_value.take()
                    .unwrap_or_else(|| Err(AError::async_empty()));
                self.value = value;
            }
            Some(Err(AError::AsyncValuePulled))
                => Err(AError::async_pulled())?,
            Some(Err(_)) => {
                let value = async_value.take()
                    .unwrap_or_else(|| Err(AError::async_empty()));
                self.value = value;
            }
            None => Err(AError::async_not_ready())?,
        }

        self.state.lock().map(|mut s| *s = State::Taken).ok();
        Ok(())
    }

    pub fn get(&self) -> AResult<&T> {
        match self.value {
            Ok(ref value) => Ok(value),
            Err(ref err) => Err(err.clone())
        }
    }

    pub fn get_mut(&mut self) -> AResult<&mut T> {
        match self.value {
            Ok(ref mut value) => Ok(value),
            Err(ref err) => Err(err.clone())
        }
    }

    pub fn on_ready(&mut self,
                    fun: impl FnOnce(Result<&mut T,
                                            AError>, &Stale)
                                     -> CResult<()> + Send + 'static)
                    -> AResult<()> {
        let state = self.state.lock()?;

        match *state {
            State::Sleeping | State::Running | State::OnReady => {
                let on_ready = Box::new(fun);

                self.on_ready
                    .lock()
                    .map(|mut on_ready_funs|
                         on_ready_funs.push(Box::new(on_ready)))
                    .ok();
            },
            State::Ready | State::Failed => {
                self.async_value.lock()?
                    .as_mut()
                    .ok_or(AError::async_empty())
                    .map(|value|
                         fun(value.cloned_err(), &self.stale))?.ok();
            },
            State::Taken => {
                fun(self.value.cloned_err(), &self.stale)?;
            },
        }

        Ok(())
    }

    pub fn set_default_thread_pool(pool: Option<ThreadPool>) -> AResult<()> {
        *POOL.write()? = pool;
        Ok(())
    }

    pub fn new_default_thread_pool() -> AResult<()> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .ok();
        *POOL.write()? = pool;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Stale(Arc<RwLock<bool>>);

impl Stale {
    pub fn new() -> Stale {
        Stale(Arc::new(RwLock::new(false)))
    }
    pub fn is_stale(&self) -> AResult<bool> {
        Ok(*self.0.read()?)
    }
    pub fn set_stale(&self) -> AResult<()> {
        *self.0.write()? = true;
        Ok(())
    }
    pub fn set_fresh(&self) -> AResult<()> {
        *self.0.write()? = false;
        Ok(())
    }
}

trait ClonedErr<T> {
    fn cloned_err(&mut self) -> Result<&mut T, AError>;
}

impl<T> ClonedErr<T> for Result<T, AError> {
    fn cloned_err(&mut self) -> Result<&mut T, AError> {
        match self {
            Ok(ref mut val) => Ok(val),
            Err(err) => Err(err.clone())
        }
    }
}
