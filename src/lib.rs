extern crate failure;
extern crate rayon;
#[macro_use]
extern crate lazy_static;
extern crate objekt;




use std::sync::RwLock;
use std::sync::Mutex;
use std::sync::Arc;

use std::ops::Deref;

use rayon::{ThreadPool, ThreadPoolBuilder};
use failure::{Fail, Error};
use failure::Backtrace;



lazy_static! {
    static ref POOL: RwLock<Option<ThreadPool>> = RwLock::new(ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .ok());
}


#[derive(Debug, Clone)]
pub struct ArcBacktrace(Arc<Backtrace>);

impl ArcBacktrace {
    fn new() -> Self {
        let bt = Arc::new(Backtrace::new());
        ArcBacktrace(bt)
    }
    fn backtrace(&self) -> Option<&Backtrace> {
        Some(self.0.deref())
    }
}




#[derive(Debug, Clone)]
pub enum AError {
    AsyncNotReady(ArcBacktrace),
    AsyncRunning(ArcBacktrace),
    AsyncFailed(ArcBacktrace),
    AsyncStale(ArcBacktrace),
    AsyncValuePulled(ArcBacktrace),
    AsyncValueMoved(ArcBacktrace),
    AsyncValueEmpty(ArcBacktrace),
    NoValueFn(ArcBacktrace),
    NoReadyFn(ArcBacktrace),
    MutexPoisoned(ArcBacktrace),
    NoThreadPool(ArcBacktrace),
    OtherError(ArcBacktrace, Arc<Error>)
}

impl From<Error> for AError {
    fn from(err: Error) -> Self {
        AError::OtherError(ArcBacktrace::new(),
                           Arc::new(err))
    }
}

impl Fail for AError {
    fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            AError::AsyncNotReady(bt) => bt.backtrace(),
            AError::AsyncRunning(bt) => bt.backtrace(),
            AError::AsyncFailed(bt) => bt.backtrace(),
            AError::AsyncStale(bt) => bt.backtrace(),
            AError::AsyncValuePulled(bt) => bt.backtrace(),
            AError::AsyncValueMoved(bt) => bt.backtrace(),
            AError::AsyncValueEmpty(bt) => bt.backtrace(),
            AError::NoValueFn(bt) => bt.backtrace(),
            AError::NoReadyFn(bt) => bt.backtrace(),
            AError::MutexPoisoned(bt) => bt.backtrace(),
            AError::NoThreadPool(bt) => bt.backtrace(),
            AError::OtherError(bt, _) => bt.backtrace()
        }
    }
}

impl std::fmt::Display for AError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Async ERROR: {:?}", self)
    }
}

impl<T> From<std::sync::PoisonError<T>> for AError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        let err = AError::MutexPoisoned(ArcBacktrace::new());
        err
    }
}


impl AError {
    pub fn async_not_ready() -> AError {
        AError::AsyncNotReady(ArcBacktrace::new())
    }
    pub fn async_running() -> AError {
        AError::AsyncRunning(ArcBacktrace::new())
    }
    pub fn async_pulled() -> AError {
        AError::AsyncValuePulled(ArcBacktrace::new())
    }
    pub fn async_moved() -> AError {
        AError::AsyncValueMoved(ArcBacktrace::new())
    }
    pub fn async_empty() -> AError {
        AError::AsyncValueEmpty(ArcBacktrace::new())
    }
    pub fn async_stale() -> AError {
        AError::AsyncStale(ArcBacktrace::new())
    }
    pub fn no_value_fn() -> AError {
        AError::NoValueFn(ArcBacktrace::new())
    }
    pub fn no_ready_fn() -> AError {
        AError::NoValueFn(ArcBacktrace::new())
    }
    pub fn mutex_poisoned() -> AError {
        AError::MutexPoisoned(ArcBacktrace::new())
    }
    pub fn no_thread_pool() -> AError {
        AError::NoThreadPool(ArcBacktrace::new())
    }
}


pub type AResult<T> = Result<T, AError>;
pub type CResult<T> = Result<T, Error>;

pub type AsyncValue<T> = Arc<Mutex<Option<AResult<T>>>>;

pub type AsyncValueFn<T> = FnOnce(&Stale) -> CResult<T> + Send + 'static;
pub type AsyncReadyFn<T> = FnOnce(Result<&mut T,
                                         &mut AError>,
                                  &Stale)
                                  -> CResult<()> + Send + 'static;




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
            value: Err(AError::async_not_ready()),
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
                                         fun(val.as_mut(),
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


        let mut async_value = self.async_value.try_lock()
            .map_err(|_| AError::async_not_ready() )
            .unwrap();

        match async_value.as_ref() {
            Some(Ok(_)) => {
                let value = async_value.take()
                    .unwrap_or_else(|| Err(AError::async_empty()));
                self.value = value;
            }
            Some(Err(AError::AsyncValuePulled(_)))
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
                                            &mut AError>, &Stale)
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
                         fun(value.as_mut(), &self.stale))?.ok();
            },
            State::Taken => {
                fun(self.value.as_mut(), &self.stale)?;
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

trait ResultRefConvert<T> {
    fn convert(&self) -> Result<&T, &Error>;
}

impl<T> ResultRefConvert<T> for &CResult<T> {
    fn convert(&self) -> Result<&T, &Error> {
        match self {
            Ok(value) => Ok(&value),
            Err(err) => {
                let err = err.clone();
                Err(err)
            }
        }
    }
}

trait ResultRefMutConvert<T> {
    fn convert(&mut self) -> Result<&mut T, &Error>;
}

impl<T> ResultRefMutConvert<T> for &mut CResult<T> {
    fn convert(&mut self) -> Result<&mut T, &Error> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err)
        }
    }
}
