extern crate failure;
extern crate rayon;
#[macro_use]
extern crate lazy_static;




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
    AsyncReady,
    AsyncNotReady(ArcBacktrace),
    AsyncRunning(ArcBacktrace),
    AsyncFailed(ArcBacktrace),
    AsyncValuePulled(ArcBacktrace),
    AsyncValueMoved(ArcBacktrace),
    AsyncValueEmpty(ArcBacktrace),
    NoValueFn(ArcBacktrace),
    MutexPoisoned(ArcBacktrace),
    NoThreadPool(ArcBacktrace),
    ClosureFailed(ArcBacktrace, Arc<Error>),
}

impl From<Error> for AError {
    fn from(err: Error) -> Self {
        AError::ClosureFailed(ArcBacktrace::new(),
                              Arc::new(err))
    }
}

impl Fail for AError {
    fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            AError::AsyncNotReady(bt) => bt.backtrace(),
            AError::AsyncRunning(bt) => bt.backtrace(),
            AError::AsyncFailed(bt) => bt.backtrace(),
            AError::AsyncValuePulled(bt) => bt.backtrace(),
            AError::AsyncValueMoved(bt) => bt.backtrace(),
            AError::AsyncValueEmpty(bt) => bt.backtrace(),
            AError::NoValueFn(bt) => bt.backtrace(),
            AError::MutexPoisoned(bt) => bt.backtrace(),
            AError::NoThreadPool(bt) => bt.backtrace(),
            AError::ClosureFailed(bt, _) => bt.backtrace(),
            _ => None
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
    pub fn no_value_fn() -> AError {
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
pub type AsyncValueFn<T> = Box<dyn FnOnce(&Stale) -> CResult<T> + Send + Sync>;
pub type AsyncReadyFn = Box<dyn FnOnce(&Stale) -> CResult<()> + Send + Sync>;

pub type AsyncReadyRefFn<T> =
    Box<dyn FnOnce(&CResult<T>, &Stale)
                  -> CResult<()> + Send + Sync>;

pub type AsyncReadyRefMutFn<T> =
    Box<dyn FnOnce(&mut CResult<T>, &Stale)
                  -> CResult<()> + Send + Sync>;

pub type AsyncReadyMoveFn<T> =
    Box<dyn FnOnce(CResult<T>, &Stale) -> CResult<()> + Send + Sync>;

#[derive(Clone)]
pub struct Async<T: Send + 'static> {
    pub value: AResult<T>,
    async_value: AsyncValue<T>,
    async_closure: Arc<Mutex<Option<AsyncValueFn<T>>>>,
    on_ready: Arc<Mutex<Option<AsyncReadyFn>>>,
    on_ready_ref: Arc<Mutex<Option<AsyncReadyRefFn<T>>>>,
    on_ready_ref_mut: Arc<Mutex<Option<AsyncReadyRefMutFn<T>>>>,
    on_ready_move: Arc<Mutex<Option<AsyncReadyMoveFn<T>>>>,
    started: bool,
    stale: Stale
}



impl<T: Send + 'static> Async<T> {
    pub fn new(closure: AsyncValueFn<T>)
                  -> Async<T> {
        let async_value = Async {
            value: Err(AError::async_not_ready()),
            async_value: Arc::new(Mutex::new(None)),
            async_closure: Arc::new(Mutex::new(Some(closure))),
            on_ready: Arc::new(Mutex::new(None)),
            on_ready_ref: Arc::new(Mutex::new(None)),
            on_ready_ref_mut: Arc::new(Mutex::new(None)),
            on_ready_move: Arc::new(Mutex::new(None)),
            started: false,
            stale: Stale::new() };

        async_value
    }

    pub fn new_with_value(val: T) -> Async<T> {
        Async {
            value: Ok(val),
            async_value: Arc::new(Mutex::new(None)),
            async_closure: Arc::new(Mutex::new(None)),
            on_ready: Arc::new(Mutex::new(None)),
            on_ready_ref: Arc::new(Mutex::new(None)),
            on_ready_ref_mut: Arc::new(Mutex::new(None)),
            on_ready_move: Arc::new(Mutex::new(None)),
            started: false,
            stale: Stale::new()
        }
    }

    pub fn run_sync(self) -> AResult<T> {
        let value = self.async_value.clone();

        Async::run_closure(self.async_closure,
                           self.async_value,
                           self.on_ready,
                           self.on_ready_ref,
                           self.on_ready_ref_mut,
                           self.on_ready_move,
                           self.stale)?;

        let mut value = value.lock()?;
        value.take()
            .unwrap_or_else(|| Err(AError::async_empty()))
    }

    pub fn run_closure(async_closure: Arc<Mutex<Option<AsyncValueFn<T>>>>,
                       async_value: AsyncValue<T>,
                       on_ready: Arc<Mutex<Option<AsyncReadyFn>>>,
                       on_ready_ref: Arc<Mutex<Option<AsyncReadyRefFn<T>>>>,
                       on_ready_ref_mut: Arc<Mutex<Option<AsyncReadyRefMutFn<T>>>>,
                       on_ready_move: Arc<Mutex<Option<AsyncReadyMoveFn<T>>>>,
                       stale: Stale) -> AResult<()> {
        let closure = async_closure
            .lock()?
            .take().ok_or(AError::no_value_fn())?;

        let mut value = closure(&stale).map_err(|e| e.into());

        {
            let value_ref = &value;
            on_ready_ref
                .lock()
                .map_err(|_| AError::mutex_poisoned())
                .map(|mut on_ready_ref| {
                    on_ready_ref.take().map(|on_ready_ref| {
                        on_ready_ref(value_ref, &stale).ok();
                    });
                }).ok();
        }

        {
            let value_ref_mut = &mut value;
            on_ready_ref_mut
                .lock()
                .map_err(|_| AError::mutex_poisoned())
                .map(|mut on_ready_ref_mut| {
                    on_ready_ref_mut.take().map(|on_ready_ref_mut| {
                        on_ready_ref_mut(value_ref_mut, &stale).ok();
                    });
                }).ok();
        }


        async_value.lock()?.replace(value.map_err(|e| e.into()));
        on_ready.lock()?
            .take()
            .map(|on_ready| on_ready(&stale).ok());

        on_ready_move
            .lock()
            .map_err(|_| AError::mutex_poisoned())
            .map(|mut on_ready_move| {
                on_ready_move.take().map(|on_ready_move| {
                    async_value.lock().map(|mut async_value| {
                        let move_err = Err(AError::async_moved());
                        let value = async_value.replace(move_err);
                        value.map(|value| on_ready_move(value.map_err(|e| e.into()),
                                                                      &stale).ok());
                    }).ok();
                });
            }).ok();


        Ok(())
    }

    pub fn run(&mut self) -> AResult<()> {
        if self.started {
            Err(AError::async_running())?
        }

        let closure = self.async_closure.clone();
        let async_value = self.async_value.clone();
        let stale = self.stale.clone();
        let on_ready = self.on_ready.clone();
        let on_ready_ref = self.on_ready_ref.clone();
        let on_ready_ref_mut = self.on_ready_ref_mut.clone();
        let on_ready_move = self.on_ready_move.clone();
        self.started = true;

        std::thread::spawn(move || {
            Async::run_closure(closure,
                               async_value,
                               on_ready,
                               on_ready_ref,
                               on_ready_ref_mut,
                               on_ready_move,
                               stale).ok();
        });
        Ok(())
    }

    pub fn run_pooled(&mut self, pool: Option<&ThreadPool>) -> AResult<()> {
        if self.started {
            Err(AError::async_running())?
        }

        let closure = self.async_closure.clone();
        let async_value = self.async_value.clone();
        let stale = self.stale.clone();
        let on_ready = self.on_ready.clone();
        let on_ready_ref = self.on_ready_ref.clone();
        let on_ready_ref_mut = self.on_ready_ref_mut.clone();
        let on_ready_move = self.on_ready_move.clone();
        self.started = true;

        let run = || Async::run_closure(closure,
                                        async_value,
                                        on_ready,
                                        on_ready_ref,
                                        on_ready_ref_mut,
                                        on_ready_move,
                                        stale).ok();

        match pool {
            Some(pool) => {
                pool.spawn(move || {
                    run();
                });
            }
            None => {
                let pool = POOL.read()?;
                if let Some(pool) = pool.as_ref() {
                    pool.spawn(move || {
                        run();
                    });
                } else {
                    Err(AError::no_thread_pool())?
                }
            }
        }

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

    pub fn is_started(&self) -> bool {
        self.started
    }

    pub fn set_unstarted(&mut self) {
        self.started = false;
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
            Some(Err(AError::AsyncValuePulled(_)))
                => Err(AError::async_pulled())?,
            Some(Err(_)) => {
                let value = async_value.take()
                    .unwrap_or_else(|| Err(AError::async_empty()));
                self.value = value;
            }
            None => Err(AError::async_not_ready())?,
        }
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

    pub fn on_ready(&self,
                    fun: AsyncReadyFn) {
        *self.on_ready.lock().unwrap() = Some(fun);
    }

    pub fn on_ready_ref(&self,
                    fun: AsyncReadyRefFn<T>) {
        *self.on_ready_ref.lock().unwrap() = Some(fun);
    }

    pub fn on_ready_mut(&self,
                    fun: AsyncReadyRefMutFn<T>) {
        *self.on_ready_ref_mut.lock().unwrap() = Some(fun);
    }

    pub fn on_ready_move(&self,
                    fun: AsyncReadyMoveFn<T>) {
        *self.on_ready_move.lock().unwrap() = Some(fun);
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
