use std::fmt::Debug;

use once_cell::sync::OnceCell;
use state::Container;

/// SINGLETON_TYPE is the global singleton type.
static SINGLETON_TYPE: OnceCell<SingletonType> = OnceCell::new();

/// GLOBAL is a static type that holding all global data.
static GLOBAL: OnceCell<Container![Send + Sync]> = OnceCell::new();

/// Singleton is a wrapper enum for `Container![Send + Sync]`.
///
/// - `Production` is used in our production code.
pub enum SingletonType {
    Production,
}

impl SingletonType {
    fn get<T: Clone + 'static>(&self) -> T {
        match self {
            SingletonType::Production => {
                let v: &T = GLOBAL.wait().get();
                v.clone()
            }
        }
    }

    fn set<T: Send + Sync + 'static>(&self, value: T) -> bool {
        match self {
            SingletonType::Production => GLOBAL.wait().set(value),
        }
    }
}

impl Debug for SingletonType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Singleton")
            .field("type", &match self {
                Self::Production => "Production",
            })
            .finish()
    }
}

/// Global is an empty struct that only used to carry associated functions.
pub struct GlobalInstance;

impl GlobalInstance {
    /// init global data registry.
    ///
    /// Should only be initiated once.
    pub fn init() {
        let _ = SINGLETON_TYPE.set(SingletonType::Production);
        let _ = GLOBAL.set(<Container![Send + Sync]>::new());
    }

    /// Get data from global data registry.
    pub fn get<T: Clone + 'static>() -> T {
        SINGLETON_TYPE
            .get()
            .expect("global data registry must be initiated")
            .get()
    }

    pub fn try_get<T: Clone + 'static>() -> Option<T> {
        SINGLETON_TYPE.get().map(|v| v.get())
    }

    /// Set data into global data registry.
    pub fn set<T: Send + Sync + 'static>(value: T) {
        let set = SINGLETON_TYPE
            .get()
            .expect("global data registry must be initiated")
            .set(value);
        assert!(set, "value has been set in global data registry");
    }
}
