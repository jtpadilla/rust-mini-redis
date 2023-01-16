use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Un envoltorio alrededor de una instancia `Db`. 
/// Su funcion es permitir la limpieza ordenada de `Db` al marcar que 
/// la tarea de purga en segundo plano se cierre cuando se elimine esta estructura.
#[derive(Debug)]
pub struct DbDropGuard {
    /// La instancia de `Db` que sera desmontada cuando esta estructura 
    /// `DbDropGuard` sea eliminada (dropped).
    db: Db,
}

/// Estado del servidor comportido con todas las conexiones.
/// 
/// 'Db' contiene en su interior las estructuras de datos que almacenando
/// los key/value y tambien todos los valores `broadcast::Sender`
/// para los canales activos de pub/sub.
/// 
/// En primera instancia contiene un Arc 'Atomically Reference Counted' para 
/// poder compartir con el resto de threads estos datos.
/// 
/// Cuando un 'Db' es creado la lanza tambien una tarea. Esta tarea es 
/// utilizada para gestionar la expiracion de los valores. La tarea funcionara 
/// hasta que todas las instancias de 'Db' son borradas, momento en el que
/// terminara.
#[derive(Debug, Clone)]
pub struct Db {
    /// Gestiona el estado compartido. La tarea secundaria que gestiona 
    /// las expiraciones tambien poseera un `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// El estado compartido es custodiado por un mutex. Este es un `std::sync::Mutex`
    /// standar y no se utiliza la version del mutex de Tokio.
    /// Esto es asi porque no se estan realizando operaciones asincronas mientras 
    /// se mantiene ocupado el mutex. Ademas la seccion critica es muy pequeña.
    /// 
    /// Un mutex Tokio está diseñado principalmente para usarse cuando los bloqueos 
    /// deben mantenerse en los puntos de cesion `.await`. Por lo general, todos 
    /// los demás casos se atienden mejor con un mutex estándar.
    /// 
    /// Si la sección crítica no incluye ninguna operación asíncrona pero es larga 
    /// (uso intensivo de la CPU o realiza operaciones de bloqueo), entonces toda 
    /// la operación, incluida la espera del mutex, se considera una operación 
    /// de "bloqueo" y `tokio::task::spawn_blocking` debería ser usado.
    /// 
    state: Mutex<State>,

    /// Notifica el vencimiento de la entrada de manejo de tareas en segundo plano.
    /// La tarea en segundo plano espera a que se notifique esto, luego verifica 
    /// los valores caducados o la señal de parada.
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    // Key/Value: Utilizamos un `std::collections::HashMap`.
    entries: HashMap<String, Entry>,

    /// Se utiliza un espacio separado para el key/value y el pub/sub. Tambien se
    /// utiliza un `std::collections::HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Seguimiento de las claves TTLs
    /// 
    /// Un 'BTreeMap' se utiliza para mantener los vencimientos ordenados por 
    /// fecha de vencimiento. Esto permite a la tarea secundaria iterar por 
    /// este mapa para encontrar el siguiente valor que expira.
    /// 
    /// Aunque es poco probable, es posible que se cere un venciamiento para
    /// el mismo instante. Por ese motivo, un 'Instant' es insuficiente como clave.
    /// Un identificador unico 'u64' se utiliza para garantiza que la clave sea unica.
    expirations: BTreeMap<(Instant, u64), String>,

    /// Identificador que se utilizara para la clave compuesta de la proxima expiracion.
    next_id: u64,

    /// 'True' si la instancia de la base de datos se esta deteniendo. Esto 
    /// ocurre cuando todos los values de 'Db' han sido Drop. Asignando este
    /// valor a 'true' se marca a la tarea secundaria para que se detenga.
    shutdown: bool,
}

/// Entrada en el almacen Key/Value
#[derive(Debug)]
struct Entry {
    /// Identificador unico de la entrada.
    id: u64,

    /// Datos almazanados
    data: Bytes,

    /// Instante en el que la entrada expira y debe ser eliminada de la base de datos
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// Crea un nuevo 'DbDropGuard' que recubre a una instancia de 'Db'.
    /// Este envoltorio permite realiza la purga de la Bd cuando esta instancia
    /// es 'droped'.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { 
            db: Db::new() 
        }
    }

    /// Obtiene el recurso compartido. Internamente es un 
    /// 'Arc', asi que se incremete el contador de referencias.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // Marca la instancia de 'Db' para que se detenga la tarea que purga las 
        // claves que han expirado.
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Crea una nueva instancia de 'Db' que no contiene ninguna entrada. Tambien
    /// crea la tarea que gestiona las expiraciones proporcionandole el primero
    /// clon de la base de datos.
    pub(crate) fn new() -> Db {

        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Inicial la tarea.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        // Se instancia un 'Db'
        Db { 
            shared 
        }

    }

    /// Obtiene el valor asociado con una clave.
    /// 
    /// Retorna 'None' si no hay un valor asociado con la clave. 
    /// Get the value associated with a key. Esto puede a que nunca de
    /// le asigno un valor a la clave o a que el valor expiro.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Se adquire el bloqueo
        let state = self.shared.state.lock().unwrap();

        // Se lee la entrada y clona el valor.
        //
        // Como los datos estan almacenados utilizando 'Bytes', un clone 
        // en este caso es un clonado superficial (los datos no se copias).
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Establece un valor asociado con una clave junto con un periodo de
    /// vencimiento que es opcional.
    /// 
    /// Si ya hay un valor asociado con la clave, el nuevo valor substituira 
    /// al anterior.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let notify = {
            // Se adquire el bloqueo
            let mut state = self.shared.state.lock().unwrap();

            // El Id almacenado en el estado es el que se utilizara para esta operacion.
            let id = state.next_id;

            // Se incremente el Id para proxima insercion. Gracias a la 
            // proteccion del bloqueo cada operacion 'set' tiene garantizado un Id unico.
            state.next_id += 1;

            // En caso de que se haya especificado una duracion para la expiracion 
            // del valor, se convierte este duracion en el momento exacto de 
            // la expiracion.
            //
            // Tambien se programa la expiracion en el mapa de expiraciones.
            //
            // En caso de que la nueva expiracion resulta ser la proxima a ejecutar
            // se le enviara una notificacion a la tarea subyacente. 
            let (notify, expires_at) = if expire.is_some() {
                // Se calcula cuando la clave expirara.
                let when = Instant::now() + expire.unwrap();

                // Unicamente se notificara a la tarea de gestion de las expiraciones si
                // la expiracion del nuevo valor que se esta estableciendo resulta
                // ser la proxima expiracion a ejecutarse.
                let notify = state
                    .next_expiration()
                    .map(|expiration| expiration > when)
                    .unwrap();

                // Track the expiration.
                state.expirations.insert((when, id), key.clone());

                // Resultado
                (notify, Option::Some(when))

            } else {
                (false, Option::None)
            };

            // Se asigna la clave el nuevo valor en el HashMap principal.
            // Si para esta misma clave habia un valor anterior, este se
            // obtendra como resultado de la ejecucion.
            let prev = state.entries.insert(
                key,
                Entry {
                    id,
                    data: value,
                    expires_at,
                },
            );

            // Si previamente habia un valor asociado a la clave y ese valor tenia
            // definida una expiracion entonces hay que aliminar la correpondiente
            // entrada de mapa de expiraciones.
            if let Some(prev) = prev {
                if let Some(when) = prev.expires_at {
                    // clear expiration
                    state.expirations.remove(&(when, prev.id));
                }
            }

            // Se liberta el mutex antes de notificar la tarea en segundo plano. 
            // Esto ayuda a reducir la contención al evitar que la tarea en segundo 
            // plano se active y no pueda adquirir el mutex debido a que esta función 
            // aún lo retiene.
            //drop(state);

            notify
        };

        if notify {
            // Finalmente, solo se notifica a la tarea en segundo plano si necesita 
            // actualizar su estado para reflejar un nuevo vencimiento.
            self.shared.background_task.notify_one();
        }

    }

    /// Retorna un 'tokio::sync::broadcast::Receiver' para el canal requerido.
    /// 
    /// El 'Receiver' recibido se puede utilizar para recibir valores difundidos
    /// por los comandos 'PUBLISH'.
    pub fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Se adquiere el bloqueo
        let mut state = self.shared.state.lock().unwrap();

        // Si no hay una entrada para el canal requerido, entonces se crea un 
        // nuevo canal de difusion y se asocia con el canal.
        // En caso de que si existe, se retirna el 'Receiver' asociado a el.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => {
                // Para el canal indicado ya tenemos registrado un 'Sender'
                // del que utilizaremos la funcion 'subscrive(&self)' para 
                // clonar un nuevo 'tokio::sync::broadcast::Receiver'.
                e.get().subscribe()
            },
            Entry::Vacant(e) => {
                // No existe el canal de difusion, asi que se crea uno.
                //
                // El canal es creado con la capacidad de 1024 mensajes. Un
                // mensaje es almacenado en el canal hasta que TODOS los 
                // subscriptores lo han recibido. Esto significa que 
                // un subscriptor lento podria dejar mensajes almacenados
                // indefinidamente.
                //
                // Cuando la capacidad del canal se llene, la publicación 
                // dará como resultado que se eliminen los mensajes antiguos. 
                // Esto evita que los consumidores lentos bloqueen todo el sistema.
                let (tx, rx) = broadcast::channel(1024);

                // Se inserta en el mapa el 'tokio::sync::broadcast::Sender'
                e.insert(tx);

                // Y como resultado entregamos un 'tokio::sync::broadcast::Receiver'
                rx
            }
        }
    }

    /// Publica un mensaje en el canal y retorna el numero de subscriptores
    /// que hay en el momento del envio (no quiered decir que todos lo reciban)
    pub fn publish(&self, key: &str, value: Bytes) -> usize {
        // Se adquiere el bloqueo
        let state = self.shared.state.lock().unwrap();

        // Se buscan el 'tokio::sync::broadcast::Sender' para el canal.
        state
            .pub_sub
            .get(key)
            // Si se encuentra utilizamos el closure del '.map' para
            // enviar el mensaje con el 'Sender' recuperado.
            // Del Option resultante del envio retornamos el numero de subscriptores
            // o un valor 0 se se produjo un error en el envio.
            .map(|tx| tx.send(value).unwrap_or(0))
            // Si no existia en el mapa el canal, se retornaran 0 subscriptores
            .unwrap_or(0)
    }

    /// Le envia la senyal a la tarea de shutdown. Esta funcion es llamada por la
    /// implementacion del trait 'Drop' de 'DbDropGuard'.
    fn shutdown_purge_task(&self) {

        {
            // Se adquiere el bloqueo
            let mut state = self.shared.state.lock().unwrap();

            // Se marca `State::shutdown` a `true`.
            state.shutdown = true;

            // Se liberta el mutex antes de notificar la tarea en segundo plano. 
            // Esto ayuda a reducir la contención al evitar que la tarea en segundo 
            // plano se active y no pueda adquirir el mutex debido a que esta función 
            // aún lo retiene.
            //drop(state);
        }

        // Se le envia la notificacion a la tarea
        self.shared.background_task.notify_one();

    }
}

impl Shared {
    /// Purga todas las claves que han expirado y retorna el `Instant` de la 
    /// que sera la siguiente expiracion.
    fn purge_expired_keys(&self) -> Option<Instant> {
        // Se adquiere el bloqueo
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // la base de datos se esta deteniendo.
            // Todos los handlers del estado compartido seran borrados.
            // La tarea en background se detendra.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        let state = &mut *state;

        // Se buscaran todas las claves que han expirado ya.
        let now = Instant::now();

        // Hay que tener en cuenta que el siguiente iterador entregara las entradas
        // del hash ordenadas por su clave.
        // Esto quiere decir que cuando la caducidad de la entrada sea posterior
        // a la establecida, todas las restantes entradas seran posteriores y ya
        // no es necesario continuiar avanzando la entrada.
        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                // se ha terminado la purga, la entrada actual ya es posterior al instante
                // definidi como limite y tambien es por tanto la proxima entrada
                // que caducara.
                // La tarea esperara hasta entonces.
                return Some(when);
            }

            // La clave ha expirado, se borra.
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }

        None
    }

    /// Retorna `true` si la base de datos esta parando.
    ///
    /// De momento no hay ningun mecanismo que vacie el estado.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    /// Desde el mapa 'expiratons' (de tipo BTreeMap<(Instant, u64), String>) se
    /// obtiene un iterador que estara ordenado de la clave.
    /// Se hace avanzar el iterador a la primera posicion para obtener la primera clave
    /// (que sera la clave con el instante mas bajo).
    /// De esta clave que esta formada por una tupla extrae el primer campo que es 
    /// el Instant.
    /// En realidad retornara un Option<Instant> ya que el caso de que el iterador 
    /// de las claves este vacio la expresion funcional retornara un 'Option.None'.
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Tarea ejecutada en segundo plano.
///
/// La terea estara dormida esperando alguna notificacion.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // La tarea permanecera en un blucle hasta que se le notifique la parada
    while !shared.is_shutdown() {
        // Se borran las entradas expiradas y el resultado nos indicara para
        // cuando es la siguiente caducidad.
        if let Some(when) = shared.purge_expired_keys() {
            // Hay que esperar los siguientes eventos:
            //  1) Ha transcurrido el tiempo hasta la siguienet expiracion
            //  2) Hemos recibido una notificacion general.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // Como no hay previstas expiraciones unicamente esperamos 
            // una notificacion general.
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}
